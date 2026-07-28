# VOD: concurrent range-request burst exhausts a `max_streams: 1` profile and fails over to a different file

## Summary

When a VOD client (Emby / libmpv MKV direct-play, and similar) opens a large
file, it issues 2–3 **near-simultaneous** HTTP range requests:

1. `GET` (no `Range` / `bytes=0-`) — initial open
2. `GET Range: bytes=<near EOF>-` — MKV Cues / SeekHead index
3. `GET Range: bytes=<near start>-` — playback start

On a profile with `max_streams: 1`, requests #2/#3 are counted as *separate*
provider connections, the profile is reported "at capacity", and the request
**fails over to a different M3U account** — which resolves to a **different
underlying file** than the one already streaming. Setting `max_streams` to
unlimited makes the symptom disappear, which isolates it to connection
counting/matching, not provider behavior.

## Root cause (selection layer, not just reservation)

Every request in the burst arrives **without** a Dispatcharr session id, so
`stream_vod` (`apps/proxy/vod_proxy/views.py`) mints a fresh session id per
request via its 301 redirect. The burst therefore becomes 2–3 **independent
sessions**, and native session-reuse can't help: `find_matching_idle_session`
(`multi_worker_connection_manager.py`) only reuses a session whose
`active_streams == 0`, and request #1 is still actively streaming when #2/#3
arrive.

The failover is decided at **profile selection**:

- `_get_m3u_profile()` (`views.py`) checks `pool_has_capacity_for_profile()`
  (`apps/m3u/connection_pool.py`), i.e. `profile_connections:{id}` vs
  `max_streams`.
- Request #1 reserves the slot (`profile_connections → 1`). Request #2's
  selection reads the counter `== 1 == max_streams` → logs
  `[PROFILE-SELECTION] All profiles at capacity …` → the candidate loop in
  `stream_vod` selects the next M3U account → a different file.

The reservation layer (`_check_and_reserve_profile_slot` →
`reserve_profile_slot`, an INCR-vs-`max_streams`) would also reject #2, but
selection rejects it first — so a fix that only touches reservation never runs.
This is the key correction relative to a "just make the reservation
client-aware" framing: **selection must be made client-aware too.**

## Reproduction

- Profile with `max_streams: 1`, content available on ≥2 M3U accounts.
- Play the title in Emby (or any MKV direct-play client that probes with a
  range burst).
- Observe `[PROFILE-SELECTION] All profiles at capacity …` followed by a
  `[VOD-FAILOVER]` to a different account/stream id within ~1s of the first
  request, and playback landing on a different file.

## Proposed fix — coalesce the burst by `(client_ip, content_uuid)`

Recognize that several session ids from the same `(client_ip, content_uuid)`
within a few seconds are one logical client, and let them share one provider
slot / one file. A small Redis "group" plus coordination at four points:

```
group key:  {client_ip}:{content_uuid}
fields:     refcount, reserved, profile_id, account_id, TTL (short, refreshed)
```

1. **`stream_vod`** — establish per-request `(client_ip, content_uuid)` context;
   wrap the streaming generator so the context survives into the lazy
   teardown/release path.
2. **`_get_m3u_profile`** — if a confirmed group exists for this
   `(ip, content)`, return the group's already-chosen profile **bypassing the
   capacity check**, and pin selection to the group's account (skip other
   candidates) so the burst can't drift to a different provider/file. *This is
   what stops the failover.*
3. **`_check_and_reserve_profile_slot`** — the first group member does the real
   native reservation and records the chosen profile/account; later members ride
   it (no second INCR), so `profile_connections` stays correct.
4. **`_decrement_profile_connections`** — refcounted release: the real native
   release fires only when the **last** group member ends.

Each burst request keeps its own upstream provider socket (its own `Range`);
only the profile-slot reservation and the chosen file are shared. Cross-content
and cross-client requests are unaffected (distinct groups → native capacity
still applies).

### Where this could land natively

The equivalent logic could live directly in `_get_m3u_profile` /
`reserve_profile_slot` / `release_profile_slot`, given a
`(client_ip, content_uuid)` context threaded from `stream_vod` (the same context
`stream_content_with_session` already computes). That would remove the
external-plugin machinery. A reference implementation exists as a drop-in plugin
(monkeypatch at enable, revert in `stop()`) — link below.

## Runtime notes (relevant to any fix)

- uWSGI, 4 workers, `lazy-apps = true`, `gevent = 400` with
  `gevent-early-monkey-patch`. `threading.local()` is therefore greenlet-local
  (one in-flight request per greenlet), and the streaming generator runs in the
  request's greenlet — so per-request context threaded via a greenlet-local is
  safe *provided it is reset at request end* (the generator's `finally`, and the
  non-streaming/`except` paths). The plugin closes the native generator
  explicitly before clearing context so the teardown release (which runs in
  native's `GeneratorExit` handler on the common client-stop path) sees the
  correct context rather than firing later at GC.
- Because burst requests use distinct session ids, each is its own greenlet, so
  a session's selection, reservation and release all run in one greenlet — no
  cross-greenlet context needed.

## Validation (on a live instance)

Confirmed on a real deployment across all 4 workers.

Concurrent burst, `max_streams: 1` — coalesced onto one slot, no failover:
```
[PROFILE-SELECTION] Selected profile 3 (TREX raw Default): 0/1 connections
[PROFILE-RESERVE] Profile 3 slot reserved: 1/1
[VOD-CC] group OWNER reserved profile 3 (account 3) for <ip>/<uuidA>
[VOD-CC] selection: reusing group profile 3 … (bypassing capacity, 1/1)
[VOD-CC] group RIDER shares profile 3 slot … (members=2)
[VOD-CC] group member left profile 3, 1 remain (slot held)
[VOD-CC] group LAST member -> releasing profile 3
[PROFILE-DECR] Profile 3 connections: 0
```

Different content from the same client correctly does **not** coalesce (movie B
fails over to another account while movie A holds the slot), and the burst then
stays pinned to the account it landed on:
```
[PROFILE-SELECTION] All profiles at capacity for M3U account 3, rejecting request
[PROFILE-SELECTION] Selected profile 8 (Strong 4K VOD Default): 0/1 connections
[VOD-CC] group OWNER reserved profile 8 (account 8) for <ip>/<uuidB>
[VOD-CC] selection: pinning <ip>/<uuidB> to group account 8, skipping account 3
[VOD-CC] selection: reusing group profile 8 … (bypassing capacity, 1/1)
[VOD-CC] group RIDER shares profile 8 slot … (members=2)
[VOD-CC] group LAST member -> releasing profile 8
[PROFILE-DECR] Profile 8 connections: 0
```

All slots return to 0 on teardown (no leak); live TV counting is unaffected.

## Trade-offs / considerations

- Coalescing by `(client_ip, content_uuid)` under-counts genuinely-separate
  playbacks that share both (e.g. two devices behind one public IP playing the
  same title). Different titles or different client IPs are unaffected.
- Abrupt-crash leaks are bounded by the group TTL and recovered by the existing
  stale-connection cleanup.
- The reference plugin covers the Xtream-Codes path (`stream_xc_movie/episode →
  stream_vod`). A native implementation would naturally cover the internal
  `/proxy/vod/...` path too.

## Reference implementation

Drop-in Dispatcharr plugin (four monkeypatches, atomic Redis Lua for the group
accounting, self-tests, full design doc): `<REPO_URL>`.
