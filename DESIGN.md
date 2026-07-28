# VOD concurrency fix — design resolution

Grounded in current Dispatcharr `main` (cloned 2026-07-27, commit `1835d24`).

## Root cause (confirmed in source AND in Andy's live logs)

`stream_vod` (`apps/proxy/vod_proxy/views.py`) mints a **fresh session id per
request** via a 301 redirect when a request arrives without one. Every
Xtream-Codes VOD request in Emby's open-file burst arrives session-id-less, so
the 2–3 near-simultaneous range requests become **2–3 independent sessions**.

The failover that maps a burst request onto a **different provider file**
happens at **profile selection**, not at reservation — this is the correction
to the original handoff framing, confirmed by the live log line
`[PROFILE-SELECTION] All profiles at capacity … rejecting request`:

- `_get_m3u_profile()` checks `pool_has_capacity_for_profile()`, i.e.
  `profile_connections:{id}` vs `max_streams`.
- Request #1 reserves the slot (`profile_connections:3 → 1`). Request #2's
  selection reads the counter == 1 == `max_streams` → "All profiles at capacity"
  → the candidate loop in `stream_vod` fails over to the next M3U account → a
  **different underlying file** than the one already playing.
- `_check_and_reserve_profile_slot` (the reservation layer the handoff memos
  focused on) *would* also reject #2 — but selection rejects it first, so a fix
  that only touches reservation never runs. A reservation-only fix does **not**
  work; this was caught during live testing.

Each burst request keeps its own session id, so native session-reuse
(`_get_m3u_profile`'s session path, and `find_matching_idle_session`'s
`active_streams == 0` gate) never helps: while #1 is streaming it is neither
idle nor sharing a session id with #2/#3.

## The fix — group-coalescing across selection AND reservation

Recognise that several different session ids from the same
`(client_ip, content_uuid)` within a few seconds are the same logical client,
and let them share ONE provider slot / one file. A small Redis group plus four
coordinated monkeypatches (this is the same shape as the proven older
`cedric-marcoux/dispatcharr_vod_fix` plugin, whose primary patch was also
`_get_m3u_profile`, re-targeted to current internals):

```
group key:  vodcc:grp:{client_ip}:{content_uuid}
fields:     refcount, reserved, profile_id, account_id, created_at, last_activity
TTL:        GROUP_TTL_SECONDS (30s), refreshed on activity
```

1. **`stream_vod`** — set per-request `(client_ip, content_uuid)` context in a
   greenlet-local, and wrap the streaming generator so that context survives
   into the lazy teardown (profile-release) path. This is patched as the
   module global, which the Emby path (`stream_xc_movie/episode → stream_vod`)
   and native's internal `_get_m3u_profile` call both resolve to.
2. **`_get_m3u_profile`** — if a confirmed group exists for this `(ip, content)`:
   return the group's already-chosen profile **bypassing the capacity check**
   when this candidate is the group's account, or return `None` (skip) for any
   other account so the candidate loop stays **pinned to the owner's
   account/file**. *This is what actually stops the failover.*
3. **`_check_and_reserve_profile_slot`** — the first group member does the real
   native reservation and records the chosen `profile_id`/`account_id`; later
   members ride it (no second INCR). Keeps `profile_connections` correct.
4. **`_decrement_profile_connections`** — refcounted release: the real native
   release fires only when the **last** group member ends, so the provider slot
   is held for exactly as long as any burst request is still streaming.

Each burst request still opens its **own** upstream provider socket (its own
`Range`) via its own `RedisBackedVODConnection` — three concurrent range reads,
sharing one profile-slot reservation and one file.

## Why the counting / rollback stays correct

- **One reserve ↔ one release per group.** The owner's single native reserve is
  balanced by the last-out member's single native release. Riders never INCR and
  never release. Verified in `test_logic.py` (burst, partial-failure).
- **Partial failure:** native calls `_decrement_profile_connections` on every
  rollback path (416, create-failure, mid-stream error) and at teardown; our
  version maps each onto a group leave, so a failed burst member leaves the group
  and the slot stays held for the survivors.
- **Owner finishing first:** the no-Range probe (#1) often ends before playback
  (#2/#3). Release is tied to *last-out*, not to the owner, so the slot is not
  freed early. Verified.

## Why context threading is safe (the part the old plugin got wrong)

Dispatcharr runs uWSGI with `gevent = 400` + `gevent-early-monkey-patch`, so
`threading.local()` is **greenlet-local** — one in-flight request per greenlet.
Each burst request is a distinct session id ⇒ its own greenlet ⇒ its selection,
reservation and release all run in the **same** greenlet. Context is set
synchronously at the top of `stream_vod` (covering selection + reservation +
synchronous rollback releases) and re-asserted inside a wrapper around the
streaming generator (covering the lazy teardown release).

The generator wrapper explicitly closes the native generator **before** clearing
context, so native's teardown release (which runs in native's `GeneratorExit`
handler on the common *client-stop* path) sees the correct group plan instead of
firing later at GC with the context already reset. Missing this leaks the
counter on every client stop — it was caught and fixed with a regression test.

The old `cedric-marcoux` plugin stashed context on the singleton manager and
cleared it in a `finally` that fired before the lazy generator's decrement,
which is why it leaked and needed a `cleanup_orphan_counter` band-aid. This
design avoids that class of bug.

## Timing / races

Requests are ~250ms apart and each goes through a 301 redirect round-trip, so
the owner confirms its reservation (and writes the group) well before a rider
selects. The only race — a rider joining in the sub-millisecond window before
the owner marks the group `reserved` — is handled conservatively: the rider
backs out and does an independent native reservation (today's behaviour, never
an over-subscription).

## Blast radius / known trade-offs

- Touches **all** VOD streaming (`vod_proxy`), every client, movies + episodes.
  **Not** live TV, XC metadata endpoints, EPG, or DVR.
- **Under-counts genuinely-separate playbacks that share `(IP, title)`** — e.g.
  two devices behind one public IP playing the same movie. Near-zero for a
  single-user homelab; possible on shared/NAT'd IPs. Different titles from the
  same IP are unaffected (distinct groups).
- VOD stats UI may show N range-readers as N entries while the provider slot
  reads 1 (cosmetic).
- Abrupt-crash leak is bounded by the group TTL and recovered by Dispatcharr's
  own stale-connection cleanup (our decrement falls back to a raw native
  decrement when it runs outside a request) — no worse than stock.
- Fail-safe: if internals change, `install()` refuses to patch; any per-request
  error falls back to native. Failure mode is "no coalescing," never "broken
  streaming."

## Multi-worker propagation

uWSGI: 4 workers, `lazy-apps = true`. Each imports an enabled plugin at boot and
applies the patches. Enable + **restart** to guarantee all workers; confirm from
the per-worker `[VOD-CC] active … pid=` logs. Runtime enable-without-restart
relies on the loader's `.reload_token` pull and isn't guaranteed prompt.

## Upstreaming note

The equivalent logic could land directly in `_get_m3u_profile` /
`reserve_profile_slot` / `release_profile_slot` given a `(client_ip,
content_uuid)` context threaded from `stream_vod`, removing the greenlet-local
machinery. The plugin is the **test vehicle**, not necessarily the final shape.
