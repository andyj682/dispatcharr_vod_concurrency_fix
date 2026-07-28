# Dispatcharr VOD Concurrency Fix (plugin)

Plugin for Dispatcharr that stops an MKV "open-file burst" (2–3 near-simultaneous HTTP range
requests) from exhausting a `max_streams: 1` VOD profile and failing over to a
different provider account / different underlying file. This has been observed and tested with Emby, but likely applies to other MKV direct-play players.

The plugin coalesces requests by `(client IP, content)`: the first
request reserves one provider connection slot and the others within a few
seconds **ride** that reservation and stay pinned to the same provider/file,
instead of each being counted separately and rejected. Each request still opens
its own upstream range read. The plugin hooks both profile *selection* (so the
burst isn't rejected "at capacity") and *reservation* (so the slot is counted
once). See `DESIGN.md` for the full rationale and `patch.py` for the code.

- The selection/reservation logic is checked by `test_logic.py` (run
  `python test_logic.py` — no Dispatcharr or Redis required).
- No configuration needed. No user data collected.

---

## Install

**Files:** this folder must contain `plugin.json` and `plugin.py` (and
`patch.py`). Folder name on the host: `dispatcharr_vod_concurrency_fix`.

### Option A — Import via the UI (recommended)
1. Zip the plugin folder so the archive contains the folder with `plugin.json`
   and `plugin.py` inside it (`dispatcharr_vod_concurrency_fix.zip`).
   > **Windows note:** do NOT build the zip with PowerShell `Compress-Archive` —
   > it writes Windows `\` path separators that Dispatcharr's Linux importer
   > can't read ("missing plugin.py"). Build it with Python instead so entries
   > use `/`:
   > ```bash
   > py -3 -c "import zipfile,os; d='dispatcharr_vod_concurrency_fix'; z=zipfile.ZipFile(d+'.zip','w',zipfile.ZIP_DEFLATED); [z.write(os.path.join(d,f), d+'/'+f) for f in os.listdir(d) if os.path.isfile(os.path.join(d,f))]; z.close()"
   > ```
2. Dispatcharr UI → **Plugins** → **Import** → upload the zip.
3. Toggle the plugin **enabled** (accept the trust warning — plugins run
   server-side code).
4. **Restart the Dispatcharr container.** This is what guarantees the patch is
   applied in *all* uWSGI workers, including the ones that serve VOD (see
   "Why restart?" below).

### Option B — Drop-in folder
1. Copy this folder to `data/plugins/dispatcharr_vod_concurrency_fix/` on the
   host (→ `/app/data/plugins/…` in the container).
2. UI → **Plugins** → click **reload** (or `POST /api/plugins/plugins/reload/`).
3. Enable the plugin, then **restart the container**.

### Why restart?
Dispatcharr runs 4 uWSGI workers with `lazy-apps = true`. Each worker imports an
*enabled* plugin's code at boot and applies the monkeypatch then. Enabling
without a restart only reliably patches the worker that handled the enable
request; a restart patches all of them.

---

## Verify the patch is live in every worker (do this before trusting it)

The plugin logs one line per worker per entry point the first time it runs:

```
[VOD-CC] installed VOD concurrency-coalescing patch in worker pid=<PID>
[VOD-CC] active in worker pid=<PID> at stream
[VOD-CC] active in worker pid=<PID> at reserve
```

Steps:
1. After enabling + restarting, play a few different VOD titles (enough to hit
   multiple workers).
2. Look at the Dispatcharr logs and collect the distinct `pid=` values on the
   `[VOD-CC] active … at reserve` / `… at stream` lines.
3. You should see **more than one** distinct worker PID over several plays. If
   you only ever see one PID, the patch is not in every worker — restart again
   and re-check.

---

## Test the actual fix

1. Set the VOD profile's `max_streams: 1` (the condition that used to fail).
2. Play the title that triggers Emby's burst.
3. **Before:** the log showed `[PROFILE-SELECTION] All profiles at capacity …`
   then `[VOD-FAILOVER]` to a second provider with a different Stream ID right
   after the first range request.
   **After:** you should see the coalescing lines instead, e.g.:
   ```
   [VOD-CC] group OWNER reserved profile 3 (account 7) for <ip>/<uuid>
   [VOD-CC] selection: reusing group profile 3 for <ip>/<uuid> (bypassing capacity, 1/1)
   [VOD-CC] group RIDER shares profile 3 slot for <ip>/<uuid> (members=2)
   [VOD-CC] selection: reusing group profile 3 for <ip>/<uuid> (bypassing capacity, 1/1)
   [VOD-CC] group RIDER shares profile 3 slot for <ip>/<uuid> (members=3)
   ...
   [VOD-CC] group member left profile 3, N remain (slot held)
   [VOD-CC] group LAST member -> releasing profile 3
   ```
   Crucially: **no `[PROFILE-SELECTION] All profiles at capacity`** and **no
   `[VOD-FAILOVER]`** for the burst, and playback stays on the correct file.
   The `selection: reusing group profile … bypassing capacity` line is the one
   that proves the failover was prevented.
4. Afterwards, confirm the provider connection count returns to 0 (no leaked
   slot) — the `group LAST member -> releasing` line should fire once per burst.

Grep helper (adjust to your log access):
```bash
docker logs <dispatcharr-container> 2>&1 | grep -E "VOD-CC|VOD-FAILOVER|PROFILE-SELECTION|PROFILE-RESERVE|PROFILE-DECR"
```

---

## Uninstall / disable

- UI → **Plugins** → toggle **off** (Dispatcharr calls the plugin's `stop()`,
  which reverts the monkeypatch in that worker and deactivates it in the rest).
- For a clean, guaranteed revert across all workers, **restart the container**
  after disabling.

---

## Local logic test (optional)

```bash
python test_logic.py
```
Simulates the burst (selection bypass + rider), account pinning,
different-client, partial-failure, and client-disconnect teardown — asserting
the selection ladder and one-reserve / one-release symmetry. No Dispatcharr or
Redis required.

---

## Risks / what to watch for (beyond the happy path)

The patch touches VOD streaming through the **Xtream-Codes path**
(`stream_xc_movie/episode -> stream_vod`), every profile, movies and episodes.
Live TV (`live_proxy`), the XC metadata endpoints, EPG, and DVR are **not**
touched.

1. **Reading the log trace — `[VOD-FAILOVER]` can appear benignly.** When a
   burst request is pinned to the group's account, selection returns `None` for
   *other* candidate accounts, and Dispatcharr logs
   `[VOD-FAILOVER] Account X at capacity, trying next provider` for each skipped
   account — even though it was deliberately skipped, not truly full. This is
   only cosmetic **as long as the request then lands on the group's account**
   (you'll see `[VOD-CC] selection: reusing group profile … bypassing capacity`
   right after, and playback stays on the right file). The real failure signal
   is `[PROFILE-SELECTION] All profiles at capacity … rejecting` followed by a
   503 / a switch to a *different Stream ID*. In the common case (the group is
   on your highest-priority provider) the loop picks it first and you won't see
   any `[VOD-FAILOVER]` for the burst at all.

2. **Coverage boundary.** Only the XC path is coalesced. Requests that hit
   `/proxy/vod/...` directly (some non-Emby clients) and HEAD requests run
   *native* — safe, just not coalesced. If your Emby is Xtream-Codes (the
   `/movie/…`, `/series/…` URLs), you're on the covered path.

3. **Account pinning trade-off.** To keep a burst on one file, selection skips
   non-group accounts for the same (ip, content). If the group's account has its
   profile *deleted mid-burst*, that request falls back to native for that
   account and could 503 rather than failing over. Extreme edge (admin deleting
   a profile during playback); chosen deliberately over the alternative
   (silently drifting to a different provider/file).

4. **Under-counting genuinely-separate playbacks that share (IP, title).**
   Groups are keyed by `(client_ip, content_uuid)`. Two *real* separate
   playbacks that share both — e.g. two devices behind one public IP playing the
   *same* movie at once — merge into one provider slot. Each still opens its own
   upstream socket, so the provider sees 2 connections while Dispatcharr counts
   1; if the provider enforces its own limit the second could be rejected. Near
   zero for a single-user homelab; possible on shared/NAT'd IPs. *Watch for:* a
   second device on the **same title** failing while the first works. Different
   titles from the same IP are unaffected (separate groups); different clients
   (different IPs) on the same title correctly still hit capacity.

5. **VOD stats UI may show more entries than provider connections** — each burst
   request is its own session, so the panel can show N range readers for one
   logical playback while the provider connection count correctly reads 1.
   Cosmetic.

6. **Leaked slot on abrupt crash — bounded and self-healing.** If a stream dies
   skipping its teardown (worker crash, hard TCP reset), a group can hold its
   slot up to the group TTL (`GROUP_TTL_SECONDS`, 30s); within that window a new
   same-(ip,content) request could bypass capacity onto the phantom group.
   Dispatcharr's own stale-connection cleanup still recovers the native counter
   (our decrement falls back to a raw native decrement outside a request
   context). No worse than stock, and scoped to one (ip, content).

7. **Changed-internals / per-request safety.** If Dispatcharr refactors the
   patched functions, `install()` refuses to patch and logs why; any per-request
   error falls back to native. Failure mode is "no coalescing," never "broken
   streaming."

**Context safety:** the `(ip, content)` context lives in a greenlet-local that
is set at the top of `stream_vod` and always reset at request end (the streaming
generator's `finally`, or the non-streaming/`except` paths). Combined with
uWSGI/gevent using a fresh greenlet per request, a prior request's context
cannot bleed into a later one.

## Redis keys used

- `vodcc:grp:{client_ip}:{content_uuid}` — hash: `refcount`, `reserved`,
  `profile_id`, `account_id`, `created_at`, `last_activity`; TTL
  `GROUP_TTL_SECONDS` (30s), refreshed on activity. Deliberately distinct from
  the older `cedric-marcoux/dispatcharr_vod_fix` plugin's `vod_client_slot:`
  keys.

Native keys (`profile_connections:{id}`, `vod_persistent_connection:{session}`,
etc.) are untouched except through the unmodified native reserve/release calls.

---

## Prior art / acknowledgments

The idea of coalescing a VOD client's near-simultaneous range-request burst by
`(client IP, content)` with a short grace period comes from
[`cedric-marcoux/dispatcharr_vod_fix`](https://github.com/cedric-marcoux/dispatcharr_vod_fix)
(MIT). This plugin is **not a fork** — it's an independent implementation for
current Dispatcharr, targeting different hook points (`stream_vod`,
`_get_m3u_profile`, `_check_and_reserve_profile_slot`,
`_decrement_profile_connections`), coalescing at both profile *selection* and
*reservation*, and using atomic Redis Lua with greenlet-local request context.
But that project was the inspiration for the approach, and credit is due.

Designed and built by [andyj682](https://github.com/andyj682) with Claude
(Anthropic) as a pair-programming collaborator — Dispatcharr code analysis,
concurrency design, and implementation.
