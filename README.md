# Dispatcharr VOD Concurrency Fix (plugin)

> ### ⚠️ Running Dispatcharr v0.31.0 or newer? You need plugin v1.2.0.
>
> **Plugin v1.1.0 and earlier break all VOD playback on Dispatcharr v0.31.0**
> (every VOD request returns `HTTP 500 Streaming error`). v0.31.0 added a
> parameter to an internal function this plugin wraps; the old wrapper rejects
> it while Python is binding arguments, which happens *before* any of the
> plugin's own error handling can fall back to native behaviour. Live TV is
> unaffected.
>
> **Fix:** upgrade to v1.2.0.
> **Immediate workaround if you can't upgrade right now:** **disable the plugin**
> in the Dispatcharr UI (Plugins → toggle off). That restores the original
> function and VOD works again — no Dispatcharr rollback needed. Note this means
> the plugin's *enabled* toggle; there is no per-feature setting that helps,
> because the failure happens before any setting is read.
>
> v1.2.0 runs on **v0.31.0 and on v0.30.0 and earlier** — one build covers both,
> so it is safe to upgrade the plugin before or after Dispatcharr.

> ### Running Dispatcharr v0.32.0? Use plugin v1.3.0.
>
> v0.32.0 lets some players keep a whole burst on one Dispatcharr session
> (FFmpeg-based players that reuse its now-cacheable session redirect). With
> those players, v1.2.0 could occasionally leave a quick stop-and-replay
> uncounted, so a second stream could be let onto a full provider. v1.3.0
> tracks the coalescing group by session and fixes that. **Emby is not
> affected** (its requests come from the Emby server, which doesn't reuse the
> redirect), but v1.3.0 is the right build for v0.32.0 either way.
>
> v0.32.0's own redirect change does **not** fix the burst failover for Emby,
> so the plugin is still needed there. v1.3.0 runs on v0.30.0, v0.31.0 and
> v0.32.0.
>
> **v0.34.x:** every hooked function is unchanged. Core now *resumes* an idle
> session for a request that arrives without one (and waits up to 1 s for a
> running one to close), so two requests from a client that sends a
> User-Agent can land on the same session. v1.3.0's session-level tracking
> covers that (see `test_core_session_resume_v0_34`). Emby's VOD requests
> carry no User-Agent, and core never resumes for an unknown one, so Emby
> still gets one session per request and still needs the plugin (confirmed
> live on v0.34.1). On v0.34 the **Check status** button
> runs in a background worker, not a streaming one, so it can't vouch for the
> streaming workers; use the log lines under *Verify the patch is live*.

Plugin for Dispatcharr that stops overlapping VOD requests from one client —
the MKV "open-file burst" (2–3 near-simultaneous HTTP range requests) at start,
**and a seek** (a new connection that opens before the old one closes) — from
exhausting a `max_streams: 1` VOD profile and failing over to a different
provider account / different underlying file. This has been observed and tested
with Emby, but likely applies to other MKV direct-play players.

The plugin coalesces requests by `(client IP, content)`: the first
request reserves one provider connection slot and the others within a few
seconds **ride** that reservation and stay pinned to the same provider/file,
instead of each being counted separately and rejected. Each request still opens
its own upstream range read. The plugin hooks both profile *selection* (so the
burst isn't rejected "at capacity") and *reservation* (so the slot is counted
once). See `DESIGN.md` for the full rationale and `patch.py` for the code.

- No configuration needed. No user data collected.

*Targets current Dispatcharr's multi-worker VOD proxy. If the internals it
patches go missing — or a request hits an unexpected error — it falls back to
native behavior (no coalescing) rather than breaking playback.*

*One caveat, learned from v0.31.0: that fail-open cannot cover a changed
**signature** on a patched function. Python raises `TypeError` while binding the
arguments, before the wrapper body — and therefore before any guard or
`try/except` — runs. v1.2.0 closes this off by making every wrapper accept and
forward arguments it doesn't recognise, so a future added parameter is a no-op
rather than an outage.*

---

## Install

### Option A — Import via the UI (recommended)
1. Download `dispatcharr_vod_concurrency_fix.zip` from the
   [latest release](https://github.com/andyj682/dispatcharr_vod_concurrency_fix/releases/latest).
2. Dispatcharr UI → **Plugins** → **Import** → upload the zip.
3. Toggle the plugin **enabled** (accept the trust warning — plugins run
   server-side code).
4. **Restart the Dispatcharr container** (see "Why restart?" below).

### Option B — Drop-in folder (from source)
1. Clone or copy this repo into `data/plugins/dispatcharr_vod_concurrency_fix/`
   on the host (→ `/app/data/plugins/…` in the container). The folder must be
   named `dispatcharr_vod_concurrency_fix` and contain `plugin.json` +
   `plugin.py`.
2. UI → **Plugins** → click **reload** (or `POST /api/plugins/plugins/reload/`).
3. Enable the plugin, then **restart the container**.

### Why restart?
Dispatcharr runs 4 uWSGI workers with `lazy-apps = true`. Each worker imports an
*enabled* plugin's code at boot and applies the monkeypatch then. Enabling
without a restart only reliably patches the worker that handled the enable
request; a restart patches all of them.

---

## Uninstall / disable

- UI → **Plugins** → toggle **off** (Dispatcharr calls the plugin's `stop()`,
  which reverts the monkeypatch in that worker and deactivates it in the rest).
- For a clean, guaranteed revert across all workers, **restart the container**
  after disabling.

---

## Troubleshooting

### Verify the patch is live

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

### Test the actual fix

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
5. **Seek test:** play a title, let it run **more than a minute**, then seek. It
   should stay on the same provider. In the log the seek shows
   `selection: reusing group profile … (bypassing capacity)` / `group RIDER` —
   **not** `All profiles at capacity` followed by a fresh `group OWNER` on a
   different account. (Before v1.1.0, a seek after ~30s of steady play failed
   over because the coalescing group had expired mid-playback.)

Grep helper (adjust to your log access):
```bash
docker logs <dispatcharr-container> 2>&1 | grep -E "VOD-CC|VOD-FAILOVER|PROFILE-SELECTION|PROFILE-RESERVE|PROFILE-DECR"
```

### Local logic test

```bash
python test_logic.py
```
Simulates the burst (selection bypass + rider), account pinning,
different-client, partial failure, and every way a response can end — asserting
the selection ladder and one-reserve / one-release symmetry. No Dispatcharr,
Django or Redis required: it models Django's streaming response, Dispatcharr
v0.32.0's per-session accounting (including bursts that share one session), and
per-request greenlet context.

It also covers the two things v1.2.0 is about: **signature parity** (an argument
the wrapper doesn't recognise must be accepted *and* forwarded — the guard that
would have caught the v0.31.0 break before it shipped) and **profile
allowlists** (permitted → pinned; not permitted or empty → native fallthrough;
unrestricted → unchanged). The fakes mirror v0.31.0's call shape, and one test
pins the wrapper against v0.30.0's older shape so a single build is proven on
both.

---

## Limitations / potential fail points

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
   slot up to the group TTL (`GROUP_TTL_SECONDS`, 60s); within that window a new
   same-(ip,content) request could bypass capacity onto the phantom group.
   Dispatcharr's own stale-connection cleanup still recovers the native counter
   (our decrement falls back to a raw native decrement outside a request
   context). No worse than stock, and scoped to one (ip, content).

7. **Per-user M3U profile allowlists: the group key is user-agnostic.**
   Dispatcharr v0.31.0 added per-user profile allowlists
   (`allowed_m3u_profile_ids`). The plugin honours them — if a group's pinned
   profile isn't permitted for a request, the pin is abandoned and native
   selection decides, so a restricted user never receives a profile they aren't
   allowed to use. But groups are keyed by `(client IP, content)` with **no user
   component**, so two users behind one IP watching the same title share a
   group. The degradation is graceful and per-request (whoever can't use the pin
   simply falls back to native), and in practice this means **coalescing stands
   down for allowlist-restricted requests** rather than misbehaving. Scoping the
   group key by user id would be more precise but changes the Redis key format;
   it's deliberately deferred. If you don't use allowlists (the default — the
   feature is off unless you set it), nothing here applies and behaviour is
   identical to v1.1.0.

**Context safety:** the `(ip, content, session)` context lives in a
greenlet-local that is set at the top of `stream_vod` and always reset at request
end — when the stream ends or is closed (including a response closed before it
started), or on the non-streaming/`except` paths. Combined with uWSGI/gevent
using a fresh greenlet per request, a prior request's context cannot bleed into
a later one.

## Redis keys used

- `vodcc:grp:{client_ip}:{content_uuid}` — hash: `refcount`, `reserved`,
  `profile_id`, `account_id`, `created_at`, `last_activity`, plus one
  `m:{session_id}` member count per member session (v1.3.0); TTL
  `GROUP_TTL_SECONDS` (60s), refreshed on reserve/release events **and
  periodically while a member session is streaming** (so a mid-playback **seek** —
  a new connection opening before the old one closes — finds the group and rides
  it instead of failing over). Deliberately distinct from
  the older `cedric-marcoux/dispatcharr_vod_fix` plugin's `vod_client_slot:`
  keys.

Native keys (`profile_connections:{id}`, `vod_persistent_connection:{session}`,
etc.) are untouched except through the unmodified native reserve/release calls.

---

## Acknowledgments

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
