# VOD concurrency fix — design resolution

Grounded in Dispatcharr `main` (cloned 2026-07-27, commit `1835d24`).
Re-verified against **v0.31.0** for plugin v1.2.0: both manager hooks are
byte-identical to v0.30.0, and `_get_m3u_profile` gained one parameter — see
"Signature-agnostic wrappers" and "Per-user profile allowlists" below.
Re-verified against **v0.32.0** for plugin v1.3.0: every hook is still
byte-identical, but v0.32.0 changed how a burst can arrive (one session instead
of one per request, for clients that reuse its now-cacheable session redirect)
— see "Session-level membership (v1.3.0)" below.

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
fields:     refcount, reserved, profile_id, account_id, created_at, last_activity,
            m:{session_id} (per-session member count, v1.3.0)
TTL:        GROUP_TTL_SECONDS (60s), refreshed on reserve/release AND
            periodically while streaming (see "Seek coverage" below)
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
  never release. Membership is counted per *session*, so it does not matter
  which of a session's requests core joins or releases from (v1.3.0). Verified in
  `test_logic.py` (burst, partial failure, shared session, create race).
- **Partial failure:** native calls `_decrement_profile_connections` on every
  rollback path (416, create-failure, mid-stream error) and at teardown; our
  version maps each onto a group leave, so a failed burst member leaves the group
  and the slot stays held for the survivors.
- **Owner finishing first:** the no-Range probe (#1) often ends before playback
  (#2/#3). Release is tied to *last-out*, not to the owner, so the slot is not
  freed early. Verified.

## Session-level membership (v1.3.0)

Core does its profile accounting **per session**: the request that takes a
session's `active_streams` from 0 to 1 reserves the slot; later requests on the
same session are siblings and reserve nothing (`manager:1394`); whichever request
on the session is **last out** releases it — from the body's teardown, or, since
v0.32.0, from `_ClosingVODStream`'s closer if the body never started. The
reserving request and the releasing request can be different.

Through v1.2.0 the plugin tracked group membership **per request** (a
greenlet-local "plan" set by the reserve and consumed by the release). That only
lined up with core while every request had its own session — true for every
client before v0.32.0, because each burst request arrived at the bare URL and was
minted a new session.

v0.32.0 made the session-mint 301 cacheable (`Cache-Control: private,
max-age=21600`) so that FFmpeg-based players keep it for the life of one open.
For those clients a whole burst now arrives on **one** session, and the two
models diverge:

1. The request that joined the group (the short no-Range open) finishes first.
   It isn't last out, so core doesn't release, and its plan dies with it.
2. A sibling request, last out, releases — but it has no plan, so the release
   went natively, outside the group.
3. The group is left behind: `refcount 1`, `reserved 1`, with no session behind
   it.
4. A stop-and-replay within the group TTL is a new open, so a new session. It
   joins the leftover group as a rider and takes **no** slot: an uncounted stream
   on a profile core believes is free. Each such replay refreshes the leftover
   group, so it can keep itself alive.

This was reproduced in a simulation driving the real v1.2.0 code (stock
v0.32.0: counter 1 during the replay; v1.2.0: 0, with the leftover group still
there).

**Fix:** membership is counted per session, as `m:{session_id}` in the group hash
alongside the total `refcount`. A reserve joins on behalf of its request's
session; a release leaves on behalf of its request's session — so the join and
leave pair exactly the way core's reserve and release do, whichever request
performs each. The session id comes from `stream_vod`'s own argument, which is
what core hands the connection manager. Release outcomes:

| Leave result | Meaning | Action |
|---|---|---|
| last member | group empties | native release |
| members remain | slot still in use | none |
| not a member | this session reserved natively (the sub-millisecond race, an unlimited profile, or the plugin off at reserve time) | native release, pairing with that native reserve |
| group gone | expired | native release (best effort, as before) |

Two related changes ride along:

- **TTL refresh is gated on membership by session.** v1.1.0 refreshed the group
  while the *request that joined* streamed. On a shared session that request is
  usually the short one and the long playback is a sibling, so the refresh is now
  done by any request whose **session** is a member — and only by those, so an
  unrelated stream for the same `(ip, content)` can't keep a stale group alive.
- **A request with no session id is never grouped.** It can't be paired safely, so
  it reserves and releases natively. In practice only core's session-minting 301
  lacks one, and it reserves nothing.

**Emby is unaffected by the v0.32.0 change.** Its burst comes from the Emby
server's own HTTP client (no `User-Agent`, so not FFmpeg), which never reuses the
cached redirect, so each request still gets its own session and v1.2.0 behaved
correctly there (confirmed live on v0.32.0). v1.3.0 is for clients that *do*
reuse it. Both shapes are covered in `test_logic.py`, which models core's v0.32.0
session accounting rather than assuming it.

**v0.34.x session resume.** A request without a session id now goes through
`_find_resumable_vod_session` in every mode: an idle session with the same
fingerprint (content, client IP, User-Agent, user) is resumed at once, or a
running one is given up to 1 s to close and then resumed; a running session is
never joined. Resume only *checks* that a session is idle — it doesn't claim it —
so two session-less requests can both be 301'd to the same session. That makes
the shared-session shape above reachable for Emby too, not only for clients
that reuse the cached redirect. Nothing new is needed: the 301 reserves nothing
and passes through untouched, the resumed session rejoins (or re-owns) the group
as any session does, and two resumers count as one member.
`test_core_session_resume_v0_34` covers two resumers on one session while
another member session still streams, plus a resume after a full stop.

The Lua scripts are exercised offline only through Python emulators; run
`tools/lua_selftest.py` inside the container to check them against the real
Redis after changing them.

## Seek coverage (v1.1.0)

The same over-count bites on a **seek**: the player opens a new range request
before the old connection closes, so for a moment there are two connections for
one (ip, content). Coalescing already handles that *if the group is alive* — the
new request rides. The gap (fixed in v1.1.0): the group key's TTL was only
refreshed on reserve/release events, and a single steady playback connection
generates none, so after `GROUP_TTL_SECONDS` the group would expire *mid-watch*
while the stream still held the provider slot. A seek after that point found no
group → became a fresh reservation → hit the capacity wall → failed over.
(Confirmed live: a seek 61s into steady playback logged `All profiles at
capacity` + a fresh `group OWNER` on the failover account, no `group RIDER`.)

Fix: the streaming generator refreshes the group's TTL periodically
(`GROUP_REFRESH_INTERVAL_SECONDS`, one cheap `EXPIRE`, throttled) so the group
stays alive as long as any connection is streaming; `EXPIRE` no-ops on a missing
key so it can only extend a live group, never resurrect a dead one. Base TTL
raised to 60s for pause cushion. Residual edge: a *pause* longer than the TTL
with the connection held open (no bytes flowing → no refresh) could still let the
group lapse; rare, and no worse than pre-v1.1.0.

## Signature-agnostic wrappers (v1.2.0)

Dispatcharr v0.31.0 changed `_get_m3u_profile`:

```
v0.30.0:  _get_m3u_profile(m3u_account, profile_id, session_id=None)
v0.31.0:  _get_m3u_profile(m3u_account, profile_id, session_id=None,
                           restrict_to_profile_ids=None)
```

Its one production caller, `_select_vod_stream`, passes the new parameter **as a
keyword, unconditionally** — the *value* is usually `None`, but the keyword is
always present in the call expression. Our wrapper's fixed three-parameter
signature therefore raised `TypeError`, and `stream_vod` wraps its whole body in
a blanket `except Exception` that returns a 500. Result: **every VOD playback
request 500'd** while the plugin was enabled on v0.31.0.

The important lesson is about the *failure mode*, not the parameter. This
plugin's safety story was "any error falls back to native," but argument binding
happens **before the function body**, so neither the `if not _ACTIVE` guard nor
the wrapper's `try/except` ever ran. A signature mismatch is the one class of
upstream change that a fail-open monkeypatch cannot absorb — and it converts the
usual quiet "plugin silently stopped helping" into a hard outage. Disabling the
plugin's own feature setting wouldn't have helped either, for the same reason;
only fully disabling the plugin (which restores the original function object)
did.

So every wrapper now ends its signature with `*args, **kwargs` and forwards them
verbatim to the original — `patched_get_m3u_profile`, and also `patched_reserve`
/ `patched_decrement`, whose signatures still match core but which are the same
kind of exposure. Forwarding matters as much as accepting: swallowing a
selection constraint would silently hand users profiles they aren't permitted to
use, which is worse than a crash because nothing surfaces it.

`test_logic.py` now carries a structural guard for this (an unrecognised
argument must be accepted *and* observed arriving at the original) plus a test
pinning the wrapper against v0.30.0's older signature, so one build serves both.
This matters because the previous suite **passed green while the plugin was
crashing every request in the field** — its fakes carried the old signature, so
it was structurally incapable of seeing the break. A logic test that mirrors
only the call shapes you already handle cannot detect a compatibility break; the
fakes have to track upstream's signature.

## Per-user profile allowlists (v0.31.0)

v0.31.0 also added a per-user M3U profile allowlist. `get_allowed_m3u_profiles`
reads `user.custom_properties.allowed_m3u_profile_ids` as a tri-state (absent →
unrestricted, `[]` → allow nothing, `[ids]` → those active profiles, malformed →
fails closed) and returns profiles **grouped by account**; `_select_vod_stream`
then derives a per-candidate-account `restrict_to_profile_ids`, so what reaches
selection is `None` or a set of int profile ids.

The group/rider path returns the pinned profile **directly**, bypassing core's
selection and therefore its allowlist filtering, so it has to apply the
constraint itself. The pin is an optimisation; the allowlist is a constraint, so
the constraint wins:

```python
restrict = kwargs.get("restrict_to_profile_ids")
if restrict is None and args:
    restrict = args[0]
if restrict is not None and _as_int(group_profile_id) not in restrict:
    # abandon the pin; let native selection decide
```

One check, placed before both branches, because both bypass core's filtering:

- **the group's own account** — we must not hand back a profile this request may
  not use;
- **any other account** (the `return None` pin) — if the skip weren't gated, a
  restricted user could have every candidate they *can* use skipped, turning a
  playable request into "No available stream".

Two details worth stating because both are silent when wrong:

- `group_profile_id` comes out of Redis as a **string** while the allowlist holds
  **ints**. Without the `int()` cast the check always fails, the pin is always
  abandoned, and the plugin quietly stops doing its job — no error anywhere.
- `restrict` is scoped to the **candidate account** being tested, so for a
  non-group account it can never contain the group account's profile id. The
  practical consequence: whenever allowlists are in play, the cross-account pin
  stands down and native selection decides. That is the intended graceful
  degradation, not an oversight — correctly serving a restricted user matters
  more than coalescing them.

Cost when the feature is unused — the default, and every Dispatcharr before
v0.31.0 — is one `is None` identity comparison per call, and behaviour is
unchanged.

**Known limitation, deliberately not fixed here.** The group key is
`(client_ip, content_uuid)` — **user-agnostic**. Two users behind one client IP
with different allowlists share a group. The check above degrades that
gracefully and per-request, which is the right trade-off. Scoping the group key
by user id would be more precise, but it changes the Redis key format and needs
a migration story, so it is out of scope for a compatibility release.

## Why context threading is safe (the part the old plugin got wrong)

Dispatcharr runs uWSGI with `gevent = 400` + `gevent-early-monkey-patch`, so
`threading.local()` is **greenlet-local** — one in-flight request per greenlet.
Each request's selection, reservation and own releases run in its own greenlet.
Context — which request this is: `(client_ip, content_uuid, session_id)` — is set
synchronously at the top of `stream_vod` (covering selection + reservation +
synchronous rollback releases) and carried inside a wrapper around the streaming
body (covering the lazy teardown release). Membership itself lives in Redis, so
the context only has to identify the session.

**Teardown ordering, stated precisely (corrected in v1.3.0).** Django's
`StreamingHttpResponse` keeps a list of closers and runs them in registration
order, swallowing exceptions. Core's body registered its closer when core built
the response; ours registers when we assign `streaming_content` afterwards. So on
every close — client stop, completion, or (v0.32.0+) a response closed before its
body started — Django runs core's teardown first, with our context still set, and
ours second, which then clears the context. Through v1.2.0 this section and the
code comment claimed the wrapper *explicitly* closed the native generator first.
It never did: the response's `streaming_content` getter returns a `map` over the
body, which has no `close`, so that call was a no-op and the ordering was Django's
all along. The suite now models Django 6.0.8's response faithfully, and the rider
case of `test_teardown_through_django_response` would fail if the order were
ever reversed.

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
- Fail-safe, **with one documented exception**: if the internals go missing,
  `install()` refuses to patch, and any per-request error falls back to native,
  so the failure mode is "no coalescing," not "broken streaming." That does
  **not** hold for a changed *signature* on a patched function — see
  "Signature-agnostic wrappers" below.

## Adjacent upstream machinery to watch

v0.31.0's connection manager grew a `_resolve_effective_profile(existing_state,
fallback_profile, ...)` that prefers the profile already bound on the **session
hash** over the view's pick, "so reserve and teardown always target the same
counter". For a *new* session the stored id is `None` and it returns the view's
pick — i.e. ours — so the group pin still wins. It only overrides for a session
that already has a different profile bound (reconnect, idle-session adoption,
create race), and it is now the one place upstream can override our selection.

When it *does* override, it calls our patched `reserve`/`decrement` with a
profile the group never pinned, in three distinct shapes:

- **create race** (`manager:1313-1314`) — the only site where our pick was
  already reserved earlier in the same request (`manager:1233`), so upstream
  decrements our profile and reserves its own back to back, on the same
  session: the decrement leaves the group and the reserve rejoins it.
- **reconnect / idle reuse** (`manager:1341`, `:1361`) — no prior reserve in the
  request; upstream reserves its effective profile directly while the group is
  pinned to another.

All three are covered by `test_upstream_effective_profile_override`, which
asserts the property that actually matters: **the native counter returns to 0 and
native reserve/release stay symmetric**, whatever upstream re-points. They do —
the group's refcounting absorbs the override, because a rider never touches the
native counter in either direction, so the group still takes exactly one native
reservation and releases exactly one. (This was initially suspected to cause
counter drift; the test disproves that, and is mutation-checked — a
double-counting rider fails it.)

One semantic consequence is worth stating, since the accounting hides it: in the
reconnect shape the group holds the *pinned* profile's slot while the request
streams from the *effective* profile, so that profile carries one uncounted
provider connection. This is the same under-counting trade-off already documented
for two devices sharing `(IP, title)` — balanced books, one unattributed
connection — and it is bounded by the group's lifetime.

`_resolve_effective_profile` logs nothing when it overrides, so it cannot be
observed directly in a live log; only its effect on the counters can. A live
check is therefore "play, reconnect/seek, confirm `profile_connections:*` all
return to 0", not a grep.

## Multi-worker propagation

uWSGI: 4 workers, `lazy-apps = true`. Each imports an enabled plugin at boot and
applies the patches. Enable + **restart** to guarantee all workers; confirm from
the per-worker `[VOD-CC] active … pid=` logs. Runtime enable-without-restart
relies on the loader's `.reload_token` pull and isn't guaranteed prompt.

## Logger level (why the plugin sets its own)

Dispatcharr's `LOGGING` dictConfig names every logger it manages — `apps`,
`apps.proxy`, `celery`, `core.tasks`, `django.geventpool`, root — and gives each
an explicit handler with `propagate: False`. **`plugins.*` is not in that list**,
so a plugin logger has no handler of its own, propagates to root, and its
effective level *is* root's.

That is fine everywhere this plugin actually runs: uWSGI and daphne keep root at
`DISPATCHARR_LOG_LEVEL` (INFO by default). It is not fine in a Celery prefork
child, where billiard reconfigures root and leaves it at WARNING — every plugin
`logger.info()` is then dropped at the logger, while core's `apps.*` lines from
the same process still appear because that logger carries its own level. Since
all of this plugin's diagnostics are INFO, the result would be a working plugin
that looks like an absent one.

So `patch.py` gives the logger an explicit level at import, adopting `apps`'s
level (which keeps `DISPATCHARR_LOG_LEVEL` effective) and only when the level is
still `NOTSET` (so a test or an operator that already set one keeps control).

**This is preventive, not a fix for an observed problem here** — nothing this
plugin patches runs in a prefork child. It is worth having anyway because the
failure mode is an *absence*: you would only learn you needed it by failing to
find a log line, which is indistinguishable from the plugin not being loaded.
That exact confusion cost a sibling plugin a long investigation. The proper fix
is upstream — one `"plugins"` entry in `LOGGING["loggers"]` — but the
plugin-side guard works on releases that predate it.

## Upstreaming note

The equivalent logic could land directly in `_get_m3u_profile` /
`reserve_profile_slot` / `release_profile_slot` given a `(client_ip,
content_uuid)` context threaded from `stream_vod`, removing the greenlet-local
machinery. The plugin is the **test vehicle**, not necessarily the final shape.
