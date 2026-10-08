"""
VOD concurrency-coalescing monkeypatch for current Dispatcharr.

Problem
-------
Emby / libmpv (and other MKV direct-play clients) open a large VOD file with a
short burst of 2-3 near-simultaneous HTTP range requests:

    1. GET (no Range / bytes=0-)      -> initial open
    2. GET Range: bytes=<near EOF>-   -> MKV Cues / SeekHead index
    3. GET Range: bytes=<near start>- -> playback start

Each request arrives at Dispatcharr's Xtream-Codes VOD endpoint WITHOUT a
Dispatcharr session id, so `stream_vod` mints a *fresh* session id per request
(via the 301 redirect). The three requests therefore reach profile selection as
three independent sessions.

The failover that maps a burst request onto a DIFFERENT provider file happens at
**profile selection**, not at reservation:

  * `_get_m3u_profile()` (apps/proxy/vod_proxy/views.py) checks
    `pool_has_capacity_for_profile()`, i.e. `profile_connections:{id}` vs
    `max_streams`. Request #1 reserves the slot (counter -> 1); request #2's
    selection reads counter == 1 == max_streams -> "All profiles at capacity"
    -> the candidate loop in `stream_vod` fails over to a different M3U account
    -> a different underlying file than the one already playing.
  * The reservation layer (`_check_and_reserve_profile_slot`) would also reject
    #2, but selection rejects it first, so a fix that only touches reservation
    never runs.

Fix (group-coalescing across selection + reservation)
-----------------------------------------------------
We recognise that several different session ids from the same
`(client_ip, content_uuid)` within a few seconds are the same logical client,
and let them share ONE provider connection slot / one file. This is done with a
small Redis "group" and four coordinated monkeypatches:

  group key:  vodcc:grp:{client_ip}:{content_uuid}
  fields:     refcount, reserved, profile_id, account_id, created_at,
              last_activity, m:{session_id} (per-session member count)
              (short TTL, refreshed on activity and while members stream)

  1. stream_vod            -- set per-request (client_ip, content_uuid,
                              session_id) context in a greenlet-local, and wrap
                              the streaming body so that context survives into
                              the lazy teardown (profile-release) path.
  2. _get_m3u_profile      -- if a confirmed group exists for this (ip, content),
                              return the group's already-chosen profile and BYPASS
                              the capacity check (and pin selection to the group's
                              account so a burst request can't land on a different
                              provider/file). This is what stops the failover.
  3. _check_and_reserve_profile_slot
                           -- first group member does the real native reservation
                              and records the chosen profile/account; later members
                              ride it (no second INCR). Keeps the counter correct.
  4. _decrement_profile_connections
                           -- refcounted release: the real native release fires
                              only when the LAST group member ends.

Each burst request still opens its OWN upstream provider socket (its own Range)
via its own RedisBackedVODConnection -> correct concurrent range reads; they
just share one profile-slot reservation and stay on one provider/file.

Session-level membership (v1.3.0)
---------------------------------
Group membership is counted per SESSION (`m:{session_id}` in the group hash),
not per request. Core reserves a profile slot once per session -- in the request
that takes the session's active_streams from 0 to 1 -- and releases it once per
session, in whichever request on that session is LAST out. Those can be two
different requests. Through v1.2.0 the plugin tracked membership per request (a
greenlet-local "plan"), which only lined up with core while every request had
its own session.

Dispatcharr v0.32.0 made the session-mint 301 cacheable, so clients that reuse
it (FFmpeg-based players) send a whole burst on ONE session. Then the request
that joined the group could finish first, and a sibling request with no plan of
its own did the native release outside the group -- leaving an orphan group
that a quick replay would ride without taking a slot (an uncounted stream).
Keying membership by session pairs our join/leave exactly the way core pairs
its reserve/release, whichever request does each.

Why this is safe under Dispatcharr's runtime
--------------------------------------------
Dispatcharr runs uWSGI with `gevent = 400` + `gevent-early-monkey-patch`, so
`threading.local()` is greenlet-local: one in-flight request per greenlet at a
time, isolated per request. Each request's selection, reservation and own
releases run in its own greenlet. We set the context synchronously at the top of
`stream_vod` (covering selection + reservation + synchronous rollback releases)
and re-assert it inside a wrapper around the streaming body (covering the lazy
teardown release). The context only identifies the request -- (ip, content,
session) -- while membership itself lives in Redis, so it does not matter which
of a session's requests ends up doing the release.

Timing: requests are ~250ms apart and each goes through a 301 redirect
round-trip, so the group owner has confirmed its reservation well before a rider
selects. The sub-millisecond simultaneous-arrival race (a rider joining before
the owner marks the group reserved) is handled conservatively -- the rider backs
out and does an independent native reservation (today's behaviour, never an
over-subscription).

Multi-worker: uWSGI runs 4 workers with lazy-apps; each imports an enabled
plugin at boot and applies these patches. Enable + restart to guarantee all
workers are patched; confirm from the per-worker `[VOD-CC] active ... pid=` logs.

This module is import-safe: any failure to locate/patch the current Dispatcharr
internals leaves native behaviour completely untouched, and any per-request
error falls back to native.

Signature-agnostic wrappers
---------------------------
That fail-open has one blind spot, found the hard way on Dispatcharr v0.31.0: it
cannot cover a changed *signature* on a patched function. Python raises
TypeError while binding arguments, before the wrapper body -- so before the
`if not _ACTIVE` guard and before the try/except. `stream_vod`'s blanket
`except Exception` then turned that into HTTP 500 on every VOD request.

So every wrapper here accepts `*args/**kwargs` and forwards them verbatim to the
original, which makes a newly-added upstream parameter a no-op instead of an
outage. Forwarding is as important as accepting: silently dropping a parameter
like v0.31.0's `restrict_to_profile_ids` would hand users profiles they are not
permitted to use -- worse than a crash, because nothing surfaces it.
"""

from __future__ import annotations

import logging
import os
import threading
import time

logger = logging.getLogger("plugins.dispatcharr_vod_concurrency_fix")


def _align_logger_level() -> None:
    """Give this plugin's logger its own level instead of inheriting root's.

    Dispatcharr's LOGGING dictConfig names every logger it manages -- `apps`,
    `apps.proxy`, `celery`, `core.tasks`, `django.geventpool`, root -- and gives
    each an explicit handler with `propagate: False`. **`plugins.*` is not in
    that list**, so a plugin logger has no handler, propagates to root, and its
    effective level IS root's.

    That is fine in uWSGI and daphne (root sits at DISPATCHARR_LOG_LEVEL, INFO
    by default), which is where everything this plugin patches runs -- so there
    is no exposure today. It is NOT fine in a Celery prefork child, where
    billiard reconfigures root and leaves it at WARNING: every plugin
    `logger.info()` is then discarded at the logger, while core's `apps.*` lines
    from the same process still appear because that logger has its own level.
    A working plugin becomes indistinguishable from an absent one -- a failure
    you can only notice as an absence, which is to say you don't.

    All of this plugin's diagnostics (`[VOD-CC] ...`, the per-worker residency
    lines) are INFO, so they are exactly what would vanish. Taken preventively:
    if this code ever runs somewhere root has been turned down, it stays visible.

    Adopt `apps`'s level rather than hard-coding INFO so DISPATCHARR_LOG_LEVEL
    keeps working, and only act when the level is NOTSET so anything that has
    already set one (a test silencing this logger, an operator debugging) keeps
    control.
    """
    if logger.level == logging.NOTSET:
        logger.setLevel(logging.getLogger("apps").getEffectiveLevel() or logging.INFO)


_align_logger_level()

# --------------------------------------------------------------------------- #
# Tunables
# --------------------------------------------------------------------------- #

# How long a coalescing group survives without activity. Bounds leaks from
# abrupt client crashes that skip the release path, and the documented
# same-session edge case. Refreshed on every reserve/release touch AND
# periodically while a stream is active (see GROUP_REFRESH_INTERVAL_SECONDS) so
# the group doesn't evaporate mid-playback and cause a seek to fail over.
GROUP_TTL_SECONDS = 60

# While a connection is streaming, refresh the group's TTL at most this often
# (one cheap EXPIRE) so the group stays alive for the whole playback. Must be
# comfortably below GROUP_TTL_SECONDS.
GROUP_REFRESH_INTERVAL_SECONDS = 15

# Redis key prefix. Deliberately distinct from the older cedric-marcoux plugin's
# `vod_client_slot:` keys so the two never collide if both are ever present.
GROUP_PREFIX = "vodcc:grp:"

# --------------------------------------------------------------------------- #
# Module state
# --------------------------------------------------------------------------- #

_ACTIVE = False

# Originals captured at install time (also tagged on the patched callables so a
# plugin reload can't capture an already-patched function as the "original").
_orig_stream_vod = None
_orig_get_m3u_profile = None
_orig_reserve = None
_orig_decrement = None

_pid_logged = set()

# Greenlet-local (gevent-patched threading.local) per-request context: which
# request this is -- (client_ip, content_uuid, session_id). Group MEMBERSHIP is
# not kept here; it lives in Redis, counted per session (see "Session-level
# membership" in the module docstring).
_local = threading.local()

_script_cache = {}


# --------------------------------------------------------------------------- #
# Lua scripts (atomic group accounting)
# --------------------------------------------------------------------------- #
#
# The group hash carries a total `refcount` plus one `m:{session_id}` count per
# member session. Core reserves once per session (the request that takes it
# 0 -> 1) and releases once per session (whichever request is last out), so we
# join and leave per session too. Each script starts with a `-- vodcc:<name>`
# tag; the offline test harness dispatches on it.

# Join a group as session ARGV[3]: bump the total refcount and that session's
# member count, refresh the TTL. Returns {refcount, reserved}.
_LUA_JOIN = """-- vodcc:join
local k = KEYS[1]
local ttl = tonumber(ARGV[1])
local now = ARGV[2]
local mf = ARGV[3]
local rc = redis.call('HINCRBY', k, 'refcount', 1)
redis.call('HINCRBY', k, mf, 1)
redis.call('HSET', k, 'last_activity', now)
if rc == 1 then
  redis.call('HSET', k, 'created_at', now)
end
redis.call('EXPIRE', k, ttl)
if redis.call('HGET', k, 'reserved') == '1' then
  return {rc, 1}
end
return {rc, 0}
"""

# Undo a join by session ARGV[1] (the owner's real reservation failed, or a
# rider is backing out to reserve independently). Deletes the hash when empty.
_LUA_UNDO = """-- vodcc:undo
local k = KEYS[1]
local mf = ARGV[1]
if redis.call('EXISTS', k) == 0 then
  return 0
end
if redis.call('HINCRBY', k, mf, -1) <= 0 then
  redis.call('HDEL', k, mf)
end
local rc = redis.call('HINCRBY', k, 'refcount', -1)
if rc <= 0 then
  redis.call('DEL', k)
end
return rc
"""

# Mark a group confirmed and record the chosen profile/account (owner path).
_LUA_MARK_RESERVED = """-- vodcc:mark
local k = KEYS[1]
local ttl = tonumber(ARGV[1])
if redis.call('EXISTS', k) == 0 then
  return 0
end
redis.call('HSET', k, 'reserved', '1', 'profile_id', ARGV[2], 'account_id', ARGV[3])
redis.call('EXPIRE', k, ttl)
return 1
"""

# Leave a group on behalf of session ARGV[3]. Returns {code, refcount}:
#   code =  1  -> that was the last member; caller does the real release
#   code =  0  -> members remain; caller must NOT release
#   code = -1  -> group already gone (expired); caller does a best-effort release
#   code = -2  -> this session is not a member (it reserved natively, or never
#                 joined); caller does the native release that pairs with it
_LUA_LEAVE = """-- vodcc:leave
local k = KEYS[1]
local ttl = tonumber(ARGV[1])
local now = ARGV[2]
local mf = ARGV[3]
if redis.call('EXISTS', k) == 0 then
  return {-1, 0}
end
local m = tonumber(redis.call('HGET', k, mf) or '0')
if m <= 0 then
  return {-2, tonumber(redis.call('HGET', k, 'refcount') or '0')}
end
if redis.call('HINCRBY', k, mf, -1) <= 0 then
  redis.call('HDEL', k, mf)
end
local rc = redis.call('HINCRBY', k, 'refcount', -1)
redis.call('HSET', k, 'last_activity', now)
if rc <= 0 then
  redis.call('DEL', k)
  return {1, rc}
end
redis.call('EXPIRE', k, ttl)
return {0, rc}
"""

# Keep a group alive while session ARGV[2] -- a member -- is streaming. Only a
# member's stream refreshes it, so an unrelated stream for the same (ip,
# content) cannot keep a stale group alive. EXPIRE on a missing key is never
# reached (HGET fails first), so this cannot resurrect a dead group.
_LUA_TOUCH = """-- vodcc:touch
local k = KEYS[1]
local ttl = tonumber(ARGV[1])
local mf = ARGV[2]
if tonumber(redis.call('HGET', k, mf) or '0') > 0 then
  redis.call('EXPIRE', k, ttl)
  return 1
end
return 0
"""


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _get_scripts(redis_client):
    cache_key = id(redis_client)
    cached = _script_cache.get(cache_key)
    if cached is None:
        cached = {
            "join": redis_client.register_script(_LUA_JOIN),
            "undo": redis_client.register_script(_LUA_UNDO),
            "mark": redis_client.register_script(_LUA_MARK_RESERVED),
            "leave": redis_client.register_script(_LUA_LEAVE),
            "touch": redis_client.register_script(_LUA_TOUCH),
        }
        _script_cache[cache_key] = cached
    return cached


def _group_key(client_ip, content_uuid) -> str:
    return f"{GROUP_PREFIX}{client_ip}:{content_uuid}"


def _member_field(session_id) -> str:
    return f"m:{session_id}"


def _get_redis():
    try:
        from core.utils import RedisClient
        return RedisClient.get_client()
    except Exception:
        return None


def _log_pid_once(where: str) -> None:
    key = f"{where}:{os.getpid()}"
    if key not in _pid_logged:
        _pid_logged.add(key)
        logger.info("[VOD-CC] active in worker pid=%s at %s", os.getpid(), where)


def _decode(v):
    return v.decode() if isinstance(v, (bytes, bytearray)) else v


def _as_int(value):
    """Redis hands ids back as strings; Dispatcharr's allowlists hold ints.

    Returns None when the value can't be read as an int, so callers fail closed
    (an id we cannot parse is an id we cannot prove is permitted).
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _reset_ctx() -> None:
    _local.client_ip = None
    _local.content_uuid = None
    _local.session_id = None


def _set_ctx(client_ip, content_uuid, session_id=None) -> None:
    _local.client_ip = client_ip
    _local.content_uuid = content_uuid
    _local.session_id = session_id


def _ctx():
    """(client_ip, content_uuid, session_id) for the current request."""
    return (
        getattr(_local, "client_ip", None),
        getattr(_local, "content_uuid", None),
        getattr(_local, "session_id", None),
    )


def _snapshot_ctx() -> dict:
    client_ip, content_uuid, session_id = _ctx()
    return {"client_ip": client_ip, "content_uuid": content_uuid,
            "session_id": session_id}


def _restore_ctx(snap: dict) -> None:
    _local.client_ip = snap.get("client_ip")
    _local.content_uuid = snap.get("content_uuid")
    _local.session_id = snap.get("session_id")


# --------------------------------------------------------------------------- #
# Patched: _get_m3u_profile (selection)
# --------------------------------------------------------------------------- #

def patched_get_m3u_profile(m3u_account, profile_id, session_id=None, *args, **kwargs):
    """Group-aware profile selection.

    If a confirmed group exists for this request's (client_ip, content_uuid):
      * and this candidate account IS the group's account -> return the group's
        chosen profile, bypassing the capacity check;
      * and this candidate account is NOT the group's account -> return None so
        the candidate loop skips it and stays pinned to the group's account
        (keeps the burst on one provider/file).
    Otherwise behave exactly like native selection.

    The `*args/**kwargs` tail is deliberate and load-bearing. Dispatcharr
    v0.31.0 added a `restrict_to_profile_ids` parameter and its caller passes it
    UNCONDITIONALLY as a keyword, so a wrapper with a fixed signature raises
    TypeError while binding arguments -- before `if not _ACTIVE` and before the
    try/except below, which is why this plugin's usual fail-open could not catch
    it and every VOD request 500'd. Accepting anything and forwarding it
    verbatim makes the next added parameter a no-op for us instead of an outage.
    Forwarding matters as much as accepting: silently dropping a selection
    constraint would hand users profiles they are not permitted to use, which is
    worse than a crash because it is invisible.
    """
    if not _ACTIVE:
        return _orig_get_m3u_profile(m3u_account, profile_id, session_id, *args, **kwargs)

    _log_pid_once("select")

    client_ip = getattr(_local, "client_ip", None)
    content_uuid = getattr(_local, "content_uuid", None)
    if client_ip is None or content_uuid is None or m3u_account is None:
        return _orig_get_m3u_profile(m3u_account, profile_id, session_id, *args, **kwargs)

    try:
        redis_client = _get_redis()
        if redis_client is None:
            return _orig_get_m3u_profile(m3u_account, profile_id, session_id, *args, **kwargs)

        gkey = _group_key(client_ip, content_uuid)
        raw = redis_client.hgetall(gkey)
        if raw:
            data = {_decode(k): _decode(v) for k, v in raw.items()}
            if data.get("reserved") == "1" and data.get("profile_id") and data.get("account_id"):
                group_account_id = str(data["account_id"])
                group_profile_id = str(data["profile_id"])

                # The group pin is an OPTIMISATION; a caller-supplied profile
                # allowlist is a CONSTRAINT, so the constraint wins. Read it
                # defensively -- core passes it by keyword, but positional is
                # cheap to support.
                restrict = kwargs.get("restrict_to_profile_ids")
                if restrict is None and args:
                    restrict = args[0]

                # One check covering BOTH branches below, because both bypass
                # core's own allowlist filtering:
                #   * group's account -- we must not hand back a profile this
                #     request may not use;
                #   * other accounts  -- the `return None` pin would otherwise
                #     skip every candidate the user CAN use, turning a
                #     restricted playback into "No available stream".
                # Note `restrict` is scoped to THIS candidate account, so for a
                # non-group account it can never contain the group's profile id
                # -- i.e. whenever the allowlist feature is in play the
                # cross-account pin correctly stands down and native selection
                # decides. Unrestricted requests (the default, and every
                # Dispatcharr before v0.31.0) short-circuit on `is None` and see
                # no behaviour change at all.
                if restrict is not None and _as_int(group_profile_id) not in restrict:
                    logger.info(
                        "[VOD-CC] group profile %s not permitted for this request; "
                        "abandoning pin, using native selection", group_profile_id,
                    )
                    return _orig_get_m3u_profile(
                        m3u_account, profile_id, session_id, *args, **kwargs
                    )

                if str(m3u_account.id) == group_account_id:
                    # This is the group's account: return its profile, bypass capacity.
                    try:
                        from apps.m3u.models import M3UAccountProfile
                        from apps.m3u.connection_pool import get_profile_connection_count
                        prof = M3UAccountProfile.objects.get(
                            id=int(group_profile_id),
                            m3u_account=m3u_account,
                            is_active=True,
                        )
                        count = get_profile_connection_count(prof, redis_client)
                        logger.info(
                            "[VOD-CC] selection: reusing group profile %s for "
                            "%s/%s (bypassing capacity, %s/%s)",
                            prof.id, client_ip, content_uuid, count, prof.max_streams,
                        )
                        return (prof, count)
                    except Exception:
                        # Group profile no longer loadable: fall through to native.
                        pass
                else:
                    # A confirmed group is pinned to a different account: skip this
                    # candidate so the loop reaches the group's account/file.
                    logger.info(
                        "[VOD-CC] selection: pinning %s/%s to group account %s, "
                        "skipping account %s",
                        client_ip, content_uuid, group_account_id, m3u_account.id,
                    )
                    return None

        return _orig_get_m3u_profile(m3u_account, profile_id, session_id, *args, **kwargs)

    except Exception as exc:
        logger.error("[VOD-CC] selection error, falling back to native: %s", exc)
        try:
            return _orig_get_m3u_profile(m3u_account, profile_id, session_id, *args, **kwargs)
        except Exception:
            raise


# --------------------------------------------------------------------------- #
# Patched: reserve
# --------------------------------------------------------------------------- #

def patched_reserve(self, m3u_profile, *args, **kwargs):
    """Group-aware replacement for `_check_and_reserve_profile_slot`.

    Joins this request's (ip, content) group on behalf of its SESSION. The first
    member does the real native reservation; later members ride it. A request
    with no session id (it cannot be grouped safely) reserves natively.

    Core's call sites pass one positional argument today, but "matches today" is
    not durable -- see the note on `patched_get_m3u_profile`. Accepting and
    forwarding anything extra costs nothing and removes this hook as a future
    break surface.
    """
    if not _ACTIVE:
        return _orig_reserve(self, m3u_profile, *args, **kwargs)

    _log_pid_once("reserve")

    client_ip, content_uuid, session_id = _ctx()
    if (client_ip is None or content_uuid is None or session_id is None
            or m3u_profile is None):
        return _orig_reserve(self, m3u_profile, *args, **kwargs)
    try:
        max_streams = int(getattr(m3u_profile, "max_streams", 0) or 0)
    except (TypeError, ValueError):
        max_streams = 0
    if max_streams == 0:
        return _orig_reserve(self, m3u_profile, *args, **kwargs)

    try:
        redis_client = getattr(self, "redis_client", None) or _get_redis()
        if redis_client is None:
            return _orig_reserve(self, m3u_profile, *args, **kwargs)

        scripts = _get_scripts(redis_client)
        gkey = _group_key(client_ip, content_uuid)
        mf = _member_field(session_id)
        now = str(time.time())
        account_id = str(getattr(m3u_profile, "m3u_account_id", "") or "")

        rc, reserved = scripts["join"](keys=[gkey], args=[GROUP_TTL_SECONDS, now, mf])
        rc = int(rc)
        reserved = int(reserved)

        if rc == 1:
            ok = _orig_reserve(self, m3u_profile, *args, **kwargs)
            if ok:
                scripts["mark"](
                    keys=[gkey],
                    args=[GROUP_TTL_SECONDS, str(m3u_profile.id), account_id],
                )
                logger.info(
                    "[VOD-CC] group OWNER reserved profile %s (account %s) for %s/%s",
                    m3u_profile.id, account_id, client_ip, content_uuid,
                )
                return True
            scripts["undo"](keys=[gkey], args=[mf])
            logger.info(
                "[VOD-CC] group OWNER at capacity for profile %s (%s/%s)",
                m3u_profile.id, client_ip, content_uuid,
            )
            return False

        if reserved == 1:
            logger.info(
                "[VOD-CC] group RIDER shares profile %s slot for %s/%s (members=%s)",
                m3u_profile.id, client_ip, content_uuid, rc,
            )
            return True

        # rc > 1 but the owner hasn't confirmed yet (a sub-millisecond race the
        # real burst doesn't exhibit): back out and reserve independently, so we
        # never ride a phantom slot. This session is then not a member, so its
        # release finds no membership and goes native -- pairing with this
        # native reservation.
        scripts["undo"](keys=[gkey], args=[mf])
        return _orig_reserve(self, m3u_profile, *args, **kwargs)

    except Exception as exc:
        logger.error("[VOD-CC] reserve error, falling back to native: %s", exc)
        try:
            return _orig_reserve(self, m3u_profile, *args, **kwargs)
        except Exception:
            raise


# --------------------------------------------------------------------------- #
# Patched: decrement / release
# --------------------------------------------------------------------------- #

def patched_decrement(self, m3u_profile_id, *args, **kwargs):
    """Group-aware replacement for `_decrement_profile_connections`.

    Leaves the group on behalf of this request's SESSION, whichever of the
    session's requests core happens to release from. The real native release
    fires only when the last member leaves; a session that never joined (or a
    group that has already expired) releases natively. Outside a request
    context -- core's background stale-connection cleanup -- it is native.

    Same signature-agnostic hardening as `patched_reserve`.
    """
    if not _ACTIVE:
        return _orig_decrement(self, m3u_profile_id, *args, **kwargs)

    client_ip, content_uuid, session_id = _ctx()
    if client_ip is None or content_uuid is None or session_id is None:
        return _orig_decrement(self, m3u_profile_id, *args, **kwargs)

    try:
        redis_client = getattr(self, "redis_client", None) or _get_redis()
        if redis_client is None:
            return _orig_decrement(self, m3u_profile_id, *args, **kwargs)

        scripts = _get_scripts(redis_client)
        code, rc = scripts["leave"](
            keys=[_group_key(client_ip, content_uuid)],
            args=[GROUP_TTL_SECONDS, str(time.time()), _member_field(session_id)],
        )
        code = int(code)

        if code == 0:
            logger.info(
                "[VOD-CC] group member left profile %s, %s remain (slot held)",
                m3u_profile_id, rc,
            )
            return None
        if code == 1:
            logger.info(
                "[VOD-CC] group LAST member -> releasing profile %s", m3u_profile_id
            )
        elif code == -1:
            logger.info(
                "[VOD-CC] group already gone -> releasing profile %s natively",
                m3u_profile_id,
            )
        return _orig_decrement(self, m3u_profile_id, *args, **kwargs)

    except Exception as exc:
        logger.error("[VOD-CC] decrement error, falling back to native: %s", exc)
        try:
            return _orig_decrement(self, m3u_profile_id, *args, **kwargs)
        except Exception:
            raise


# --------------------------------------------------------------------------- #
# Patched: stream_vod (context carrier + streaming-body wrapper)
# --------------------------------------------------------------------------- #

class _ContextStream:
    """Wraps core's streaming body to carry this request's context.

    Two jobs:

    * Keep (ip, content, session) set while the body runs, so core's lazy
      teardown release -- which runs inside that body -- reaches
      `patched_decrement` with the right session.
    * Refresh the group's TTL while bytes flow, but only if this request's
      session is a member. A single steady playback generates no
      reserve/release events, so without this the group would expire mid-watch
      and a later seek would fail over (fixed in v1.1.0). In v0.32.0+ that
      steady stream can be a sibling request on the member session rather than
      the one that joined, which is why the check is by session.

    Teardown ordering, stated precisely: Django's `StreamingHttpResponse` keeps a
    list of closers and runs them in registration order, swallowing exceptions.
    Core's body registered its closer when core built the response; ours
    registers when `patched_stream_vod` assigns `streaming_content` afterwards.
    So on every close Django runs core's teardown first -- with our context
    still set -- and ours second. We never close core's body ourselves: the
    response's `streaming_content` getter returns a `map` over it, which has no
    `close`, so there is no handle to close, and Django already does it. (Before
    v1.3.0 the comment here claimed an explicit close did this; it never did.)

    `close()` also resets the context when the body never started. Since
    Dispatcharr v0.32.0 core releases an unstarted response's reservation from
    its own closer, which runs first, so the release still sees our context.
    """

    def __init__(self, native, snap, gkey):
        self._native = iter(native)
        self._snap = snap
        self._gkey = gkey
        self._member_field = (_member_field(snap["session_id"])
                              if snap.get("session_id") else None)
        self._redis = None
        self._started = False
        self._done = False
        self._last_refresh = 0.0

    def __iter__(self):
        return self

    def __next__(self):
        if self._done:
            raise StopIteration
        if not self._started:
            self._started = True
            _restore_ctx(self._snap)
            if self._member_field:
                self._redis = _get_redis()
            self._last_refresh = time.time()
        try:
            chunk = next(self._native)
        except BaseException:
            # Exhausted or failed: core's own teardown already ran inside the
            # body, with our context set. Now it is safe to clear.
            self._finish()
            raise
        self._maybe_refresh()
        return chunk

    def _maybe_refresh(self):
        if self._redis is None:
            return
        now = time.time()
        if now - self._last_refresh < GROUP_REFRESH_INTERVAL_SECONDS:
            return
        self._last_refresh = now
        try:
            _get_scripts(self._redis)["touch"](
                keys=[self._gkey], args=[GROUP_TTL_SECONDS, self._member_field]
            )
        except Exception:
            pass

    def _finish(self):
        if not self._done:
            self._done = True
            _reset_ctx()

    def close(self):
        self._finish()


def _extract_stream_args(args, kwargs):
    """stream_vod(request, content_type, content_id, session_id=None,
    profile_id=None, user=None) -> (request, content_id, session_id)."""
    request = kwargs.get("request")
    if request is None and len(args) >= 1:
        request = args[0]
    content_id = kwargs.get("content_id")
    if content_id is None and len(args) >= 3:
        content_id = args[2]
    session_id = kwargs.get("session_id")
    if session_id is None and len(args) >= 4:
        session_id = args[3]
    return request, content_id, session_id


def patched_stream_vod(*args, **kwargs):
    """Wrap `stream_vod`: establish (client_ip, content_uuid, session_id) context
    for the whole VOD request (selection + reservation), and carry it into the
    streaming body so it survives into the lazy release path."""
    if not _ACTIVE:
        return _orig_stream_vod(*args, **kwargs)

    _log_pid_once("stream")

    request, content_id, session_id = _extract_stream_args(args, kwargs)
    client_ip = None
    content_uuid = None
    try:
        if request is not None:
            from apps.proxy.vod_proxy.utils import get_client_info
            client_ip = get_client_info(request)[0]
        if content_id is not None:
            content_uuid = str(content_id)
    except Exception:
        client_ip = content_uuid = None

    if client_ip is None or content_uuid is None:
        _reset_ctx()
        return _orig_stream_vod(*args, **kwargs)

    # No session id = the first request, which core answers with a 301 that
    # mints one; nothing is reserved on it, so grouping never needs it.
    _set_ctx(client_ip, content_uuid, str(session_id) if session_id else None)

    try:
        response = _orig_stream_vod(*args, **kwargs)
    except Exception:
        _reset_ctx()
        raise

    streaming_content = getattr(response, "streaming_content", None)
    if streaming_content is None:
        # Non-streaming response (301 redirect, 429/503, error): any synchronous
        # rollback release has already run. Clear and return.
        _reset_ctx()
        return response

    response.streaming_content = _ContextStream(
        streaming_content, _snapshot_ctx(), _group_key(client_ip, content_uuid)
    )
    return response


# --------------------------------------------------------------------------- #
# Install / uninstall
# --------------------------------------------------------------------------- #

_PATCH_TAG = "_vodcc_patched"


def install() -> bool:
    """Install the monkeypatches. Idempotent and reload-safe."""
    global _orig_stream_vod, _orig_get_m3u_profile, _orig_reserve, _orig_decrement, _ACTIVE

    try:
        from apps.proxy.vod_proxy import views as vod_views
        from apps.proxy.vod_proxy.multi_worker_connection_manager import (
            MultiWorkerVODConnectionManager as M,
        )
    except Exception as exc:
        logger.error("[VOD-CC] could not import Dispatcharr VOD internals: %s", exc)
        return False

    # Verify the internals still look the way we expect; otherwise do NOT patch.
    if not hasattr(vod_views, "stream_vod") or not hasattr(vod_views, "_get_m3u_profile"):
        logger.error("[VOD-CC] views.stream_vod / _get_m3u_profile missing -- not patching.")
        return False
    for attr in ("_check_and_reserve_profile_slot", "_decrement_profile_connections"):
        if not hasattr(M, attr):
            logger.error("[VOD-CC] %s missing on manager -- not patching.", attr)
            return False

    try:
        cur = vod_views.stream_vod
        if not getattr(cur, _PATCH_TAG, False):
            _orig_stream_vod = cur
        cur = vod_views._get_m3u_profile
        if not getattr(cur, _PATCH_TAG, False):
            _orig_get_m3u_profile = cur
        cur = M._check_and_reserve_profile_slot
        if not getattr(cur, _PATCH_TAG, False):
            _orig_reserve = cur
        cur = M._decrement_profile_connections
        if not getattr(cur, _PATCH_TAG, False):
            _orig_decrement = cur

        for fn in (patched_stream_vod, patched_get_m3u_profile,
                   patched_reserve, patched_decrement):
            setattr(fn, _PATCH_TAG, True)

        vod_views.stream_vod = patched_stream_vod
        vod_views._get_m3u_profile = patched_get_m3u_profile
        M._check_and_reserve_profile_slot = patched_reserve
        M._decrement_profile_connections = patched_decrement

        _ACTIVE = True
        logger.info(
            "[VOD-CC] installed VOD concurrency-coalescing patch in worker pid=%s",
            os.getpid(),
        )
        return True
    except Exception as exc:
        logger.exception("[VOD-CC] install failed: %s", exc)
        uninstall()
        return False


def uninstall() -> bool:
    """Revert the monkeypatches (best effort) and deactivate."""
    global _ACTIVE
    _ACTIVE = False
    try:
        from apps.proxy.vod_proxy import views as vod_views
        from apps.proxy.vod_proxy.multi_worker_connection_manager import (
            MultiWorkerVODConnectionManager as M,
        )
    except Exception:
        return False

    try:
        if _orig_stream_vod is not None:
            vod_views.stream_vod = _orig_stream_vod
        if _orig_get_m3u_profile is not None:
            vod_views._get_m3u_profile = _orig_get_m3u_profile
        if _orig_reserve is not None:
            M._check_and_reserve_profile_slot = _orig_reserve
        if _orig_decrement is not None:
            M._decrement_profile_connections = _orig_decrement
        logger.info("[VOD-CC] uninstalled patch in worker pid=%s", os.getpid())
        return True
    except Exception as exc:
        logger.error("[VOD-CC] uninstall error: %s", exc)
        return False
