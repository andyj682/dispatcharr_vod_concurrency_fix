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
              last_activity   (short TTL, refreshed on activity)

  1. stream_vod            -- set per-request (client_ip, content_uuid) context
                              in a greenlet-local, and wrap the streaming
                              generator so that context survives into the lazy
                              teardown (profile-release) path.
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

Why this is safe under Dispatcharr's runtime
--------------------------------------------
Dispatcharr runs uWSGI with `gevent = 400` + `gevent-early-monkey-patch`, so
`threading.local()` is greenlet-local: one in-flight request per greenlet at a
time, isolated per request. Each burst request is a *separate* session id => a
separate greenlet => its selection, reservation and release all happen in the
SAME greenlet. We set the context synchronously at the top of `stream_vod`
(covering selection + reservation + synchronous rollback releases) and
re-assert it inside a wrapper around the streaming generator (covering the lazy
teardown release). We never clear it in a premature `finally`.

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
"""

from __future__ import annotations

import logging
import os
import threading
import time

logger = logging.getLogger("plugins.dispatcharr_vod_concurrency_fix")

# --------------------------------------------------------------------------- #
# Tunables
# --------------------------------------------------------------------------- #

# How long a coalescing group survives without activity. Bounds leaks from
# abrupt client crashes that skip the release path, and the documented
# same-session edge case. Refreshed on every reserve/release touch.
GROUP_TTL_SECONDS = 30

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

# Greenlet-local (gevent-patched threading.local) per-request context + plan.
_local = threading.local()

_script_cache = {}


# --------------------------------------------------------------------------- #
# Lua scripts (atomic group accounting)
# --------------------------------------------------------------------------- #

# Join a group: increment refcount, refresh TTL, report refcount + whether the
# owner has already confirmed a real reservation. Returns {refcount, reserved}.
_LUA_JOIN = """
local k = KEYS[1]
local ttl = tonumber(ARGV[1])
local now = ARGV[2]
local rc = redis.call('HINCRBY', k, 'refcount', 1)
redis.call('HSET', k, 'last_activity', now)
if rc == 1 then
  redis.call('HSET', k, 'created_at', now)
end
redis.call('EXPIRE', k, ttl)
local reserved = redis.call('HGET', k, 'reserved')
if reserved == '1' then
  return {rc, 1}
end
return {rc, 0}
"""

# Undo a join (owner's real reservation failed, or a rider backing out to go
# independent). Deletes the group hash when it empties.
_LUA_UNDO = """
local k = KEYS[1]
local rc = redis.call('HINCRBY', k, 'refcount', -1)
if rc <= 0 then
  redis.call('DEL', k)
end
return rc
"""

# Mark a group confirmed and record the chosen profile/account (owner path).
_LUA_MARK_RESERVED = """
local k = KEYS[1]
local ttl = tonumber(ARGV[1])
if redis.call('EXISTS', k) == 0 then
  return 0
end
redis.call('HSET', k, 'reserved', '1', 'profile_id', ARGV[2], 'account_id', ARGV[3])
redis.call('EXPIRE', k, ttl)
return 1
"""

# Leave a group: decrement refcount. Returns {code, refcount}:
#   code =  1  -> this was the last member; caller must do the real release
#   code =  0  -> members remain; caller must NOT release
#   code = -1  -> group already gone (expired); caller does a best-effort release
_LUA_LEAVE = """
local k = KEYS[1]
local ttl = tonumber(ARGV[1])
local now = ARGV[2]
if redis.call('EXISTS', k) == 0 then
  return {-1, 0}
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
        }
        _script_cache[cache_key] = cached
    return cached


def _group_key(client_ip, content_uuid) -> str:
    return f"{GROUP_PREFIX}{client_ip}:{content_uuid}"


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


def _reset_ctx() -> None:
    _local.client_ip = None
    _local.content_uuid = None
    _local.plan = None
    _local.group_key = None


def _set_ctx(client_ip, content_uuid) -> None:
    _local.client_ip = client_ip
    _local.content_uuid = content_uuid
    _local.plan = None
    _local.group_key = None


def _snapshot_ctx() -> dict:
    return {
        "client_ip": getattr(_local, "client_ip", None),
        "content_uuid": getattr(_local, "content_uuid", None),
        "plan": getattr(_local, "plan", None),
        "group_key": getattr(_local, "group_key", None),
    }


def _restore_ctx(snap: dict) -> None:
    _local.client_ip = snap.get("client_ip")
    _local.content_uuid = snap.get("content_uuid")
    _local.plan = snap.get("plan")
    _local.group_key = snap.get("group_key")


# --------------------------------------------------------------------------- #
# Patched: _get_m3u_profile (selection)
# --------------------------------------------------------------------------- #

def patched_get_m3u_profile(m3u_account, profile_id, session_id=None):
    """Group-aware profile selection.

    If a confirmed group exists for this request's (client_ip, content_uuid):
      * and this candidate account IS the group's account -> return the group's
        chosen profile, bypassing the capacity check;
      * and this candidate account is NOT the group's account -> return None so
        the candidate loop skips it and stays pinned to the group's account
        (keeps the burst on one provider/file).
    Otherwise behave exactly like native selection.
    """
    if not _ACTIVE:
        return _orig_get_m3u_profile(m3u_account, profile_id, session_id)

    _log_pid_once("select")

    client_ip = getattr(_local, "client_ip", None)
    content_uuid = getattr(_local, "content_uuid", None)
    if client_ip is None or content_uuid is None or m3u_account is None:
        return _orig_get_m3u_profile(m3u_account, profile_id, session_id)

    try:
        redis_client = _get_redis()
        if redis_client is None:
            return _orig_get_m3u_profile(m3u_account, profile_id, session_id)

        gkey = _group_key(client_ip, content_uuid)
        raw = redis_client.hgetall(gkey)
        if raw:
            data = {_decode(k): _decode(v) for k, v in raw.items()}
            if data.get("reserved") == "1" and data.get("profile_id") and data.get("account_id"):
                group_account_id = str(data["account_id"])
                group_profile_id = str(data["profile_id"])

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

        return _orig_get_m3u_profile(m3u_account, profile_id, session_id)

    except Exception as exc:
        logger.error("[VOD-CC] selection error, falling back to native: %s", exc)
        try:
            return _orig_get_m3u_profile(m3u_account, profile_id, session_id)
        except Exception:
            raise


# --------------------------------------------------------------------------- #
# Patched: reserve
# --------------------------------------------------------------------------- #

def patched_reserve(self, m3u_profile):
    """Group-aware replacement for `_check_and_reserve_profile_slot`."""
    if not _ACTIVE:
        return _orig_reserve(self, m3u_profile)

    _log_pid_once("reserve")

    client_ip = getattr(_local, "client_ip", None)
    content_uuid = getattr(_local, "content_uuid", None)
    if client_ip is None or content_uuid is None or m3u_profile is None:
        return _orig_reserve(self, m3u_profile)
    try:
        max_streams = int(getattr(m3u_profile, "max_streams", 0) or 0)
    except (TypeError, ValueError):
        max_streams = 0
    if max_streams == 0:
        return _orig_reserve(self, m3u_profile)

    try:
        redis_client = getattr(self, "redis_client", None) or _get_redis()
        if redis_client is None:
            return _orig_reserve(self, m3u_profile)

        scripts = _get_scripts(redis_client)
        gkey = _group_key(client_ip, content_uuid)
        now = str(time.time())
        account_id = str(getattr(m3u_profile, "m3u_account_id", "") or "")

        rc, reserved = scripts["join"](keys=[gkey], args=[GROUP_TTL_SECONDS, now])
        rc = int(rc)
        reserved = int(reserved)

        if rc == 1:
            ok = _orig_reserve(self, m3u_profile)
            if ok:
                scripts["mark"](
                    keys=[gkey],
                    args=[GROUP_TTL_SECONDS, str(m3u_profile.id), account_id],
                )
                _local.plan = "GROUP"
                _local.group_key = gkey
                logger.info(
                    "[VOD-CC] group OWNER reserved profile %s (account %s) for %s/%s",
                    m3u_profile.id, account_id, client_ip, content_uuid,
                )
                return True
            scripts["undo"](keys=[gkey], args=[])
            _local.plan = "NONE"
            _local.group_key = gkey
            logger.info(
                "[VOD-CC] group OWNER at capacity for profile %s (%s/%s)",
                m3u_profile.id, client_ip, content_uuid,
            )
            return False

        if reserved == 1:
            _local.plan = "GROUP"
            _local.group_key = gkey
            logger.info(
                "[VOD-CC] group RIDER shares profile %s slot for %s/%s (members=%s)",
                m3u_profile.id, client_ip, content_uuid, rc,
            )
            return True

        # rc > 1 but owner not confirmed yet (sub-ms race the real burst doesn't
        # exhibit): back out and reserve independently -> never ride a phantom slot.
        scripts["undo"](keys=[gkey], args=[])
        ok = _orig_reserve(self, m3u_profile)
        _local.plan = "INDEP" if ok else "NONE"
        _local.group_key = gkey
        return ok

    except Exception as exc:
        logger.error("[VOD-CC] reserve error, falling back to native: %s", exc)
        try:
            return _orig_reserve(self, m3u_profile)
        except Exception:
            raise


# --------------------------------------------------------------------------- #
# Patched: decrement / release
# --------------------------------------------------------------------------- #

def patched_decrement(self, m3u_profile_id):
    """Group-aware replacement for `_decrement_profile_connections`."""
    if not _ACTIVE:
        return _orig_decrement(self, m3u_profile_id)

    plan = getattr(_local, "plan", None)
    gkey = getattr(_local, "group_key", None)
    if plan is None:
        return _orig_decrement(self, m3u_profile_id)

    # Consume the plan so a stray second decrement can't double-act.
    _local.plan = None
    _local.group_key = None

    try:
        if plan == "NONE":
            return None
        if plan == "INDEP":
            return _orig_decrement(self, m3u_profile_id)

        # plan == "GROUP"
        redis_client = getattr(self, "redis_client", None) or _get_redis()
        if redis_client is None or gkey is None:
            return _orig_decrement(self, m3u_profile_id)

        scripts = _get_scripts(redis_client)
        code, rc = scripts["leave"](
            keys=[gkey], args=[GROUP_TTL_SECONDS, str(time.time())]
        )
        code = int(code)
        if code == 1 or code == -1:
            logger.info(
                "[VOD-CC] group LAST member -> releasing profile %s", m3u_profile_id
            )
            return _orig_decrement(self, m3u_profile_id)

        logger.info(
            "[VOD-CC] group member left profile %s, %s remain (slot held)",
            m3u_profile_id, rc,
        )
        return None

    except Exception as exc:
        logger.error("[VOD-CC] decrement error, falling back to native: %s", exc)
        try:
            return _orig_decrement(self, m3u_profile_id)
        except Exception:
            raise


# --------------------------------------------------------------------------- #
# Patched: stream_vod (context carrier + generator wrapper)
# --------------------------------------------------------------------------- #

def _extract_request_and_content(args, kwargs):
    """stream_vod(request, content_type, content_id, session_id=None,
    profile_id=None, user=None)."""
    request = kwargs.get("request")
    if request is None and len(args) >= 1:
        request = args[0]
    content_id = kwargs.get("content_id")
    if content_id is None and len(args) >= 3:
        content_id = args[2]
    return request, content_id


def patched_stream_vod(*args, **kwargs):
    """Wrap `stream_vod`: establish (client_ip, content_uuid) context for the
    whole VOD request (selection + reservation), and re-assert it inside the
    streaming generator so it survives into the lazy release path."""
    if not _ACTIVE:
        return _orig_stream_vod(*args, **kwargs)

    _log_pid_once("stream")

    request, content_id = _extract_request_and_content(args, kwargs)
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

    _set_ctx(client_ip, content_uuid)

    try:
        response = _orig_stream_vod(*args, **kwargs)
    except Exception:
        _reset_ctx()
        raise

    streaming_content = getattr(response, "streaming_content", None)
    if streaming_content is None:
        # Non-streaming response (301 redirect, 429/503, error): any synchronous
        # rollback release already ran (consuming the plan). Clear and return.
        _reset_ctx()
        return response

    snap = _snapshot_ctx()
    native_gen = streaming_content

    def _wrapped_gen():
        _restore_ctx(snap)
        try:
            for chunk in native_gen:
                yield chunk
        finally:
            # On client disconnect the server closes THIS generator; native's
            # profile-release runs inside native_gen's GeneratorExit handler,
            # which without an explicit close here would only fire later at GC,
            # after we reset the context -- stripping the greenlet-local plan the
            # release needs (=> a leaked group refcount on the common client-stop
            # path). Close native_gen NOW, while context is still set.
            try:
                close = getattr(native_gen, "close", None)
                if close is not None:
                    close()
            finally:
                _reset_ctx()

    response.streaming_content = _wrapped_gen()
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
