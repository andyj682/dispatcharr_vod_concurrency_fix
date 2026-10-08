"""
Self-contained logic test for the VOD concurrency-coalescing patch (4 patches).

Runs WITHOUT Dispatcharr, Django or a real Redis. It:
  * emulates the plugin's Lua scripts in plain Python (valid because Redis runs
    each script atomically on a single thread, which a single-threaded test
    reproduces),
  * injects fake `core.utils`, `apps.m3u.models`, `apps.m3u.connection_pool`,
    `apps.proxy.vod_proxy.utils` modules so the lazily imported bits of patch.py
    resolve,
  * stubs the native reserve/release/select with simple counters that enforce
    max_streams,
  * models the three runtime facts the plugin's correctness rests on:
      - Django 6.0.8's `StreamingHttpResponse` (sync mode): the
        `streaming_content` getter returns a `map` with no close(); assigning
        it registers the value's close() as a resource closer; close() runs the
        closers in registration order and swallows their exceptions;
      - Dispatcharr v0.32.0's session-level accounting: the request that takes a
        session's active_streams from 0 to 1 reserves, sibling requests on that
        session do not, and whichever request is LAST out releases -- from the
        body's teardown or, if the body never started, from core's
        `_ClosingVODStream` closer;
      - gevent's greenlet-local context, by swapping each simulated request's
        context in and out around everything it does.

Checks:
  * Emby burst (3 sessions, same ip+content) selects+reserves ONE slot and frees
    it once, after the last member ends -- and rider selection bypasses capacity.
  * Rider selection pins to the group's account (skips a different account).
  * Different client / content does NOT ride.
  * Partial-failure rollback keeps the slot for survivors.
  * Teardown through a Django-faithful response: completion, client disconnect,
    and a response closed before its body started (owner and rider).
  * Shared-session bursts (v0.32.0's cacheable session redirect): whichever
    request core releases from, the group is left cleanly and a quick replay is
    counted -- the regression v1.3.0 fixes. Also a probe session plus a
    playback session with siblings, and core's create race on one session.
  * A session that reserved natively, or a request with no session id, releases
    natively.
  * Group TTL is refreshed while a MEMBER session streams, and only then.
  * Signature parity: an argument the wrapper does not know about is accepted
    AND forwarded to the original (the class of break that Dispatcharr v0.31.0
    caused by adding a parameter and passing it unconditionally).
  * A caller-supplied profile allowlist is honored rather than overridden by
    the group pin.
  * Upstream's `_resolve_effective_profile` override keeps counters balanced.
  * The plugin logger sets its own level.

Fidelity is the point. The fakes mirror the CURRENT upstream call shapes:
`native_select` carries v0.31.0's four-parameter signature and `select()` passes
`restrict_to_profile_ids=` as a keyword on every call, as `_select_vod_stream`
does -- the pre-v1.2.0 fakes carried the old signature, so the suite passed
while the plugin was crashing every VOD request in the field. Likewise, through
v1.2.0 the fake response stored the raw generator, so the suite "proved" a
close mechanism that never existed in production, and every burst request had
its own session, so the suite could not see v0.32.0's shared sessions.

Run:  python test_logic.py     (or: py -3 test_logic.py)
"""

import importlib.util
import os
import sys
import types

_here = os.path.dirname(os.path.abspath(__file__))


# --------------------------------------------------------------------------- #
# Shared test environment
# --------------------------------------------------------------------------- #
class Env:
    def __init__(self):
        self.store = {}            # group_key -> {field: value}
        self.profile_counts = {}   # pid -> int
        self.native_reserves = 0
        self.native_releases = 0
        self.profiles = {}         # pid -> FakeProfile
        self.native_capacity = {}  # pid -> max (for native selection stub)
        self.touch_calls = {}      # group_key -> member-gated TTL refreshes
        self.native_select_calls = []  # what the wrapper forwarded to native


ENV = Env()  # rebound per scenario


# --------------------------------------------------------------------------- #
# Fake Redis + Lua emulators
# --------------------------------------------------------------------------- #
class FakeScript:
    def __init__(self, fn):
        self.fn = fn

    def __call__(self, keys=None, args=None):
        return self.fn(ENV.store, keys or [], args or [])


def _emu_join(store, keys, args):
    k = keys[0]; now = args[1]; mf = args[2]
    h = store.setdefault(k, {})
    h["refcount"] = int(h.get("refcount", 0)) + 1
    h[mf] = int(h.get(mf, 0)) + 1
    h["last_activity"] = now
    if h["refcount"] == 1:
        h["created_at"] = now
    return [h["refcount"], 1 if h.get("reserved") == "1" else 0]


def _emu_undo(store, keys, args):
    k = keys[0]; mf = args[0]; h = store.get(k)
    if h is None:
        return 0
    h[mf] = int(h.get(mf, 0)) - 1
    if h[mf] <= 0:
        h.pop(mf, None)
    h["refcount"] = int(h.get("refcount", 0)) - 1
    rc = h["refcount"]
    if rc <= 0:
        store.pop(k, None)
    return rc


def _emu_mark(store, keys, args):
    k = keys[0]; h = store.get(k)
    if h is None:
        return 0
    h["reserved"] = "1"; h["profile_id"] = args[1]; h["account_id"] = args[2]
    return 1


def _emu_leave(store, keys, args):
    k = keys[0]; now = args[1]; mf = args[2]; h = store.get(k)
    if h is None:
        return [-1, 0]
    if int(h.get(mf, 0)) <= 0:
        return [-2, int(h.get("refcount", 0))]
    h[mf] = int(h[mf]) - 1
    if h[mf] <= 0:
        h.pop(mf, None)
    h["refcount"] = int(h["refcount"]) - 1
    rc = h["refcount"]; h["last_activity"] = now
    if rc <= 0:
        store.pop(k, None)
        return [1, rc]
    return [0, rc]


def _emu_touch(store, keys, args):
    k = keys[0]; mf = args[1]; h = store.get(k)
    if h is not None and int(h.get(mf, 0)) > 0:
        ENV.touch_calls[k] = ENV.touch_calls.get(k, 0) + 1
        return 1
    return 0


class FakeRedis:
    # Each Lua script in patch.py starts with a `-- vodcc:<name>` tag.
    _EMULATORS = {"join": _emu_join, "undo": _emu_undo, "mark": _emu_mark,
                  "leave": _emu_leave, "touch": _emu_touch}

    def register_script(self, text):
        tag = text.split("\n", 1)[0].strip()
        assert tag.startswith("-- vodcc:"), "Lua script is missing its vodcc tag"
        return FakeScript(self._EMULATORS[tag[len("-- vodcc:"):]])

    def hgetall(self, key):
        h = ENV.store.get(key)
        return dict(h) if h else {}


_FAKE_REDIS = FakeRedis()


# --------------------------------------------------------------------------- #
# Inject fake Dispatcharr modules that patch.py imports lazily
# --------------------------------------------------------------------------- #
class FakeProfile:
    def __init__(self, pid, max_streams, account_id):
        self.id = pid
        self.max_streams = max_streams
        self.m3u_account_id = account_id


def _install_fake_modules():
    core_utils = types.ModuleType("core.utils")

    class RedisClient:
        @staticmethod
        def get_client():
            return _FAKE_REDIS
    core_utils.RedisClient = RedisClient
    sys.modules["core"] = types.ModuleType("core")
    sys.modules["core.utils"] = core_utils

    m3u_models = types.ModuleType("apps.m3u.models")

    class _DoesNotExist(Exception):
        pass

    class _Manager:
        def get(self, id=None, m3u_account=None, is_active=None):
            prof = ENV.profiles.get(int(id))
            if prof is None:
                raise _DoesNotExist()
            return prof

    class M3UAccountProfile:
        objects = _Manager()
        DoesNotExist = _DoesNotExist
    m3u_models.M3UAccountProfile = M3UAccountProfile

    conn_pool = types.ModuleType("apps.m3u.connection_pool")
    conn_pool.get_profile_connection_count = lambda prof, redis: ENV.profile_counts.get(prof.id, 0)

    vod_utils = types.ModuleType("apps.proxy.vod_proxy.utils")
    vod_utils.get_client_info = lambda request: (request.get("ip"), request.get("ua", "ua"))

    for name in ("apps", "apps.m3u", "apps.proxy", "apps.proxy.vod_proxy"):
        sys.modules.setdefault(name, types.ModuleType(name))
    sys.modules["apps.m3u.models"] = m3u_models
    sys.modules["apps.m3u.connection_pool"] = conn_pool
    sys.modules["apps.proxy.vod_proxy.utils"] = vod_utils


_install_fake_modules()

# import patch.py in isolation
spec = importlib.util.spec_from_file_location("vodcc_patch", os.path.join(_here, "patch.py"))
patch = importlib.util.module_from_spec(spec)
spec.loader.exec_module(patch)


# --------------------------------------------------------------------------- #
# Fake native manager (reserve/release) + native selection stub
# --------------------------------------------------------------------------- #
class FakeManager:
    redis_client = _FAKE_REDIS

    def _orig_reserve(self, profile):
        ENV.native_reserves += 1
        cur = ENV.profile_counts.get(profile.id, 0)
        if profile.max_streams > 0 and cur + 1 > profile.max_streams:
            return False
        ENV.profile_counts[profile.id] = cur + 1
        return True

    def _orig_decrement(self, pid):
        ENV.native_releases += 1
        cur = ENV.profile_counts.get(pid, 0)
        if cur > 0:
            ENV.profile_counts[pid] = cur - 1
        return ENV.profile_counts.get(pid, 0)


def native_select(m3u_account, profile_id, session_id=None,
                  restrict_to_profile_ids=None, **unexpected):
    """Native selection stub, mirroring Dispatcharr v0.31.0's `_get_m3u_profile`.

    Picks the account's profile if it is permitted and has capacity, and honours
    `restrict_to_profile_ids` the way core does (only listed profile ids on this
    account are selectable; None means unrestricted).

    `**unexpected` stands in for a parameter a FUTURE Dispatcharr adds, and
    every call is recorded so tests can assert the wrapper forwarded rather than
    swallowed what it was given.
    """
    ENV.native_select_calls.append({
        "profile_id": profile_id,
        "session_id": session_id,
        "restrict_to_profile_ids": restrict_to_profile_ids,
        "unexpected": unexpected,
    })
    for pid, prof in ENV.profiles.items():
        if prof.m3u_account_id != m3u_account.id:
            continue
        if restrict_to_profile_ids is not None and pid not in restrict_to_profile_ids:
            continue
        cur = ENV.profile_counts.get(pid, 0)
        if prof.max_streams == 0 or cur < prof.max_streams:
            return (prof, cur)
        return None
    return None


class FakeAccount:
    def __init__(self, aid):
        self.id = aid


# --------------------------------------------------------------------------- #
# Harness
# --------------------------------------------------------------------------- #
MGR = FakeManager()


def setup(profiles):
    global ENV
    ENV = Env()
    for prof in profiles:
        ENV.profiles[prof.id] = prof
    patch._orig_reserve = FakeManager._orig_reserve
    patch._orig_decrement = FakeManager._orig_decrement
    patch._orig_get_m3u_profile = native_select
    patch._ACTIVE = True
    patch._script_cache.clear()


def select(account, ip, uuid, profile_id=None, session_id=None,
           restrict_to_profile_ids=None, **unexpected):
    """Call the patched selector the way `_select_vod_stream` does.

    Three positional args, then `restrict_to_profile_ids` as a keyword that is
    ALWAYS present in the call expression -- its value is often None, but the
    keyword itself is unconditional, which is precisely why a wrapper with a
    fixed signature raises TypeError at argument binding on v0.31.0.
    """
    patch._set_ctx(ip, uuid)
    return patch.patched_get_m3u_profile(
        account, profile_id, session_id,
        restrict_to_profile_ids=restrict_to_profile_ids,
        **unexpected,
    )


_session_seq = [0]


def new_session():
    _session_seq[0] += 1
    return f"sess-{_session_seq[0]}"


def reserve(profile, ip, uuid, session_id=None):
    """One request reserving. Without a session id it gets a fresh one: one
    session per request, the pre-v0.32.0 shape (and Emby's, even on v0.32.0)."""
    patch._set_ctx(ip, uuid, session_id or new_session())
    ok = patch.patched_reserve(MGR, profile)
    return ok, patch._snapshot_ctx()


def release(pid, ctx):
    """Release from a request whose context is `ctx`."""
    patch._restore_ctx(ctx)
    patch.patched_decrement(MGR, pid)
    patch._reset_ctx()


# --------------------------------------------------------------------------- #
# Django / core / gevent models for the streaming tests
# --------------------------------------------------------------------------- #
class FakeStreamingHttpResponse:
    """Django 6.0.8's StreamingHttpResponse, sync mode, where it matters."""

    def __init__(self, streaming_content):
        self._resource_closers = []
        self.streaming_content = streaming_content

    @property
    def streaming_content(self):
        return map(self.make_bytes, self._iterator)

    @streaming_content.setter
    def streaming_content(self, value):
        self._iterator = iter(value)
        if hasattr(value, "close"):
            self._resource_closers.append(value.close)

    @staticmethod
    def make_bytes(value):
        return bytes(value)

    def __iter__(self):
        return iter(self.streaming_content)

    def close(self):
        for closer in self._resource_closers:
            try:
                closer()
            except Exception:
                pass
        self._resource_closers.clear()


class ClosingVODStream:
    """Dispatcharr v0.32.0's `_ClosingVODStream`: closes the body, then runs
    `on_close` (which releases only if the body never started)."""

    def __init__(self, generator, on_close):
        self._generator = generator
        self._on_close = on_close

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._generator)

    def close(self):
        try:
            self._generator.close()
        finally:
            self._on_close()


class Greenlet:
    """One simulated request greenlet. Its greenlet-local context is swapped in
    on enter and saved on exit, the way gevent keeps it per greenlet."""

    def __init__(self):
        self.ctx = {"client_ip": None, "content_uuid": None, "session_id": None}

    def __enter__(self):
        patch._restore_ctx(self.ctx)
        return self

    def __exit__(self, *exc):
        self.ctx = patch._snapshot_ctx()
        patch._reset_ctx()
        return False


class Core:
    """What the plugin depends on in v0.32.0's `stream_content_with_session`:
    per-session active_streams; the request taking a session 0 -> 1 reserves,
    siblings don't; the LAST request out releases, from the body's finally or,
    if the body never started, from the `_ClosingVODStream` closer."""

    def __init__(self, profile):
        self.profile = profile
        self.active = {}

    def _last_out(self, sid):
        self.active[sid] -= 1
        if self.active[sid] == 0:
            patch.patched_decrement(MGR, self.profile.id)

    def response(self, sid, chunks):
        n = self.active.get(sid, 0) + 1
        self.active[sid] = n
        if n == 1:
            assert patch.patched_reserve(MGR, self.profile), "reserve refused"
        state = {"started": False, "released": False}

        def body():
            state["started"] = True
            try:
                for _ in range(chunks):
                    yield b"x"
            finally:
                if not state["released"]:
                    state["released"] = True
                    self._last_out(sid)

        def on_close():
            if state["started"] or state["released"]:
                return
            state["released"] = True
            self._last_out(sid)

        return FakeStreamingHttpResponse(ClosingVODStream(body(), on_close))


def open_request(core, g, ip, uuid, sid, chunks=4):
    """Run the patched stream_vod in greenlet `g`, core answering for `sid`."""
    saved = patch._orig_stream_vod
    patch._orig_stream_vod = lambda *a, **k: core.response(sid, chunks)
    try:
        with g:
            return patch.patched_stream_vod({"ip": ip}, "movie", uuid, sid)
    finally:
        patch._orig_stream_vod = saved


def pull(g, resp, n=None):
    """The server iterating `resp` in `g`: n chunks, or to the end."""
    with g:
        if not hasattr(resp, "_server_iter"):
            resp._server_iter = iter(resp)
        count = 0
        while n is None or count < n:
            try:
                next(resp._server_iter)
            except StopIteration:
                break
            count += 1
        return count


def close(g, resp):
    with g:
        resp.close()


PASS = "PASS"
FAIL = "FAIL"
_failures = []


def check(name, cond):
    print(f"  [{PASS if cond else FAIL}] {name}")
    if not cond:
        _failures.append(name)


# --------------------------------------------------------------------------- #
# Scenarios
# --------------------------------------------------------------------------- #
def test_emby_burst():
    print("test_emby_burst (selection + reservation + release)")
    p3 = FakeProfile(3, max_streams=1, account_id=7)
    setup([p3])
    acct = FakeAccount(7)
    ip, uuid = "10.0.0.5", "content-uuid-burst"

    # Req #1 (owner): native selection (counter 0 -> ok), then reserve.
    sel1 = select(acct, ip, uuid)
    check("owner selects profile natively", sel1 and sel1[0].id == 3)
    ok1, ctx1 = reserve(p3, ip, uuid)
    check("owner reserves", ok1 and ENV.profile_counts[3] == 1)

    # Req #2 (rider): selection must BYPASS capacity (counter is 1 == max).
    sel2 = select(acct, ip, uuid)
    check("rider selection bypasses capacity (no failover)", bool(sel2) and sel2[0].id == 3)
    ok2, ctx2 = reserve(p3, ip, uuid)
    check("rider reserves by riding (counter stays 1)", ok2 and ENV.profile_counts[3] == 1)

    # Req #3 (rider)
    sel3 = select(acct, ip, uuid)
    check("rider #3 selection bypasses capacity", bool(sel3) and sel3[0].id == 3)
    ok3, ctx3 = reserve(p3, ip, uuid)
    check("rider #3 rides", ok3 and ENV.profile_counts[3] == 1)

    check("exactly one native reservation total", ENV.native_reserves == 1)

    # teardown
    release(3, ctx2); check("counter 1 after first member ends", ENV.profile_counts[3] == 1)
    release(3, ctx1); check("counter 1 after second ends", ENV.profile_counts[3] == 1)
    release(3, ctx3); check("counter 0 after last ends", ENV.profile_counts[3] == 0)
    check("exactly one native release", ENV.native_releases == 1)
    check("group cleaned up", not ENV.store)


def test_rider_pins_to_group_account():
    print("test_rider_pins_to_group_account")
    # account 7 = group account (owner), account 9 = a different account/file
    p3 = FakeProfile(3, max_streams=1, account_id=7)
    p5 = FakeProfile(5, max_streams=5, account_id=9)  # different acct, has capacity
    setup([p3, p5])
    ip, uuid = "10.0.0.5", "movieX"

    select(FakeAccount(7), ip, uuid)
    reserve(p3, ip, uuid)  # owner on account 7

    # rider's candidate loop hits account 9 first (has capacity): must be SKIPPED
    # so the loop stays pinned to the group's account 7.
    sel_other = select(FakeAccount(9), ip, uuid)
    check("rider skips non-group account (pins to owner's file)", sel_other is None)
    sel_group = select(FakeAccount(7), ip, uuid)
    check("rider selects the group account, bypassing capacity",
          bool(sel_group) and sel_group[0].id == 3)


def test_different_client_does_not_ride():
    print("test_different_client_does_not_ride")
    p3 = FakeProfile(3, max_streams=1, account_id=7)
    setup([p3])
    acct = FakeAccount(7)

    select(acct, "10.0.0.5", "uuidA"); ok1, ctx1 = reserve(p3, "10.0.0.5", "uuidA")
    # different ip, same content -> different group, no bypass
    sel2 = select(acct, "10.0.0.9", "uuidA")
    check("different client does NOT get a capacity bypass", sel2 is None)
    ok2, ctx2 = reserve(p3, "10.0.0.9", "uuidA")
    check("different client hits capacity at reservation too", ok2 is False)
    check("counter is 1", ENV.profile_counts[3] == 1)
    release(3, ctx1); check("counter 0 after owner ends", ENV.profile_counts[3] == 0)


def test_partial_failure_keeps_slot():
    print("test_partial_failure_keeps_slot")
    p3 = FakeProfile(3, max_streams=1, account_id=7)
    setup([p3])
    ip, uuid = "10.0.0.5", "uuidP"
    _, ctx1 = reserve(p3, ip, uuid)   # owner
    _, ctx2 = reserve(p3, ip, uuid)   # rider
    release(3, ctx2); check("slot held for survivor", ENV.profile_counts[3] == 1)
    check("no native release yet", ENV.native_releases == 0)
    release(3, ctx1); check("freed after owner ends", ENV.profile_counts[3] == 0)
    check("one native release total", ENV.native_releases == 1)


def test_teardown_through_django_response():
    """Every way a response ends, through a Django-faithful response.

    Django runs closers in registration order: core's (registered when core
    built the response) first, ours second. That ordering is what lets core's
    teardown release see our context, so the rider case below would break if it
    were ever reversed.
    """
    print("test_teardown_through_django_response")
    ip, uuid = "10.0.0.5", "content-uuid-close"
    gkey = patch._group_key(ip, uuid)

    r = FakeStreamingHttpResponse(iter([b"a"]))
    check("premise: Django's streaming_content getter has no close()",
          not hasattr(r.streaming_content, "close"))

    # (a) normal completion
    p = FakeProfile(3, max_streams=1, account_id=7)
    setup([p]); core = Core(p); g = Greenlet()
    resp = open_request(core, g, ip, uuid, "S-a")
    check("owner reserved one slot", ENV.profile_counts.get(3) == 1)
    pull(g, resp); close(g, resp)
    check("completion: counter back to 0 and group gone",
          ENV.profile_counts[3] == 0 and gkey not in ENV.store)
    check("completion: context cleared", g.ctx["session_id"] is None)

    # (b) client disconnect mid-stream
    setup([p]); core = Core(p); g = Greenlet()
    resp = open_request(core, g, ip, uuid, "S-b")
    pull(g, resp, 1); close(g, resp)
    check("disconnect: released through the group (counter 0, group gone)",
          ENV.profile_counts[3] == 0 and gkey not in ENV.store)
    check("disconnect: context cleared", g.ctx["session_id"] is None)

    # (c) closed before the body started, sole owner (v0.32.0's closer path)
    setup([p]); core = Core(p); g = Greenlet()
    resp = open_request(core, g, ip, uuid, "S-c")
    close(g, resp)
    check("unstarted owner: core's closer released it (counter 0, group gone)",
          ENV.profile_counts[3] == 0 and gkey not in ENV.store)
    check("unstarted: context cleared even though the body never ran",
          g.ctx["session_id"] is None)

    # (d) closed before the body started, a rider while the owner streams
    setup([p]); core = Core(p); g1, g2 = Greenlet(), Greenlet()
    r1 = open_request(core, g1, ip, uuid, "S-d1"); pull(g1, r1, 1)
    r2 = open_request(core, g2, ip, uuid, "S-d2")
    check("rider joined without a second native reservation",
          ENV.native_reserves == 1 and ENV.store[gkey]["refcount"] == 2)
    close(g2, r2)
    check("unstarted rider: group shrank by one, slot held for the owner",
          ENV.profile_counts[3] == 1 and ENV.store[gkey]["refcount"] == 1)
    pull(g1, r1); close(g1, r1)
    check("owner ends: counter 0, group gone, exactly one native release",
          ENV.profile_counts[3] == 0 and gkey not in ENV.store
          and ENV.native_releases == 1)


def test_group_ttl_refresh_only_for_member_sessions():
    """Seek coverage (v1.1.0), now by session.

    A steady playback generates no reserve/release events, so the stream itself
    keeps the group alive. On v0.32.0 that stream may be a sibling request on
    the member session rather than the request that joined, so the refresh is
    gated on the session's membership -- and an unrelated stream for the same
    (ip, content) must not keep a stale group alive.
    """
    print("test_group_ttl_refresh_only_for_member_sessions")
    ip, uuid = "10.0.0.5", "content-uuid-ttl"
    gkey = patch._group_key(ip, uuid)
    p = FakeProfile(3, max_streams=1, account_id=7)
    setup([p]); core = Core(p)
    saved = patch.GROUP_REFRESH_INTERVAL_SECONDS
    patch.GROUP_REFRESH_INTERVAL_SECONDS = 0          # refresh on every chunk
    try:
        g1, g2 = Greenlet(), Greenlet()
        r1 = open_request(core, g1, ip, uuid, "S1", chunks=6)
        r2 = open_request(core, g2, ip, uuid, "S1", chunks=6)   # sibling
        pull(g2, r2, 3)
        check("a sibling on the member session refreshes the group TTL",
              ENV.touch_calls.get(gkey, 0) >= 1)

        before = ENV.touch_calls.get(gkey, 0)
        g3 = Greenlet()
        with g3:
            patch._set_ctx(ip, uuid, "S-not-a-member")
            stream = patch._ContextStream(iter([b"a", b"b", b"c"]),
                                          patch._snapshot_ctx(), gkey)
            list(stream)
        check("a non-member's stream for the same (ip, content) does not",
              ENV.touch_calls.get(gkey, 0) == before)
        pull(g1, r1); close(g1, r1); pull(g2, r2); close(g2, r2)
        check("session released at the end: counter 0, group gone",
              ENV.profile_counts[3] == 0 and gkey not in ENV.store)
    finally:
        patch.GROUP_REFRESH_INTERVAL_SECONDS = saved


def test_shared_session_burst():
    """THE v1.3.0 REGRESSION. Dispatcharr v0.32.0 made the session-mint 301
    cacheable, so a client that reuses it sends a burst on ONE session. Core
    reserves in the first request and releases from whichever is last out.

    Through v1.2.0 the plugin kept membership per request: if the reserving
    request ended first, the sibling released outside the group, the group was
    orphaned, and a quick replay rode it without taking a slot -- an uncounted
    stream on a full profile. Both end orders are checked.
    """
    print("test_shared_session_burst")
    ip, uuid = "10.0.0.5", "content-uuid-shared"
    gkey = patch._group_key(ip, uuid)
    for first_out in ("reserving request", "sibling"):
        tag = f"[{first_out} ends first]"
        p = FakeProfile(3, max_streams=1, account_id=7)
        setup([p]); core = Core(p)
        g1, g2 = Greenlet(), Greenlet()
        r1 = open_request(core, g1, ip, uuid, "S1"); pull(g1, r1, 1)
        r2 = open_request(core, g2, ip, uuid, "S1"); pull(g2, r2, 1)
        check(f"{tag} one native reservation for the shared session",
              ENV.native_reserves == 1 and ENV.profile_counts[3] == 1)
        first, last = ((g1, r1), (g2, r2)) if first_out.startswith("reserving") \
            else ((g2, r2), (g1, r1))
        pull(*first); close(*first)
        check(f"{tag} slot held while the session's other request streams",
              ENV.profile_counts[3] == 1)
        pull(*last); close(*last)
        check(f"{tag} last out released it: counter 0, no orphan group",
              ENV.profile_counts[3] == 0 and gkey not in ENV.store)

        # Stop + quick replay: a new open, so a new session, within the TTL.
        g3 = Greenlet()
        r3 = open_request(core, g3, ip, uuid, "S2"); pull(g3, r3, 1)
        check(f"{tag} a quick replay is COUNTED", ENV.profile_counts[3] == 1)
        pull(g3, r3); close(g3, r3)
        check(f"{tag} replay released: counter 0, group gone",
              ENV.profile_counts[3] == 0 and gkey not in ENV.store)


def test_probe_session_and_playback_session_with_siblings():
    """A probe on its own session owns the slot; playback on another session --
    itself two requests on one session -- rides it. The slot is held until the
    last member SESSION ends, whichever request each session releases from."""
    print("test_probe_session_and_playback_session_with_siblings")
    ip, uuid = "10.0.0.5", "content-uuid-probe"
    gkey = patch._group_key(ip, uuid)
    p = FakeProfile(3, max_streams=1, account_id=7)
    setup([p]); core = Core(p)
    gp, g1, g2 = Greenlet(), Greenlet(), Greenlet()
    rp = open_request(core, gp, ip, uuid, "PROBE"); pull(gp, rp, 1)
    r1 = open_request(core, g1, ip, uuid, "PLAY"); pull(g1, r1, 1)
    r2 = open_request(core, g2, ip, uuid, "PLAY"); pull(g2, r2, 1)
    check("probe owns, playback rides: one native reservation, two members",
          ENV.native_reserves == 1 and ENV.store[gkey]["refcount"] == 2)
    pull(g1, r1); close(g1, r1)       # playback's reserving request ends first
    check("playback session still a member while its sibling streams",
          ENV.store[gkey]["refcount"] == 2 and ENV.profile_counts[3] == 1)
    pull(gp, rp); close(gp, rp)       # probe ends
    check("probe left; slot held for the playback session",
          ENV.profile_counts[3] == 1 and ENV.store[gkey]["refcount"] == 1)
    pull(g2, r2); close(g2, r2)       # playback's last request ends
    check("last member session out: counter 0, group gone, one native release",
          ENV.profile_counts[3] == 0 and gkey not in ENV.store
          and ENV.native_releases == 1)


def test_create_race_on_one_session():
    """Two requests on one session both reserve before either creates the
    session (core's create race); the loser drops its reservation in its own
    request ("Sibling already holds the profile slot")."""
    print("test_create_race_on_one_session")
    ip, uuid, sid = "10.0.0.5", "content-uuid-race", "S-race"
    gkey = patch._group_key(ip, uuid)
    p = FakeProfile(3, max_streams=1, account_id=7)
    setup([p])
    ok_a, ctx_a = reserve(p, ip, uuid, sid)
    ok_b, ctx_b = reserve(p, ip, uuid, sid)
    check("both accepted, one native reservation",
          ok_a and ok_b and ENV.native_reserves == 1)
    check("the session is counted as a member twice",
          ENV.store[gkey][f"m:{sid}"] == 2)
    release(3, ctx_b)
    check("the loser's drop keeps the slot",
          ENV.profile_counts[3] == 1 and ENV.store[gkey][f"m:{sid}"] == 1)
    release(3, ctx_a)
    check("the session's release frees it: counter 0, group gone",
          ENV.profile_counts[3] == 0 and gkey not in ENV.store)


def test_native_reservations_release_natively():
    """Two cases where a session holds a NATIVE slot rather than a membership.

    * The sub-millisecond race: a joiner arrives before the owner has marked the
      group reserved, backs out, and reserves natively. Its session is not a
      member, so its release is native too and pairs with that reservation.
    * A request with no session id can't be grouped safely: native throughout.
    """
    print("test_native_reservations_release_natively")
    ip, uuid = "10.0.0.5", "content-uuid-native"
    gkey = patch._group_key(ip, uuid)
    p = FakeProfile(3, max_streams=2, account_id=7)
    setup([p])
    ENV.store[gkey] = {"refcount": 1, "m:S-owner": 1}  # owner joined, not marked
    ok, ctx = reserve(p, ip, uuid, "S-indep")
    check("race: joiner backed out and reserved natively",
          ok and ENV.native_reserves == 1 and ENV.profile_counts[3] == 1)
    check("race: joiner is not recorded as a member",
          "m:S-indep" not in ENV.store[gkey] and ENV.store[gkey]["refcount"] == 1)
    release(3, ctx)
    check("race: its release is native and pairs with that reservation",
          ENV.profile_counts[3] == 0 and ENV.native_releases == 1)
    check("race: the owner's membership is untouched",
          ENV.store[gkey]["refcount"] == 1)

    setup([p])
    patch._set_ctx(ip, uuid, None)
    ok = patch.patched_reserve(MGR, p)
    check("no session id: reserves natively and creates no group",
          ok and ENV.profile_counts[3] == 1 and not ENV.store)
    patch.patched_decrement(MGR, 3)
    patch._reset_ctx()
    check("no session id: releases natively", ENV.profile_counts[3] == 0)


def test_selection_signature_parity():
    """Structural guard against the v0.31.0 class of break.

    Dispatcharr added a parameter to `_get_m3u_profile` and its caller passes it
    unconditionally; a wrapper with a fixed signature raised TypeError while
    binding arguments -- before any guard or try/except could fail open -- and
    every VOD request 500'd. This asserts the two properties that survive that:
    an unknown argument is ACCEPTED, and it is FORWARDED rather than swallowed.
    """
    print("test_selection_signature_parity")
    p3 = FakeProfile(3, max_streams=1, account_id=7)
    setup([p3])
    acct = FakeAccount(7)
    ip, uuid = "10.0.0.5", "content-uuid-parity"

    # No group yet -> native fallthrough, the path core takes on every first
    # request and the one that must stay byte-for-byte faithful.
    sel = select(acct, ip, uuid, session_id="sess-1", some_future_option="x")
    check("unexpected keyword accepted (no TypeError)", bool(sel) and sel[0].id == 3)
    last = ENV.native_select_calls[-1]
    check("session_id forwarded", last["session_id"] == "sess-1")
    check("unexpected keyword forwarded to native",
          last["unexpected"] == {"some_future_option": "x"})
    check("restrict keyword forwarded even when None",
          last["restrict_to_profile_ids"] is None)

    # Same guarantee on the group/rider path's fallthrough, and positionally.
    reserve(p3, ip, uuid)
    patch._set_ctx(ip, uuid)
    sel = patch.patched_get_m3u_profile(acct, None, "sess-2", {4})  # restrict positional
    check("positional restrict is read, not ignored (pin abandoned)", sel is None)
    check("positional restrict forwarded to native",
          ENV.native_select_calls[-1]["restrict_to_profile_ids"] == {4})

    # And the inactive path must forward too, or disabling the plugin would
    # reintroduce the exact crash it is the documented mitigation for.
    patch._ACTIVE = False
    try:
        sel = select(acct, ip, uuid, session_id="sess-3", some_future_option="y")
    finally:
        patch._ACTIVE = True
    check("inactive wrapper accepts + forwards unknown args",
          ENV.native_select_calls[-1]["unexpected"] == {"some_future_option": "y"})


def test_profile_allowlist_is_honored():
    """A caller-supplied allowlist is a constraint; the group pin is not.

    Dispatcharr v0.31.0's per-user M3U profile allowlist reaches selection as
    `restrict_to_profile_ids`: None (unrestricted) or a set of int profile ids.
    The rider path returns the pinned profile directly, bypassing core's own
    filtering, so it has to apply the constraint itself.
    """
    print("test_profile_allowlist_is_honoured")
    p3 = FakeProfile(3, max_streams=1, account_id=7)   # group's account
    p5 = FakeProfile(5, max_streams=5, account_id=9)   # a different account
    setup([p3, p5])
    ip, uuid = "10.0.0.5", "content-uuid-allow"

    select(FakeAccount(7), ip, uuid)
    reserve(p3, ip, uuid)   # group owner pinned to profile 3 on account 7
    check("group is confirmed and at capacity", ENV.profile_counts[3] == 1)

    # (a) Unrestricted -> today's behaviour exactly: pin honoured, capacity bypassed.
    sel = select(FakeAccount(7), ip, uuid, restrict_to_profile_ids=None)
    check("restrict=None -> pin honoured, capacity bypassed",
          bool(sel) and sel[0].id == 3)

    # (b) Pin permitted -> still pinned. This is also the int-cast regression:
    #     Redis stores profile_id as the STRING "3" while the allowlist holds
    #     the INT 3, so without the cast the check would always fail, the pin
    #     would always be abandoned, and native (1/1, at capacity) returns None
    #     -- a silent loss of the plugin's entire benefit.
    sel = select(FakeAccount(7), ip, uuid, restrict_to_profile_ids={3})
    check("pin permitted -> pinned profile returned (str/int cast applied)",
          bool(sel) and sel[0].id == 3)

    # (c) Pin NOT permitted -> abandon the pin and let native decide.
    n_before = len(ENV.native_select_calls)
    sel = select(FakeAccount(7), ip, uuid, restrict_to_profile_ids={4})
    check("pin not permitted -> falls through to native",
          len(ENV.native_select_calls) == n_before + 1)
    check("pin not permitted -> allowlist forwarded to native",
          ENV.native_select_calls[-1]["restrict_to_profile_ids"] == {4})
    check("pin not permitted -> disallowed profile is NOT returned", sel is None)

    # (d) Empty allowlist (allow nothing) -> pin abandoned, nothing selectable.
    sel = select(FakeAccount(7), ip, uuid, restrict_to_profile_ids=set())
    check("empty allowlist -> pin abandoned, nothing selected", sel is None)

    # (e) The cross-account skip is gated by the same check. `restrict` is
    #     scoped to the candidate account, so it can never contain the group
    #     account's profile id -- if the skip were not gated, a restricted user
    #     would have EVERY usable candidate skipped and get "No available
    #     stream" instead of a working provider.
    sel = select(FakeAccount(9), ip, uuid, restrict_to_profile_ids={5})
    check("restricted user is not pinned into an unusable account",
          bool(sel) and sel[0].id == 5)

    # ...while an unrestricted request still gets the pin (no regression).
    sel = select(FakeAccount(9), ip, uuid, restrict_to_profile_ids=None)
    check("unrestricted -> non-group account still skipped (pin intact)",
          sel is None)


def test_backwards_compatible_with_pre_v0_31_callers():
    """One build must serve Dispatcharr v0.30.0 and earlier too.

    Users upgrade the plugin and Dispatcharr on different days, so the fix has
    to be a no-op against the OLD three-parameter internals: nothing is passed,
    so nothing is forwarded. The native stub here carries v0.30.0's exact
    signature, which means it raises TypeError if the wrapper ever invents an
    argument to forward.
    """
    print("test_backwards_compatible_with_pre_v0_31_callers")
    p3 = FakeProfile(3, max_streams=1, account_id=7)
    p5 = FakeProfile(5, max_streams=5, account_id=9)
    setup([p3, p5])

    def native_select_v0_30(m3u_account, profile_id, session_id=None):
        """v0.30.0's signature -- no `restrict_to_profile_ids`, no **kwargs."""
        ENV.native_select_calls.append({"profile_id": profile_id,
                                        "session_id": session_id})
        for pid, prof in ENV.profiles.items():
            if prof.m3u_account_id != m3u_account.id:
                continue
            cur = ENV.profile_counts.get(pid, 0)
            return (prof, cur) if (prof.max_streams == 0
                                   or cur < prof.max_streams) else None
        return None

    patch._orig_get_m3u_profile = native_select_v0_30
    ip, uuid = "10.0.0.5", "content-uuid-legacy"

    # Old caller shape: three positional args, no restrict keyword at all.
    patch._set_ctx(ip, uuid)
    sel = patch.patched_get_m3u_profile(FakeAccount(7), None, None)
    check("v0.30.0 caller + v0.30.0 native: selection works",
          bool(sel) and sel[0].id == 3)

    reserve(p3, ip, uuid)
    patch._set_ctx(ip, uuid)
    sel = patch.patched_get_m3u_profile(FakeAccount(7), None, None)
    check("v0.30.0: rider still bypasses capacity (coalescing intact)",
          bool(sel) and sel[0].id == 3)
    patch._set_ctx(ip, uuid)
    sel = patch.patched_get_m3u_profile(FakeAccount(9), None, None)
    check("v0.30.0: non-group account still skipped (pin intact)", sel is None)
    patch._reset_ctx()


def test_upstream_effective_profile_override():
    """Upstream can re-point the slot away from our pinned pick (v0.31.0+).

    `_resolve_effective_profile` prefers the profile already bound on the
    SESSION hash over the view's pick. For a new session it returns our pick,
    but for a session that already has a DIFFERENT profile bound (reconnect,
    idle-session adoption, create race) upstream overrides us and then calls
    our patched reserve/decrement with a profile the group never pinned.

    It logs nothing when it overrides, so only its effect on the counters can
    be observed: whatever upstream re-points, the native counter must come back
    to 0 and native reserve/release must stay symmetric.
    """
    print("test_upstream_effective_profile_override")
    ip, uuid = "10.0.0.5", "content-uuid-effective"

    # --- Scenario 1: create-race swap INSIDE one request (manager:1313-1314) --
    # Upstream releases OUR profile and reserves ITS profile back to back, in
    # one request, on one session.
    p3 = FakeProfile(3, max_streams=1, account_id=7)   # group's pinned profile
    p5 = FakeProfile(5, max_streams=1, account_id=7)   # session's bound profile
    setup([p3, p5])

    select(FakeAccount(7), ip, uuid)
    ok_owner, ctx_owner = reserve(p3, ip, uuid, "S-owner")
    check("owner holds the pinned profile", ok_owner and ENV.profile_counts[3] == 1)

    patch._set_ctx(ip, uuid, "S-swap")
    sel = patch.patched_get_m3u_profile(FakeAccount(7), None, "S-swap",
                                        restrict_to_profile_ids=None)
    check("rider selection returns the pin", bool(sel) and sel[0].id == 3)
    check("rider rides the pin (manager:1233)",
          patch.patched_reserve(MGR, p3) is True)
    patch.patched_decrement(MGR, 3)                # manager:1313, drops our pick
    check("swap: reserving upstream's effective profile succeeds",
          patch.patched_reserve(MGR, p5) is True)  # manager:1314
    ctx_swap = patch._snapshot_ctx()
    check("swap did NOT double-count the native counter",
          ENV.profile_counts[3] == 1 and ENV.profile_counts.get(5, 0) == 0)

    release(5, ctx_swap)
    check("slot still held for the owner after the swapped session leaves",
          ENV.profile_counts[3] == 1)
    release(3, ctx_owner)
    check("counter returns to 0 after a swapped-profile request",
          ENV.profile_counts[3] == 0 and ENV.profile_counts.get(5, 0) == 0)
    check("native reserve/release stayed symmetric across the swap",
          ENV.native_reserves == 1 and ENV.native_releases == 1)

    # --- Scenario 2: the same swap when the group has no other member ---------
    setup([FakeProfile(3, 1, 7), FakeProfile(5, 1, 7)])
    patch._set_ctx(ip, uuid, "S-solo")
    patch.patched_reserve(MGR, ENV.profiles[3])
    check("solo owner holds the pinned profile", ENV.profile_counts[3] == 1)
    patch.patched_decrement(MGR, 3)
    check("solo swap releases the abandoned profile", ENV.profile_counts[3] == 0)
    check("solo swap re-reserves as a fresh owner",
          patch.patched_reserve(MGR, ENV.profiles[5]) is True
          and ENV.profile_counts[5] == 1)
    release(5, patch._snapshot_ctx())
    check("solo swap: both counters back to 0",
          ENV.profile_counts[3] == 0 and ENV.profile_counts[5] == 0)
    check("solo swap: native reserve/release symmetric",
          ENV.native_reserves == 2 and ENV.native_releases == 2)

    # --- Scenario 3: reconnect / idle-session reuse (manager:1341, :1361) -----
    # No prior reserve in the request; upstream reserves its effective profile
    # while the group is pinned to another, so it rides and the effective
    # profile's own counter is never touched. The group holds the pinned
    # profile's slot while the request streams from the effective one -- the
    # same under-counting trade-off documented for two devices sharing (IP,
    # title). Accounting stays balanced, which is what prevents a leak.
    setup([FakeProfile(3, 1, 7), FakeProfile(5, 1, 7)])
    select(FakeAccount(7), ip, uuid)
    _, ctx_owner = reserve(ENV.profiles[3], ip, uuid, "S-owner")
    patch._set_ctx(ip, uuid, "S-reconnect")
    check("reconnect on a differently bound profile rides the group",
          patch.patched_reserve(MGR, ENV.profiles[5]) is True)
    ctx_reconnect = patch._snapshot_ctx()
    check("reconnect did not reserve the effective profile natively",
          ENV.profile_counts.get(5, 0) == 0 and ENV.native_reserves == 1)
    release(5, ctx_reconnect)
    release(3, ctx_owner)
    check("reconnect: counters back to 0, no negative",
          ENV.profile_counts[3] == 0 and ENV.profile_counts.get(5, 0) == 0)
    check("reconnect: exactly one native reserve and one native release",
          ENV.native_reserves == 1 and ENV.native_releases == 1)


def test_logger_has_its_own_level():
    """The plugin logger must not inherit root's level.

    Dispatcharr's LOGGING dictConfig does not name `plugins.*`, so without this
    the logger's effective level is root's. In a Celery prefork child billiard
    leaves root at WARNING, which silently discards every INFO line this plugin
    emits -- i.e. all of its diagnostics -- while core's `apps.*` lines from the
    same process still show up. Nothing this plugin patches runs in a prefork
    child today, so this is preventive; the point is that the failure mode is an
    *absence*, which nobody notices.
    """
    print("test_logger_has_its_own_level")
    import logging

    check("logger has an explicit level after import (not NOTSET)",
          patch.logger.level != logging.NOTSET)

    # Adopts `apps`'s level so DISPATCHARR_LOG_LEVEL stays effective, rather
    # than hard-coding INFO.
    logging.getLogger("apps").setLevel(logging.DEBUG)
    patch.logger.setLevel(logging.NOTSET)
    patch._align_logger_level()
    check("adopts the `apps` logger's level, not a hard-coded INFO",
          patch.logger.level == logging.DEBUG)

    # The NOTSET guard: anything that already set a level keeps control.
    patch.logger.setLevel(logging.CRITICAL)
    patch._align_logger_level()
    check("an already-set level is left alone (NOTSET guard)",
          patch.logger.level == logging.CRITICAL)

    # Leave the logger in a sane state for any later test output.
    patch.logger.setLevel(logging.NOTSET)
    logging.getLogger("apps").setLevel(logging.NOTSET)
    patch._align_logger_level()


def test_manager_hook_signature_parity():
    """The same hardening on the two manager hooks.

    Their signatures match core today and all call sites pass one positional
    argument -- but v0.31.0 is proof that "matches today" is not durable, and
    the cost of accepting/forwarding is nil. Run on the GROUP path (a session
    id is set), so forwarding is checked where the plugin does its own work.
    """
    print("test_manager_hook_signature_parity")
    p3 = FakeProfile(3, max_streams=2, account_id=7)
    setup([p3])
    seen = {}

    def recording_reserve(self, profile, *args, **kwargs):
        seen["reserve"] = (args, kwargs)
        return FakeManager._orig_reserve(self, profile)

    def recording_decrement(self, pid, *args, **kwargs):
        seen["decrement"] = (args, kwargs)
        return FakeManager._orig_decrement(self, pid)

    patch._orig_reserve = recording_reserve
    patch._orig_decrement = recording_decrement

    patch._set_ctx("10.0.0.5", "content-uuid-hooks", "S-hooks")
    ok = patch.patched_reserve(MGR, p3, "future_positional", future_kw=1)
    check("reserve accepts unexpected args (no TypeError)", ok is True)
    check("reserve forwards unexpected args to native",
          seen.get("reserve") == (("future_positional",), {"future_kw": 1}))

    patch.patched_decrement(MGR, 3, "future_positional", future_kw=2)
    check("decrement accepts + forwards unexpected args",
          seen.get("decrement") == (("future_positional",), {"future_kw": 2}))
    check("slot released exactly once despite the extra args",
          ENV.profile_counts[3] == 0 and not ENV.store)
    patch._reset_ctx()


if __name__ == "__main__":
    test_emby_burst()
    test_rider_pins_to_group_account()
    test_different_client_does_not_ride()
    test_partial_failure_keeps_slot()
    test_teardown_through_django_response()
    test_group_ttl_refresh_only_for_member_sessions()
    test_shared_session_burst()
    test_probe_session_and_playback_session_with_siblings()
    test_create_race_on_one_session()
    test_native_reservations_release_natively()
    test_selection_signature_parity()
    test_profile_allowlist_is_honored()
    test_backwards_compatible_with_pre_v0_31_callers()
    test_upstream_effective_profile_override()
    test_logger_has_its_own_level()
    test_manager_hook_signature_parity()
    print()
    if _failures:
        print(f"{len(_failures)} check(s) FAILED: {_failures}")
        sys.exit(1)
    print("All checks passed.")
