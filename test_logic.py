"""
Self-contained logic test for the VOD concurrency-coalescing patch (4 patches).

Runs WITHOUT Dispatcharr or a real Redis. It:
  * emulates the four Lua scripts in plain Python (valid because Redis runs each
    script atomically on a single thread, which a single-threaded test
    reproduces),
  * injects fake `core.utils`, `apps.m3u.models`, `apps.m3u.connection_pool`,
    `apps.proxy.vod_proxy.utils` modules so the lazily-imported bits of patch.py
    resolve,
  * stubs the native reserve/release/select with simple counters that enforce
    max_streams.

Checks:
  * Emby burst (3 sessions, same ip+content) selects+reserves ONE slot and frees
    it once, after the last member ends -- and rider selection bypasses capacity.
  * Rider selection pins to the group's account (skips a different account).
  * Different client / content does NOT ride.
  * Partial-failure rollback keeps the slot for survivors.
  * Generator-close ordering keeps context for the teardown release on client
    disconnect.
  * Signature parity: an argument the wrapper does not know about is accepted
    AND forwarded to the original (the class of break that Dispatcharr v0.31.0
    caused by adding a parameter and passing it unconditionally).
  * A caller-supplied profile allowlist is honoured rather than overridden by
    the group pin.

The fakes deliberately mirror the CURRENT upstream call shape: `native_select`
carries v0.31.0's four-parameter signature and the `select()` helper passes
`restrict_to_profile_ids=` as a keyword on every call, exactly as
`_select_vod_stream` does. That is what lets this suite see a signature break at
all -- the pre-v1.2.0 fakes carried the old three-parameter signature, so the
suite passed green while the plugin was crashing every VOD request in the field.

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
        self.expire_calls = {}     # group_key -> count of EXPIRE refreshes
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
    k = keys[0]; now = args[1]
    h = store.setdefault(k, {})
    h["refcount"] = int(h.get("refcount", 0)) + 1
    h["last_activity"] = now
    if h["refcount"] == 1:
        h["created_at"] = now
    return [h["refcount"], 1 if h.get("reserved") == "1" else 0]


def _emu_undo(store, keys, args):
    k = keys[0]; h = store.get(k)
    if not h:
        return 0
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
    k = keys[0]; now = args[1]; h = store.get(k)
    if h is None:
        return [-1, 0]
    h["refcount"] = int(h.get("refcount", 0)) - 1
    rc = h["refcount"]; h["last_activity"] = now
    if rc <= 0:
        store.pop(k, None)
        return [1, rc]
    return [0, rc]


class FakeRedis:
    def register_script(self, text):
        if "HINCRBY" in text and "reserved == '1'" in text:
            return FakeScript(_emu_join)
        if "'refcount', -1" in text and "last_activity" in text:
            return FakeScript(_emu_leave)
        if "'refcount', -1" in text:
            return FakeScript(_emu_undo)
        if "'reserved', '1'" in text:
            return FakeScript(_emu_mark)
        raise AssertionError("unrecognised script")

    def hgetall(self, key):
        h = ENV.store.get(key)
        return dict(h) if h else {}

    def expire(self, key, ttl):
        # Model Redis EXPIRE: only affects an existing key; record refreshes.
        if key in ENV.store:
            ENV.expire_calls[key] = ENV.expire_calls.get(key, 0) + 1
            return 1
        return 0


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


def reserve(profile, ip, uuid):
    patch._set_ctx(ip, uuid)
    ok = patch.patched_reserve(MGR, profile)
    return ok, patch._snapshot_ctx()


def release(pid, plan):
    patch._restore_ctx(plan)
    patch.patched_decrement(MGR, pid)
    patch._reset_ctx()


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
    ok1, plan1 = reserve(p3, ip, uuid)
    check("owner reserves", ok1 and ENV.profile_counts[3] == 1)

    # Req #2 (rider): selection must BYPASS capacity (counter is 1 == max).
    sel2 = select(acct, ip, uuid)
    check("rider selection bypasses capacity (no failover)", bool(sel2) and sel2[0].id == 3)
    ok2, plan2 = reserve(p3, ip, uuid)
    check("rider reserves by riding (counter stays 1)", ok2 and ENV.profile_counts[3] == 1)

    # Req #3 (rider)
    sel3 = select(acct, ip, uuid)
    check("rider #3 selection bypasses capacity", bool(sel3) and sel3[0].id == 3)
    ok3, plan3 = reserve(p3, ip, uuid)
    check("rider #3 rides", ok3 and ENV.profile_counts[3] == 1)

    check("exactly one native reservation total", ENV.native_reserves == 1)

    # teardown
    release(3, plan2); check("counter 1 after first member ends", ENV.profile_counts[3] == 1)
    release(3, plan1); check("counter 1 after second ends", ENV.profile_counts[3] == 1)
    release(3, plan3); check("counter 0 after last ends", ENV.profile_counts[3] == 0)
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

    select(acct, "10.0.0.5", "uuidA"); ok1, plan1 = reserve(p3, "10.0.0.5", "uuidA")
    # different ip, same content -> different group, no bypass
    sel2 = select(acct, "10.0.0.9", "uuidA")
    check("different client does NOT get a capacity bypass", sel2 is None)
    ok2, plan2 = reserve(p3, "10.0.0.9", "uuidA")
    check("different client hits capacity at reservation too", ok2 is False)
    check("counter is 1", ENV.profile_counts[3] == 1)
    release(3, plan1); check("counter 0 after owner ends", ENV.profile_counts[3] == 0)


def test_partial_failure_keeps_slot():
    print("test_partial_failure_keeps_slot")
    p3 = FakeProfile(3, max_streams=1, account_id=7)
    setup([p3])
    ip, uuid = "10.0.0.5", "uuidP"
    _, plan1 = reserve(p3, ip, uuid)   # owner
    _, plan2 = reserve(p3, ip, uuid)   # rider
    release(3, plan2); check("slot held for survivor", ENV.profile_counts[3] == 1)
    check("no native release yet", ENV.native_releases == 0)
    release(3, plan1); check("freed after owner ends", ENV.profile_counts[3] == 0)
    check("one native release total", ENV.native_releases == 1)


def test_generator_close_ordering():
    print("test_generator_close_ordering")
    setup([FakeProfile(3, 1, 7)])
    seen = {}

    def make_native_gen():
        patch._local.plan = "GROUP"
        patch._local.group_key = "vodcc:grp:10.0.0.5:uuidW"
        try:
            for _ in range(5):
                yield b"chunk"
        finally:
            seen["plan_at_teardown"] = getattr(patch._local, "plan", None)

    def run(disconnect_after=None):
        class Resp:
            def __init__(self, gen):
                self.streaming_content = gen

        def fake_orig(*a, **k):
            return Resp(make_native_gen())

        saved = patch._orig_stream_vod
        patch._orig_stream_vod = fake_orig
        try:
            # request dict carries ip; content_id at args[2]
            resp = patch.patched_stream_vod({"ip": "10.0.0.5"}, "movie", "uuidW")
            gen = resp.streaming_content
            n = 0
            for _c in gen:
                n += 1
                if disconnect_after and n >= disconnect_after:
                    gen.close()
                    break
        finally:
            patch._orig_stream_vod = saved

    seen.clear(); run()
    check("plan present at native teardown (normal completion)",
          seen.get("plan_at_teardown") == "GROUP")
    seen.clear(); run(disconnect_after=2)
    check("plan present at native teardown (client disconnect)",
          seen.get("plan_at_teardown") == "GROUP")
    check("context reset after stream ends", getattr(patch._local, "plan", None) is None)


def test_group_ttl_refresh_during_streaming():
    # Regression for the seek-failover bug: a steady playback connection must
    # keep its coalescing group alive (refresh TTL) so a later seek finds the
    # group and rides it instead of failing over.
    print("test_group_ttl_refresh_during_streaming")
    setup([FakeProfile(5, 1, 5)])
    gkey = "vodcc:grp:10.0.0.5:uuidS"
    ENV.store[gkey] = {"refcount": 1, "reserved": "1", "profile_id": "5",
                       "account_id": "5"}  # group exists (owner streaming)

    saved_interval = patch.GROUP_REFRESH_INTERVAL_SECONDS
    patch.GROUP_REFRESH_INTERVAL_SECONDS = 0  # force a refresh on each chunk

    def native_gen():
        for _ in range(3):
            yield b"x"

    class Resp:
        def __init__(self, g):
            self.streaming_content = g

    def fake_orig(*a, **k):
        # simulate patched_reserve having set the group plan during the call
        patch._local.plan = "GROUP"
        patch._local.group_key = gkey
        return Resp(native_gen())

    saved = patch._orig_stream_vod
    patch._orig_stream_vod = fake_orig
    try:
        resp = patch.patched_stream_vod({"ip": "10.0.0.5"}, "movie", "uuidS")
        list(resp.streaming_content)  # consume the stream
    finally:
        patch._orig_stream_vod = saved
        patch.GROUP_REFRESH_INTERVAL_SECONDS = saved_interval

    check("group TTL refreshed while streaming (>=1 EXPIRE on the group key)",
          ENV.expire_calls.get(gkey, 0) >= 1)
    check("no group refresh once context/plan is torn down",
          getattr(patch._local, "plan", None) is None)


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


def test_profile_allowlist_is_honoured():
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
    """Upstream can re-point the slot away from our pinned pick (v0.31.0).

    v0.31.0's manager gained `_resolve_effective_profile`, which prefers the
    profile already bound on the SESSION hash over the view's pick "so reserve
    and teardown always target the same counter". For a new session the stored
    id is None and it returns our pick, so the group pin still wins -- but for a
    session that already has a DIFFERENT profile bound (reconnect, idle-session
    adoption, create race) upstream overrides us, and then calls OUR patched
    reserve/decrement with a profile the group never pinned.

    It logs nothing when it overrides, so this cannot be observed in a live log
    directly -- only its effect on the counters can. That is what these three
    scenarios assert: whatever upstream re-points, the native profile counter
    must come back to 0 and native reserve/release must stay symmetric.
    """
    print("test_upstream_effective_profile_override")
    ip, uuid = "10.0.0.5", "content-uuid-effective"

    # --- Scenario 1: create-race swap INSIDE one request (manager:1313-1314) --
    # The only call site where our pick was already reserved earlier in the same
    # request (manager:1233), so upstream decrements OUR profile and reserves
    # ITS profile back to back, with the greenlet context still set. The
    # decrement consumes the group plan and the reserve has to re-establish it.
    p3 = FakeProfile(3, max_streams=1, account_id=7)   # group's pinned profile
    p5 = FakeProfile(5, max_streams=1, account_id=7)   # session's bound profile
    setup([p3, p5])

    select(FakeAccount(7), ip, uuid)
    ok_owner, plan_owner = reserve(p3, ip, uuid)
    check("owner holds the pinned profile", ok_owner and ENV.profile_counts[3] == 1)

    patch._set_ctx(ip, uuid)                       # second request, same greenlet
    sel = patch.patched_get_m3u_profile(FakeAccount(7), None, None,
                                        restrict_to_profile_ids=None)
    check("rider selection returns the pin", bool(sel) and sel[0].id == 3)
    check("rider rides the pin (manager:1233)",
          patch.patched_reserve(MGR, p3) is True)
    patch.patched_decrement(MGR, 3)                # manager:1313, drops our pick
    check("swap: reserving upstream's effective profile succeeds",
          patch.patched_reserve(MGR, p5) is True)  # manager:1314
    plan_rider = patch._snapshot_ctx()
    check("swap did NOT double-count the native counter",
          ENV.profile_counts[3] == 1 and ENV.profile_counts.get(5, 0) == 0)

    release(5, plan_rider)                         # rider ends on the swapped id
    check("slot still held for the owner after the rider leaves",
          ENV.profile_counts[3] == 1)
    release(3, plan_owner)
    check("counter returns to 0 after a swapped-profile request",
          ENV.profile_counts[3] == 0 and ENV.profile_counts.get(5, 0) == 0)
    check("native reserve/release stayed symmetric across the swap",
          ENV.native_reserves == 1 and ENV.native_releases == 1)

    # --- Scenario 2: the same swap when the group has no other member ---------
    # The leave empties the group (so it IS the last member and DOES release),
    # and the re-reserve becomes a fresh OWNER on the new profile. Both halves
    # must be real native calls, or the slot leaks.
    setup([FakeProfile(3, 1, 7), FakeProfile(5, 1, 7)])
    patch._set_ctx(ip, uuid)
    patch.patched_reserve(MGR, ENV.profiles[3])
    check("solo owner holds the pinned profile", ENV.profile_counts[3] == 1)
    patch.patched_decrement(MGR, 3)
    check("solo swap releases the abandoned profile", ENV.profile_counts[3] == 0)
    check("solo swap re-reserves as a fresh owner",
          patch.patched_reserve(MGR, ENV.profiles[5]) is True
          and ENV.profile_counts[5] == 1)
    plan = patch._snapshot_ctx()
    release(5, plan)
    check("solo swap: both counters back to 0",
          ENV.profile_counts[3] == 0 and ENV.profile_counts[5] == 0)
    check("solo swap: native reserve/release symmetric",
          ENV.native_reserves == 2 and ENV.native_releases == 2)

    # --- Scenario 3: reconnect / idle-session reuse (manager:1341, :1361) -----
    # No prior reserve in this request, so upstream reserves its effective
    # profile directly. The group is pinned to another profile, so this rides
    # the group and the effective profile's own counter is never touched.
    #
    # NOTE the semantic this encodes: the group holds the PINNED profile's slot
    # while this request streams from the EFFECTIVE profile, so the effective
    # profile carries one uncounted provider connection. That is the same
    # under-counting trade-off already documented in the README for two devices
    # sharing (IP, title) -- accounting stays balanced, which is what prevents a
    # leak or a negative counter. Asserted rather than assumed.
    setup([FakeProfile(3, 1, 7), FakeProfile(5, 1, 7)])
    select(FakeAccount(7), ip, uuid)
    _, plan_owner = reserve(ENV.profiles[3], ip, uuid)
    patch._set_ctx(ip, uuid)
    check("reconnect on a differently-bound profile rides the group",
          patch.patched_reserve(MGR, ENV.profiles[5]) is True)
    plan_reconnect = patch._snapshot_ctx()
    check("reconnect did not reserve the effective profile natively",
          ENV.profile_counts.get(5, 0) == 0 and ENV.native_reserves == 1)
    release(5, plan_reconnect)
    release(3, plan_owner)
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
    argument -- but this release is proof that "matches today" is not durable,
    and the cost of accepting/forwarding is nil.
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

    patch._set_ctx("10.0.0.5", "content-uuid-hooks")
    ok = patch.patched_reserve(MGR, p3, "future_positional", future_kw=1)
    check("reserve accepts unexpected args (no TypeError)", ok is True)
    check("reserve forwards unexpected args to native",
          seen.get("reserve") == (("future_positional",), {"future_kw": 1}))

    patch.patched_decrement(MGR, 3, "future_positional", future_kw=2)
    check("decrement accepts + forwards unexpected args",
          seen.get("decrement") == (("future_positional",), {"future_kw": 2}))
    check("slot released exactly once despite the extra args",
          ENV.profile_counts[3] == 0)
    patch._reset_ctx()


if __name__ == "__main__":
    test_emby_burst()
    test_rider_pins_to_group_account()
    test_different_client_does_not_ride()
    test_partial_failure_keeps_slot()
    test_generator_close_ordering()
    test_group_ttl_refresh_during_streaming()
    test_selection_signature_parity()
    test_profile_allowlist_is_honoured()
    test_backwards_compatible_with_pre_v0_31_callers()
    test_upstream_effective_profile_override()
    test_logger_has_its_own_level()
    test_manager_hook_signature_parity()
    print()
    if _failures:
        print(f"{len(_failures)} check(s) FAILED: {_failures}")
        sys.exit(1)
    print("All checks passed.")
