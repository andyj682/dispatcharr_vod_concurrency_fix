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


def native_select(m3u_account, profile_id, session_id=None):
    """Native selection stub: pick the account's profile if it has capacity."""
    for pid, prof in ENV.profiles.items():
        if prof.m3u_account_id == m3u_account.id:
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


def select(account, ip, uuid, profile_id=None):
    patch._set_ctx(ip, uuid)
    return patch.patched_get_m3u_profile(account, profile_id)


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
    ip, uuid = "10.0.0.5", "movie-1905687"

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


if __name__ == "__main__":
    test_emby_burst()
    test_rider_pins_to_group_account()
    test_different_client_does_not_ride()
    test_partial_failure_keeps_slot()
    test_generator_close_ordering()
    print()
    if _failures:
        print(f"{len(_failures)} check(s) FAILED: {_failures}")
        sys.exit(1)
    print("All checks passed.")
