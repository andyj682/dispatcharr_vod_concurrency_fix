"""Run the plugin's Lua scripts against Dispatcharr's REAL Redis.

`test_logic.py` exercises the plugin against Python emulators of its Lua
scripts. This checks that the scripts themselves behave the same way on a real
Redis (nil handling, integer replies, TTL, deletion). It touches ONE throwaway
key, `vodcc:selftest:<random>`, and deletes it at the end -- never a real group
(`vodcc:grp:*`) -- and it does not install or alter the running plugin.

Run inside the container (pipe this file in on stdin):

    docker exec -i <container> su - dispatch -c "cd /app && python manage.py shell" < lua_selftest.py

Expected last line: `Lua self-test: N/N passed`.
"""

import glob
import importlib.util
import uuid

paths = sorted(set(glob.glob("/data/plugins/dispatcharr_vod_concurrency_fix/patch.py")
                   + glob.glob("/app/data/plugins/dispatcharr_vod_concurrency_fix/patch.py")))
if not paths:
    raise SystemExit("patch.py not found under /data/plugins or /app/data/plugins")
spec = importlib.util.spec_from_file_location("vodcc_lua_selftest", paths[0])
plugin = importlib.util.module_from_spec(spec)
spec.loader.exec_module(plugin)     # defines the scripts only; installs nothing

from core.utils import RedisClient  # noqa: E402

r = RedisClient.get_client()
S = plugin._get_scripts(r)
k = "vodcc:selftest:" + uuid.uuid4().hex
results = []


def chk(name, cond):
    results.append(bool(cond))
    print(("  PASS  " if cond else "  FAIL  ") + name)


def pair(v):
    return tuple(int(x) for x in v)


def field(name):
    v = r.hget(k, name)
    return None if v is None else int(v)


print("patch.py:", paths[0])
try:
    chk("join A on an empty group -> refcount 1, not reserved",
        pair(S["join"](keys=[k], args=[60, "t", "m:A"])) == (1, 0))
    chk("group TTL set (1..60s)", 0 < r.ttl(k) <= 60)
    chk("mark reserved", int(S["mark"](keys=[k], args=[60, "5", "7"])) == 1)
    chk("join B -> refcount 2, reserved",
        pair(S["join"](keys=[k], args=[60, "t", "m:B"])) == (2, 1))
    chk("join B again (same session twice) -> refcount 3",
        pair(S["join"](keys=[k], args=[60, "t", "m:B"])) == (3, 1))
    chk("member counts: A=1, B=2", field("m:A") == 1 and field("m:B") == 2)
    chk("touch by a member session refreshes",
        int(S["touch"](keys=[k], args=[60, "m:B"])) == 1)
    chk("touch by a non-member session does not",
        int(S["touch"](keys=[k], args=[60, "m:C"])) == 0)
    chk("leave by a non-member -> -2, nothing changed",
        pair(S["leave"](keys=[k], args=[60, "t", "m:C"])) == (-2, 3)
        and field("refcount") == 3)
    chk("leave B -> 0 (members remain), refcount 2",
        pair(S["leave"](keys=[k], args=[60, "t", "m:B"])) == (0, 2))
    chk("undo B -> refcount 1 and B's field removed",
        int(S["undo"](keys=[k], args=["m:B"])) == 1 and field("m:B") is None)
    chk("leave A -> 1 (last member) and the group is deleted",
        pair(S["leave"](keys=[k], args=[60, "t", "m:A"]))[0] == 1
        and not r.exists(k))
    chk("leave on a gone group -> -1",
        pair(S["leave"](keys=[k], args=[60, "t", "m:A"])) == (-1, 0))
    chk("touch on a gone group does nothing and does not resurrect it",
        int(S["touch"](keys=[k], args=[60, "m:A"])) == 0 and not r.exists(k))
    chk("undo on a gone group -> 0, still gone",
        int(S["undo"](keys=[k], args=["m:A"])) == 0 and not r.exists(k))
finally:
    r.delete(k)

print(f"Lua self-test: {sum(results)}/{len(results)} passed")
