"""
Dispatcharr VOD Concurrency Fix
===============================

Coalesces the burst of near-simultaneous HTTP range requests that Emby /
libmpv (and similar MKV direct-play clients) fire when opening a VOD file, so
they share ONE provider connection slot instead of each tripping a
`max_streams: 1` profile and failing over to a different provider account /
different underlying file.

See patch.py for the full design writeup. This module is the Dispatcharr plugin
entry point: it applies the monkeypatch at import time (Dispatcharr imports an
enabled plugin's code in every uWSGI worker at boot) and reverts it in stop().

Author: andyj682
License: MIT
"""

import logging

logger = logging.getLogger("plugins.dispatcharr_vod_concurrency_fix")

# Apply the patch as soon as the module is imported. Dispatcharr only imports an
# enabled plugin's code, and under `lazy-apps = true` every uWSGI worker imports
# it at boot -- so importing == "this worker should be patched".
try:
    from . import patch as _patch
except Exception:  # pragma: no cover - fall back to flat import layout
    import patch as _patch

try:
    _patch.install()
except Exception:  # never break app startup because of the plugin
    logger.exception("[VOD-CC] auto-install on import failed")


class Plugin:
    name = "VOD Concurrency Fix"
    version = "1.3.0"
    description = (
        "Coalesces the near-simultaneous range requests "
        "from some clients (notably Emby) when playing MKV VOD files so they "
        "share one provider slot instead of failing over to a different file "
        "and corrupting playback."
    )
    author = "andyj682"
    help_url = "https://github.com/andyj682/dispatcharr_vod_concurrency_fix"

    # No user configuration required.
    fields = []

    # A status button so the user can confirm the patch is live in a worker.
    actions = [
        {
            "id": "status",
            "label": "Show patch status",
            "description": "Report whether the concurrency patch is active in "
                           "the process that runs this button. On Dispatcharr "
                           "0.34+ that is a background worker, not a streaming "
                           "one; the logs show the streaming workers.",
            "button_label": "Check status",
            "button_variant": "outline",
        },
    ]

    def run(self, action=None, params=None, context=None):
        context = context or {}
        if action == "enable":
            ok = _patch.install()
            return {
                "status": "ok" if ok else "error",
                "message": "VOD concurrency patch installed"
                if ok else "Failed to install (see logs)",
            }
        if action == "disable":
            _patch.uninstall()
            return {"status": "ok", "message": "VOD concurrency patch reverted"}
        if action == "status":
            import os
            # On Dispatcharr 0.34+ actions run in a Celery child, which never
            # serves VOD, so this can only describe the process it runs in.
            # Streaming workers log their own "[VOD-CC] active in worker
            # pid=... at stream" line the first time they handle VOD.
            state = "active" if _patch._ACTIVE else "NOT active"
            return {
                "status": "ok",
                "message": (
                    f"Patch {state} in the process that ran this check "
                    f"(pid {os.getpid()}). On Dispatcharr 0.34+ that is a "
                    f"background worker, not one that streams VOD. To confirm "
                    f"the streaming workers, look for '[VOD-CC] active in "
                    f"worker pid=... at stream' in the logs after a play."
                ),
            }
        return {"status": "error", "message": f"Unknown action: {action}"}

    def stop(self, context=None):
        """Called by Dispatcharr on disable / delete / reload."""
        _patch.uninstall()
        return {"status": "ok", "message": "VOD concurrency patch reverted"}
