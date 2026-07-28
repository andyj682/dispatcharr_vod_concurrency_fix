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
    name = "Dispatcharr VOD Concurrency Fix"
    version = "0.2.0"
    description = (
        "Dispatcharr plugin that coalesces the near-simultaneous range requests "
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
                           "the worker that handles this request.",
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
            return {
                "status": "ok",
                "message": (
                    f"active={_patch._ACTIVE} in worker pid={os.getpid()} "
                    f"(note: this reflects ONE worker; check logs for all "
                    f"worker pids)"
                ),
            }
        return {"status": "error", "message": f"Unknown action: {action}"}

    def stop(self, context=None):
        """Called by Dispatcharr on disable / delete / reload."""
        _patch.uninstall()
        return {"status": "ok", "message": "VOD concurrency patch reverted"}
