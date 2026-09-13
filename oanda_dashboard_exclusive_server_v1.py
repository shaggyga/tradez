"""Exclusive dashboard listener boundary; no server starts on import.

On Windows, SO_REUSEADDR can admit two listeners to the same endpoint.
Set SO_EXCLUSIVEADDRUSE before bind, and disable both reuse options.  This
protects the listener itself across independent launchers/processes; it is
not process adoption, supervisor health, or HTTP-data freshness validation.
The caller retains its existing request handler, address and serve loop.
"""

from http.server import ThreadingHTTPServer
import os
import socket


SCHEMA_VERSION = "dashboard_exclusive_listener_v1_20260911"


class ExclusiveThreadingHTTPServer(ThreadingHTTPServer):
    """Require sole endpoint ownership before accepting any HTTP request."""

    allow_reuse_address = False
    allow_reuse_port = False

    def server_bind(self):
        if os.name == "nt":
            option = getattr(socket, "SO_EXCLUSIVEADDRUSE", None)
            if option is None:
                raise RuntimeError("dashboard_exclusive_socket_option_unavailable")
            # Socket errors propagate: never retry using a nonexclusive bind.
            self.socket.setsockopt(socket.SOL_SOCKET, option, 1)
        return super().server_bind()
