"""Isolated socket tests; never touches the actual dashboard port/process."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import socket
import threading
import urllib.request
from unittest.mock import Mock

import pytest

import oanda_dashboard_exclusive_server_v1 as subject


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *_args):
        pass


def test_reuse_modes_disabled():
    assert subject.ExclusiveThreadingHTTPServer.allow_reuse_address is False
    assert subject.ExclusiveThreadingHTTPServer.allow_reuse_port is False


@pytest.mark.skipif(subject.os.name != "nt", reason="Windows socket contract")
def test_windows_option_is_present_on_real_listener():
    with subject.ExclusiveThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        assert server.socket.getsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE) == 1


@pytest.mark.parametrize("contender", [ThreadingHTTPServer, subject.ExclusiveThreadingHTTPServer])
def test_duplicate_listener_rejected_on_isolated_ephemeral_port(contender):
    with subject.ExclusiveThreadingHTTPServer(("127.0.0.1", 0), Handler) as owner:
        with pytest.raises(OSError):
            contender(owner.server_address, Handler)
        assert owner.socket.fileno() >= 0


def test_bind_failure_does_not_close_the_existing_listener():
    with subject.ExclusiveThreadingHTTPServer(("127.0.0.1", 0), Handler) as owner:
        original = owner.socket.fileno()
        for _ in range(2):
            with pytest.raises(OSError):
                subject.ExclusiveThreadingHTTPServer(owner.server_address, Handler)
        assert owner.socket.fileno() == original
        assert owner.socket.getsockname() == owner.server_address


def test_closed_unconnected_listener_releases_endpoint():
    with subject.ExclusiveThreadingHTTPServer(("127.0.0.1", 0), Handler) as first:
        address = first.server_address
    with subject.ExclusiveThreadingHTTPServer(address, Handler) as second:
        assert second.server_address == address


def test_existing_handler_and_http_response_are_preserved():
    with subject.ExclusiveThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        worker = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        worker.start()
        try:
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open("http://127.0.0.1:%d/" % server.server_port, timeout=2) as response:
                assert response.status == 200
                assert response.read(3) == b"ok"
        finally:
            server.shutdown()
            worker.join(timeout=2)
        assert not worker.is_alive()


@pytest.mark.parametrize("previous", [ThreadingHTTPServer, subject.ExclusiveThreadingHTTPServer])
def test_hot_reload_same_port_after_actual_http_request(previous):
    # Cover Windows TIME_WAIT in both legacy -> exclusive migration and future
    # exclusive -> exclusive reloads; the existing unconnected case is weaker.
    with previous(("127.0.0.1", 0), Handler) as owner:
        address = owner.server_address
        worker = threading.Thread(target=owner.serve_forever, kwargs={"poll_interval": 0.01})
        worker.start()
        try:
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            with opener.open("http://127.0.0.1:%d/" % address[1], timeout=2) as response:
                assert response.status == 200
                assert response.read(3) == b"ok"
        finally:
            owner.shutdown()
            worker.join(timeout=2)
        assert not worker.is_alive()
    with subject.ExclusiveThreadingHTTPServer(address, Handler) as successor:
        assert successor.server_address == address
        assert successor.socket.fileno() >= 0


def test_missing_windows_option_refuses_before_bind(monkeypatch):
    monkeypatch.setattr(subject.os, "name", "nt")
    monkeypatch.delattr(subject.socket, "SO_EXCLUSIVEADDRUSE", raising=False)
    server = object.__new__(subject.ExclusiveThreadingHTTPServer)
    server.socket = Mock()
    bind = Mock()
    monkeypatch.setattr(ThreadingHTTPServer, "server_bind", bind)
    with pytest.raises(RuntimeError, match="exclusive_socket_option_unavailable"):
        server.server_bind()
    server.socket.setsockopt.assert_not_called()
    bind.assert_not_called()


def test_option_failure_never_falls_back_to_nonexclusive_bind(monkeypatch):
    monkeypatch.setattr(subject.os, "name", "nt")
    monkeypatch.setattr(subject.socket, "SO_EXCLUSIVEADDRUSE", -5, raising=False)
    server = object.__new__(subject.ExclusiveThreadingHTTPServer)
    server.socket = Mock()
    server.socket.setsockopt.side_effect = OSError("fixture_option_failure")
    bind = Mock()
    monkeypatch.setattr(ThreadingHTTPServer, "server_bind", bind)
    with pytest.raises(OSError, match="fixture_option_failure"):
        server.server_bind()
    bind.assert_not_called()


def test_exclusive_option_precedes_real_bind(monkeypatch):
    monkeypatch.setattr(subject.os, "name", "nt")
    monkeypatch.setattr(subject.socket, "SO_EXCLUSIVEADDRUSE", -5, raising=False)
    events = []
    server = object.__new__(subject.ExclusiveThreadingHTTPServer)
    server.socket = Mock()
    server.socket.setsockopt.side_effect = lambda *args: events.append(("exclusive", args))
    monkeypatch.setattr(ThreadingHTTPServer, "server_bind", lambda self: events.append(("bind", None)))
    server.server_bind()
    assert [row[0] for row in events] == ["exclusive", "bind"]
    assert events[0][1] == (socket.SOL_SOCKET, -5, 1)
