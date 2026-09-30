"""Unit tests for the gRPC<->Kubernetes port-forward socket bridge.

These use a real loopback ``socketpair`` for the "remote" (pod) side and a
real TCP client for the "local" (gRPC channel) side, so the bidirectional
pump and its teardown are exercised without a cluster.
"""

from __future__ import annotations

import socket

from execution_plane.worker_manager.vanilla_k8s.forward import forward_socket


def _connect(address: str) -> socket.socket:
    host, port = address.rsplit(":", 1)
    client = socket.create_connection((host, int(port)), timeout=2)
    client.settimeout(2)
    return client


class TestForwardSocket:
    """forward_socket bridges bytes both ways and tears the bridge down."""

    def test_forwards_bytes_in_both_directions(self) -> None:
        remote, pod = socket.socketpair()
        pod.settimeout(2)
        try:
            with forward_socket(remote) as address:
                client = _connect(address)
                try:
                    client.sendall(b"ping")
                    assert pod.recv(1024) == b"ping"

                    pod.sendall(b"pong")
                    assert client.recv(1024) == b"pong"
                finally:
                    client.close()
        finally:
            pod.close()

    def test_yields_a_loopback_address(self) -> None:
        remote, pod = socket.socketpair()
        try:
            with forward_socket(remote) as address:
                assert address.startswith("127.0.0.1:")
                assert int(address.rsplit(":", 1)[1]) > 0
        finally:
            pod.close()

    def test_listener_is_closed_after_exit(self) -> None:
        remote, pod = socket.socketpair()
        try:
            with forward_socket(remote) as address:
                pass
            # The listener socket is closed on exit, so a fresh connect is refused.
            host, port = address.rsplit(":", 1)
            try:
                refused = False
                conn = socket.create_connection((host, int(port)), timeout=1)
                conn.close()
            except OSError:
                refused = True
            assert refused
        finally:
            pod.close()
