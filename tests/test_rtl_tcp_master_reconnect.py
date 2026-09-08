#!/usr/bin/env python3
"""Hardware integration test for replacing rtl_tcp's master connection.

Start the patched rtl_tcp with a disposable device and ports, for example:

    rtl_tcp -a 127.0.0.1 -d v401 -p 2230 -l 2231 -s 1024000
    python3 tests/test_rtl_tcp_master_reconnect.py --master-port 2230 \
        --data-port 2231

The test deliberately disconnects the master while keeping the read-only data
client connected. It fails against the previous implementation because the
second master is never accepted and receives no RTL0 header.
"""

import argparse
import socket
import threading
import time


DONGLE_INFO_BYTES = 12


def receive_exact(sock: socket.socket, size: int) -> bytes:
    chunks = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise RuntimeError("connection closed before the RTL0 header arrived")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def connect_and_check_header(host: str, port: int, timeout: float) -> socket.socket:
    sock = socket.create_connection((host, port), timeout=timeout)
    sock.settimeout(timeout)
    header = receive_exact(sock, DONGLE_INFO_BYTES)
    if header[:4] != b"RTL0":
        sock.close()
        raise RuntimeError(f"unexpected rtl_tcp header: {header!r}")
    return sock


def wait_for_bytes(counter: list[int], minimum: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if counter[0] >= minimum:
            return
        time.sleep(0.05)
    raise RuntimeError(f"data client received only {counter[0]} bytes")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--master-port", type=int, required=True)
    parser.add_argument("--data-port", type=int, required=True)
    parser.add_argument("--timeout", type=float, default=8.0)
    args = parser.parse_args()

    master = connect_and_check_header(args.host, args.master_port, args.timeout)
    data = connect_and_check_header(args.host, args.data_port, args.timeout)
    data.settimeout(0.5)

    received = [0]
    stop_reader = threading.Event()

    def consume_data() -> None:
        while not stop_reader.is_set():
            try:
                chunk = data.recv(65536)
            except socket.timeout:
                continue
            except OSError:
                if stop_reader.is_set():
                    return
                raise
            if not chunk:
                return
            received[0] += len(chunk)

    reader = threading.Thread(target=consume_data, daemon=True)
    reader.start()

    wait_for_bytes(received, 262144, args.timeout)
    before_disconnect = received[0]
    master.close()

    deadline = time.monotonic() + args.timeout
    replacement = None
    last_error = None
    while time.monotonic() < deadline and replacement is None:
        try:
            replacement = connect_and_check_header(
                args.host, args.master_port, min(1.0, args.timeout)
            )
        except (OSError, RuntimeError) as error:
            last_error = error
            time.sleep(0.1)

    if replacement is None:
        raise RuntimeError(f"replacement master was not accepted: {last_error}")

    wait_for_bytes(received, before_disconnect + 262144, args.timeout)

    replacement.close()
    stop_reader.set()
    try:
        data.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    reader.join(timeout=1.0)
    data.close()
    print(
        "PASS: replacement master accepted and existing data client resumed "
        f"({received[0]} I/Q bytes observed)"
    )


if __name__ == "__main__":
    main()
