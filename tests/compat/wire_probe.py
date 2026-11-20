import socket
import struct
import sys


def recv(conn, size):
    data = b""
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        assert chunk, "incomplete response"
        data += chunk
    return data


def exchange(endpoint, key, version, body, response=True):
    host, port = endpoint.rsplit(":", 1)
    request = struct.pack(">hhi", key, version, 42) + body
    frame = struct.pack(">i", len(request)) + request
    with socket.create_connection((host, int(port)), timeout=5) as conn:
        conn.sendall(frame[:2])
        conn.sendall(frame[2:9])
        conn.sendall(frame[9:])
        if not response:
            assert conn.recv(1) == b"", "unsupported request returned a response"
            return
        size, = struct.unpack(">i", recv(conn, 4))
        assert 6 <= size <= 8192
        data = recv(conn, size)
        correlation, error = struct.unpack(">ih", data[:6])
        assert correlation == 42, correlation
        return error, data


def main(endpoint):
    legacy = b"\xff\xff"
    extension_tag = bytes([1, 12, 2, 89, 90])
    for version in range(5):
        body = legacy if version < 3 else legacy + extension_tag + b"\x05test\x02v" + extension_tag
        code, _ = exchange(endpoint, 18, version, body)
        assert code == 0, (version, code)
    error, fallback = exchange(endpoint, 18, 99, b"")
    assert error == 35, error
    count, = struct.unpack(">i", fallback[6:10])
    assert len(fallback) == 10 + 6 * count
    versions = {key: (first, last) for key, first, last in
                struct.iter_unpack(">hhh", fallback[10:])}
    assert versions[20] == (1, 6) and versions[32] == (1, 4) and versions[44] == (1, 1)
    assert 37 not in versions
    exchange(endpoint, 37, 3, b"", response=False)
    exchange(endpoint, 20, 0, b"", response=False)
    exchange(endpoint, 19, 4, legacy + struct.pack(">i", 4097), response=False)
    exchange(endpoint, 44, 1, legacy + b"\0\xff\xff\x7f", response=False)
    print("fragmented ApiVersions 0-4, tagged extensions, v0 fallback and rejected frames passed")


if __name__ == "__main__":
    main(sys.argv[1])
