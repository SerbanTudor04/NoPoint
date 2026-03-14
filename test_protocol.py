"""
ClouDrive - Integration Tests
==============================
Spins up a real in-process server and exercises every protocol operation.
"""

import asyncio
import os
import sys
import tempfile
import logging
from pathlib import Path

# Make sure imports work from project root
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from server.server import ClouDriveServer
from client.client import ClouDriveClient
from protocol.message import AuthError, ChecksumError

logging.basicConfig(level=logging.WARNING)   # set INFO for verbose output

HOST    = "127.0.0.1"
PORT    = 19876   # use a non-standard port for tests
PASS_OK = {"alice": "hunter2", "bob": "p@ssw0rd"}


# ── Helpers ────────────────────────────────────────────────────────────────────
OK   = "\033[32m✓\033[0m"
FAIL = "\033[31m✗\033[0m"

passed = failed = 0

def check(label: str, condition: bool, detail: str = ""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  {OK}  {label}")
    else:
        failed += 1
        print(f"  {FAIL}  {label}" + (f"  ({detail})" if detail else ""))


# ── Server fixture ─────────────────────────────────────────────────────────────
async def start_server(storage_dir: str):
    srv = ClouDriveServer(host=HOST, port=PORT, storage_root=storage_dir, users=PASS_OK)
    task = asyncio.create_task(srv.serve_forever())
    await asyncio.sleep(0.1)   # let server bind
    return task


# ── Test cases ─────────────────────────────────────────────────────────────────
async def test_auth(tmpdir):
    print("\n── Auth ──────────────────────────────────────────────────────────")

    # Good credentials
    client = ClouDriveClient(HOST, PORT)
    try:
        await client.connect("alice", "hunter2")
        check("Valid login succeeds", True)
    except Exception as e:
        check("Valid login succeeds", False, str(e))
    finally:
        await client.disconnect()

    # Bad credentials
    client2 = ClouDriveClient(HOST, PORT)
    try:
        await client2.connect("alice", "wrongpassword")
        check("Invalid login raises AuthError", False, "No exception raised")
        await client2.disconnect()
    except AuthError:
        check("Invalid login raises AuthError", True)
    except Exception as e:
        check("Invalid login raises AuthError", False, str(e))


async def test_ping(tmpdir):
    print("\n── Ping ──────────────────────────────────────────────────────────")
    async with ClouDriveClient(HOST, PORT) as c:
        await c.connect("alice", "hunter2")
        rtt = await c.ping()
        check(f"Ping round-trip ({rtt*1000:.1f}ms)", rtt < 1.0)


async def test_upload_download(tmpdir):
    print("\n── Upload / Download ─────────────────────────────────────────────")
    # Create a test file
    src = Path(tmpdir) / "hello.txt"
    src.write_bytes(b"Hello, ClouDrive! " * 100)   # 1800 bytes

    async with ClouDriveClient(HOST, PORT) as c:
        await c.connect("alice", "hunter2")

        # Upload
        progress_calls = []
        await c.upload(str(src), "docs/hello.txt",
                       on_progress=lambda s, t: progress_calls.append((s, t)))
        check("Upload completes without error", True)
        check("Progress callback fired", len(progress_calls) > 0)
        check("Final progress matches file size", progress_calls[-1][0] == src.stat().st_size)

        # Download
        dst = Path(tmpdir) / "downloaded.txt"
        await c.download("docs/hello.txt", str(dst))
        check("Download completes", dst.exists())
        check("Downloaded content matches original", dst.read_bytes() == src.read_bytes())


async def test_large_file(tmpdir):
    print("\n── Large File (chunked) ──────────────────────────────────────────")
    big = Path(tmpdir) / "big.bin"
    big.write_bytes(os.urandom(3 * 1024 * 1024))   # 3 MB → 3 chunks

    async with ClouDriveClient(HOST, PORT) as c:
        await c.connect("alice", "hunter2")

        chunks_seen = []
        await c.upload(str(big), "big.bin",
                       on_progress=lambda s, t: chunks_seen.append(s))
        check("3 MB upload completes", True)
        check("Multiple chunks sent", len(chunks_seen) >= 3)

        dst = Path(tmpdir) / "big_dl.bin"
        await c.download("big.bin", str(dst))
        check("3 MB download completes", dst.exists())
        check("Large file integrity", dst.read_bytes() == big.read_bytes())


async def test_filesystem(tmpdir):
    print("\n── Filesystem ops ────────────────────────────────────────────────")
    src = Path(tmpdir) / "test.bin"
    src.write_bytes(b"data" * 10)

    async with ClouDriveClient(HOST, PORT) as c:
        await c.connect("alice", "hunter2")

        # mkdir
        await c.mkdir("projects/alpha")
        check("mkdir creates directory", True)

        # upload into it
        await c.upload(str(src), "projects/alpha/test.bin")

        # list_dir
        entries = await c.list_dir("projects/alpha")
        names = [e["name"] for e in entries]
        check("list_dir returns uploaded file", "test.bin" in names)

        # stat
        info = await c.stat("projects/alpha/test.bin")
        check("stat returns file type",    info["type"] == "file")
        check("stat returns correct size", info["size"] == src.stat().st_size)
        check("stat returns sha256",       "sha256" in info)

        # move
        await c.move("projects/alpha/test.bin", "projects/alpha/renamed.bin")
        entries2 = await c.list_dir("projects/alpha")
        names2 = [e["name"] for e in entries2]
        check("move: new name exists",    "renamed.bin" in names2)
        check("move: old name gone",      "test.bin" not in names2)

        # delete file
        await c.delete("projects/alpha/renamed.bin")
        entries3 = await c.list_dir("projects/alpha")
        check("delete removes file", not any(e["name"] == "renamed.bin" for e in entries3))

        # delete dir
        await c.delete("projects/alpha")
        root_entries = await c.list_dir(".")
        check("delete removes directory", not any(e["name"] == "alpha" for e in root_entries))


async def test_isolation(tmpdir):
    print("\n── User isolation ────────────────────────────────────────────────")
    src = Path(tmpdir) / "secret.txt"
    src.write_bytes(b"alice's secret data")

    # Alice uploads
    async with ClouDriveClient(HOST, PORT) as alice:
        await alice.connect("alice", "hunter2")
        await alice.upload(str(src), "secret.txt")

    # Bob should NOT see alice's file
    async with ClouDriveClient(HOST, PORT) as bob:
        await bob.connect("bob", "p@ssw0rd")
        entries = await bob.list_dir(".")
        names = [e["name"] for e in entries]
        check("Bob cannot see Alice's files", "secret.txt" not in names)


# ── Runner ─────────────────────────────────────────────────────────────────────
async def main():
    with tempfile.TemporaryDirectory() as tmpdir:
        srv_task = await start_server(tmpdir)

        try:
            await test_auth(tmpdir)
            await test_ping(tmpdir)
            await test_upload_download(tmpdir)
            await test_large_file(tmpdir)
            await test_filesystem(tmpdir)
            await test_isolation(tmpdir)
        finally:
            srv_task.cancel()
            try:
                await srv_task
            except asyncio.CancelledError:
                pass

    total = passed + failed
    print(f"\n{'═'*52}")
    print(f"  Results: {passed}/{total} passed", end="")
    if failed:
        print(f"  \033[31m({failed} failed)\033[0m")
    else:
        print(f"  \033[32m(all green)\033[0m")
    print(f"{'═'*52}\n")
    return failed


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))