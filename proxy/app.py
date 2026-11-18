"""Single-process HTTP caching proxy for fixed-origin package downloads.

FastAPI (HTTP surface) + aiohttp (origin client). One uvicorn worker, all
state in this process; completed packages live on disk under CACHE_DIR.

Design notes
------------
* Single-flight: one fill task per path generation; concurrent misses attach
  to the same fill and read the growing ``.part-<gen>`` file from offset 0.
* Slow clients never block the fill or other clients: the fill only writes to
  disk; every reader tails the file at its own pace. Per-client memory is one
  read buffer, independent of package size.
* Purge unregisters the current generation. In-flight fills keep feeding
  their existing readers but are never re-registered; old files are unlinked
  once the last reader detaches.
* Origin failure fails every attached download and never leaves a cached
  half-package. Startup deletes leftover ``.part-*`` files and keeps complete
  packages.
* Verified downloads (``GET /verified/{path}``) share the same generation and
  single-flight fill as plain downloads. The caller supplies the expected
  SHA-256; the proxy re-hashes the whole file block by block (never trusting
  the origin's ``X-Package-Sha256``) and only starts the response once the
  digest matches. Verification and delivery use one pinned file descriptor,
  so a purge plus a newer fill cannot swap bytes under an accepted download.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import threading
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import aiohttp
from fastapi import FastAPI, Header
from fastapi.responses import PlainTextResponse, StreamingResponse

ORIGIN_BASE_URL = os.environ.get("ORIGIN_BASE_URL", "http://127.0.0.1:9000").rstrip("/")
CACHE_DIR = Path(os.environ.get("CACHE_DIR", "./cache")).resolve()
CHUNK = 256 * 1024  # bytes per origin read / client write / disk read
HASH_BLOCK = 1024 * 1024  # bytes per digest block

_SEGMENT_RE = re.compile(r"\A[A-Za-z0-9._~!$&'()*+,;=:@-]+\Z")
_SHA256_RE = re.compile(r"\A[0-9a-fA-F]{64}\Z")
_SHA256_PREFIX_RE = re.compile(r"\Asha256:", re.IGNORECASE)


def validate_key(raw: str) -> str:
    """Map a URL path to a safe relative cache key; reject traversal."""
    if not raw or raw.endswith("/"):
        raise ValueError("empty package path")
    parts = raw.split("/")
    for seg in parts:
        if seg in ("", ".", ".."):
            raise ValueError(f"illegal path segment: {seg!r}")
        if not _SEGMENT_RE.match(seg):
            raise ValueError(f"illegal characters in segment: {seg!r}")
    return "/".join(parts)


def _unlink_quiet(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


class PinnedFile:
    """A file descriptor that is never closed while a worker thread reads it.

    Reads/hashes run in ``to_thread`` workers that cannot be interrupted. If
    the event loop cancels mid-read (client disconnect) and the file object
    were closed immediately, the descriptor number could be recycled and the
    next ``open`` could land on it, corrupting the in-flight read. So a close
    requested while a worker is active is deferred until that worker finishes.
    """

    def __init__(self, f) -> None:
        self._f = f
        self._lock = threading.Lock()
        self._busy = 0
        self._closed = False

    def run(self, fn, *args):
        """Run ``fn(f, *args)`` on the worker thread; returns its result."""
        with self._lock:
            if self._closed:
                raise ValueError("I/O operation on closed pinned file")
            self._busy += 1
        try:
            return fn(self._f, *args)
        finally:
            close_now = False
            with self._lock:
                self._busy -= 1
                if self._busy == 0 and self._closed:
                    close_now = True
            if close_now:
                self._f.close()

    def close(self) -> None:
        with self._lock:
            self._closed = True
            if self._busy:
                return  # the active worker closes it when it finishes
            f = self._f
        f.close()


def _read_some(f, limit: int) -> bytes:
    return f.read(limit)


def parse_expected_sha256(raw: str | None) -> str | None:
    """Normalise an expected digest header; None means missing/invalid."""
    if raw is None:
        return None
    value = raw.strip()
    value = _SHA256_PREFIX_RE.sub("", value, count=1)
    if not _SHA256_RE.match(value):
        return None
    return value.lower()


def _hash_file(f) -> tuple[str, int]:
    """Hash a file object block by block and rewind to byte 0."""
    h = hashlib.sha256()
    size = 0
    f.seek(0)
    while True:
        block = f.read(HASH_BLOCK)
        if not block:
            break
        h.update(block)
        size += len(block)
    f.seek(0)
    return h.hexdigest(), size


class FillFailed(Exception):
    """Raised inside a client stream when the origin fill failed."""


class Entry:
    """One generation of a cached object.

    state: filling -> complete          (registered, file renamed into place)
                 \-> closed             (purged mid-fill; fill finished, file
                                          kept only until readers drain)
                 \-> failed             (origin error; part file removed)
    """

    def __init__(self, key: str) -> None:
        self.key = key
        self.gen = uuid.uuid4().hex[:12]
        self.state = "filling"
        self.purged = False
        self.readers = 0
        self.written = 0          # bytes flushed to the part file so far
        self.size = -1            # expected total bytes (-1 = not known yet)
        self.error: str | None = None
        self.origin_status: int | None = None
        self.headers_known = False
        self.meta: dict = {}
        self.cond = asyncio.Condition()
        self.fill_task: asyncio.Task | None = None
        # Exact file holding this generation's bytes. Part names are
        # generation-specific, so readers stay on one file across a purge.
        self.data_path: Path = CACHE_DIR / f"{key}.part-{self.gen}"
        # True once this generation's bytes sit at ``final_path``.
        self.published = False

    @property
    def final_path(self) -> Path:
        return CACHE_DIR / self.key

    @property
    def part_path(self) -> Path:
        return CACHE_DIR / f"{self.key}.part-{self.gen}"

    @property
    def meta_path(self) -> Path:
        return CACHE_DIR / f"{self.key}.meta"

    def info(self) -> dict:
        return {
            "state": self.state,
            "purged": self.purged,
            "readers": self.readers,
            "written": self.written,
            "size": self.size,
            "gen": self.gen,
            "error": self.error,
        }


class Cache:
    def __init__(self) -> None:
        self.entries: dict[str, Entry] = {}
        self.lock = asyncio.Lock()
        self.session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------ #
    # registry                                                            #
    # ------------------------------------------------------------------ #

    def _entry_from_disk(self, key: str) -> Entry | None:
        """Adopt a complete on-disk package (e.g. after a restart)."""
        final = CACHE_DIR / key
        try:
            st = final.stat()
        except OSError:
            return None
        if not final.is_file():
            return None
        e = Entry(key)
        e.state = "complete"
        e.size = st.st_size
        e.written = st.st_size
        e.headers_known = True
        e.data_path = final
        e.published = True
        try:
            e.meta = json.loads(e.meta_path.read_text())
        except (OSError, ValueError):
            e.meta = {}
        e.meta.setdefault("content_type", "application/octet-stream")
        self.entries[key] = e
        return e

    async def attach(self, key: str) -> Entry:
        """Return the current generation for key, starting a fill if needed."""
        async with self.lock:
            e = self.entries.get(key)
            if e is None:
                e = self._entry_from_disk(key)
            if e is None:
                e = Entry(key)
                self.entries[key] = e
                e.fill_task = asyncio.create_task(self._fill(e))
            e.readers += 1
            return e

    async def detach(self, e: Entry) -> None:
        async with self.lock:
            self._detach_nowait_locked(e)

    def detach_nowait(self, e: Entry) -> None:
        """Release a reader without awaiting (safe on a cancelled scope).

        No await happens inside, and the loop is single-threaded, so this
        cannot interleave with ``attach``/``purge``.
        """
        self._detach_nowait_locked(e)

    def _detach_nowait_locked(self, e: Entry) -> None:
        e.readers -= 1
        self._maybe_reap_locked(e)

    async def purge(self, key: str) -> bool:
        """Unregister the current generation; files go away with last reader."""
        async with self.lock:
            e = self.entries.pop(key, None)
            if e is not None:
                e.purged = True
                # A complete generation lives at the shared ``final_path``. Any
                # later reader (e.g. an accepted verified download that has not
                # opened its descriptor yet) must not be handed bytes from the
                # replacement fill that may soon publish there, so move this
                # generation to a name only it owns.
                if e.published and e.state == "complete" and e.readers > 0:
                    retired = e.part_path.parent / f"{e.key}.retired-{e.gen}"
                    try:
                        os.replace(e.final_path, retired)
                        e.data_path = retired
                        e.published = False
                        # No replacement fill can own ``meta_path`` yet: this
                        # entry was still the registered one under the lock.
                        _unlink_quiet(e.meta_path)
                    except OSError:
                        pass  # fall back to the shared name; digest still guards
                self._maybe_reap_locked(e)
                return True
            # Not in memory (e.g. never requested since restart): clean disk.
            existed = False
            final = CACHE_DIR / key
            for p in (final, CACHE_DIR / f"{key}.meta"):
                if p.exists():
                    _unlink_quiet(p)
                    existed = True
            try:
                for p in final.parent.glob(final.name + ".part-*"):
                    _unlink_quiet(p)
                    existed = True
            except OSError:
                pass
            # ``.retired-*`` files are intentionally left alone: they belong
            # to purged generations that may still have accepted readers
            # (verified downloads included) and are reaped when those drain.
            return existed

    def _maybe_reap_locked(self, e: Entry) -> None:
        """Delete files once no readers remain and the fill is over."""
        if e.readers > 0 or e.state == "filling":
            return
        if e.state == "failed":
            _unlink_quiet(e.data_path)  # never keep a half package
        elif e.purged:
            if e.published:
                # The bytes sit at the shared ``final_path``. Purge with live
                # readers moves them to a generation-owned name first, so this
                # is normally only reached right here, under the same lock that
                # removed the entry. Guard anyway: never delete whatever a
                # newer generation may have published at the shared name.
                owner = self.entries.get(e.key)
                if owner is None or owner is e:
                    _unlink_quiet(e.final_path)
                    _unlink_quiet(e.meta_path)
                e.published = False
            else:
                _unlink_quiet(e.data_path)  # generation-unique part/retired file

    # ------------------------------------------------------------------ #
    # origin fill                                                         #
    # ------------------------------------------------------------------ #

    async def _fill(self, e: Entry) -> None:
        assert self.session is not None
        url = f"{ORIGIN_BASE_URL}/packages/{e.key}"
        try:
            async with self.session.get(url) as resp:
                e.origin_status = resp.status
                if resp.status != 200:
                    e.error = f"origin responded with status {resp.status}"
                    await self._finish(e, "failed")
                    return
                cl = resp.content_length
                e.size = cl if cl is not None else -1
                e.meta = {
                    "content_type": resp.headers.get("Content-Type")
                    or "application/octet-stream",
                    "sha256": resp.headers.get("X-Package-Sha256"),
                    "fetched_at": time.time(),
                }
                e.part_path.parent.mkdir(parents=True, exist_ok=True)
                f = open(e.data_path, "wb")
                try:
                    # Part file exists before readers are allowed to open it.
                    e.headers_known = True
                    async with e.cond:
                        e.cond.notify_all()
                    written = 0
                    async for chunk in resp.content.iter_chunked(CHUNK):
                        await asyncio.to_thread(f.write, chunk)
                        await asyncio.to_thread(f.flush)
                        written += len(chunk)
                        e.written = written
                        async with e.cond:
                            e.cond.notify_all()
                finally:
                    await asyncio.to_thread(f.close)
                if e.size != -1 and written != e.size:
                    raise aiohttp.ClientPayloadError(
                        f"short read from origin: {written}/{e.size} bytes"
                    )
            async with self.lock:
                e.size = written if e.size == -1 else e.size
                e.meta["size"] = written
                if e.purged:
                    # Purged mid-fill: never register this generation; keep its
                    # part file only for readers already attached to it.
                    e.state = "closed"
                else:
                    try:
                        # Publishing moves the verified file under the shared
                        # name; the pinned descriptor and readers keep it alive.
                        os.replace(e.part_path, e.final_path)  # atomic publish
                        e.data_path = e.final_path
                        e.published = True
                    except OSError:
                        pass  # served from the part file; lookups will retry
                    try:
                        e.meta_path.write_text(json.dumps(e.meta))
                    except OSError:
                        pass
                    e.state = "complete"
                async with e.cond:
                    e.cond.notify_all()
                self._maybe_reap_locked(e)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - any origin/IO error fails the fill
            e.error = f"{type(exc).__name__}: {exc}"
            await self._finish(e, "failed")

    async def _finish(self, e: Entry, state: str) -> None:
        async with self.lock:
            e.state = state
            if state == "failed" and self.entries.get(e.key) is e:
                del self.entries[e.key]  # next GET retries the origin
            async with e.cond:
                e.cond.notify_all()
            self._maybe_reap_locked(e)

    # ------------------------------------------------------------------ #
    # client streaming                                                    #
    # ------------------------------------------------------------------ #

    async def open_pinned(self, e: Entry) -> PinnedFile:
        """Open this generation's bytes and pin them with an fd.

        Done under the registry lock so a concurrent purge rename (which also
        takes that lock) cannot move the file between choosing and opening it.
        The caller must already hold a reader reference, which keeps the file
        alive until the descriptor is closed. ``data_path`` is never shared
        with another generation, so bytes cannot be swapped by a refill.
        """
        async with self.lock:
            return PinnedFile(open(e.data_path, "rb"))

    async def stream(self, e: Entry):
        """Yield the package from byte 0, tailing the fill file if needed."""
        offset = 0
        f = await self.open_pinned(e)
        try:
            while True:
                async with e.cond:
                    await e.cond.wait_for(
                        lambda: e.written > offset or e.state != "filling"
                    )
                if e.state == "failed":
                    raise FillFailed(e.error or "origin fill failed")
                if e.state == "filling":
                    limit = CHUNK
                else:  # complete | closed: drain exactly to the final size
                    limit = min(CHUNK, e.size - offset)
                    if limit <= 0:
                        break
                data = await asyncio.to_thread(f.run, _read_some, limit)
                if not data:
                    if e.state == "filling":
                        continue  # defensive; written>offset implies data
                    break
                offset += len(data)
                yield data
        finally:
            try:
                f.close()
            finally:
                # Sync release: an ``await`` here can be cancelled again on
                # client disconnect and would leak the reader hold.
                self.detach_nowait(e)

    async def stream_pinned(self, e: Entry, f, size: int):
        """Yield exactly ``size`` bytes from an open, pinned file.

        The descriptor keeps the bytes alive even if the entry is purged and a
        newer generation is published mid-response. Ownership of ``f`` stays
        with the response object, which closes it on completion/disconnect.
        ``size`` is the length that was just hashed, so the bytes and the
        advertised Content-Length cannot disagree.
        """
        offset = 0
        while offset < size:
            data = await asyncio.to_thread(f.run, _read_some, min(CHUNK, size - offset))
            if not data:
                break  # truncated pinned file; short response
            offset += len(data)
            yield data


class PinnedFileResponse(StreamingResponse):
    """StreamingResponse that owns a file descriptor and a reader hold.

    Finishing, client disconnect, and cancellation all funnel through the
    ``finally`` below, so the descriptor is closed and the cache generation's
    reader count is released exactly once. Release is synchronous: awaiting in
    a cancelled scope could be cancelled again before the hold is dropped.
    """

    def __init__(self, content, f, release, **kwargs) -> None:
        super().__init__(content, **kwargs)
        self._file = f
        self._release = release

    async def __call__(self, scope, receive, send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            try:
                self._file.close()
            finally:
                self._release()


cache = Cache()


def cleanup_cache_dir() -> None:
    """On boot: drop partial fills and orphan meta, keep complete packages."""
    for p in CACHE_DIR.rglob("*"):
        if not p.is_file():
            continue
        name = p.name
        if ".part-" in name or ".retired-" in name:
            _unlink_quiet(p)
        elif name.endswith(".meta"):
            final = p.with_name(name[: -len(".meta")])
            if not final.exists():
                _unlink_quiet(p)


@asynccontextmanager
async def lifespan(app: FastAPI):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cleanup_cache_dir()
    timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_connect=10, sock_read=300)
    cache.session = aiohttp.ClientSession(timeout=timeout)
    try:
        yield
    finally:
        await cache.session.close()


app = FastAPI(title="pkg-cache-proxy", version="1.0.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/packages/{path:path}")
async def get_package(path: str):
    """Download a complete package by path (streamed from byte 0)."""
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)

    entry = await cache.attach(key)
    hit = entry.state == "complete"
    try:
        if not entry.headers_known:
            async with entry.cond:
                await entry.cond.wait_for(
                    lambda: entry.headers_known or entry.state == "failed"
                )
    except asyncio.CancelledError:
        cache.detach_nowait(entry)
        raise

    if entry.state == "failed":
        await cache.detach(entry)
        status = (
            entry.origin_status
            if entry.origin_status and entry.origin_status != 200
            else 502
        )
        return PlainTextResponse(f"origin fetch failed: {entry.error}", status_code=status)

    headers = {"X-Cache": "HIT" if hit else "MISS"}
    if entry.size >= 0:
        headers["Content-Length"] = str(entry.size)
    sha256 = entry.meta.get("sha256")
    if sha256:
        headers["X-Package-Sha256"] = sha256
    return StreamingResponse(
        cache.stream(entry),
        media_type=entry.meta.get("content_type", "application/octet-stream"),
        headers=headers,
    )


@app.get(
    "/verified/{path:path}",
    summary="Download a package only after verifying its SHA-256",
    response_description=(
        "The package bytes, after the whole file matched the expected digest"
    ),
    responses={
        200: {"description": "Verified; package bytes stream from byte 0"},
        400: {"description": "Bad path or missing/invalid X-Expected-Sha256 header"},
        404: {"description": "Origin does not have this package"},
        412: {
            "description": "Whole-file SHA-256 did not match; no package bytes are sent"
        },
        502: {"description": "Origin fetch failed"},
    },
)
async def get_verified_package(
    path: str,
    x_expected_sha256: str | None = Header(
        default=None,
        description=(
            "Expected SHA-256 of the complete package: 64 hex characters, "
            "optionally prefixed with 'sha256:'. Compare against the SHA-256 "
            "in your lock file."
        ),
        examples=[
            "sha256:0123456789abcdef0123456789abcdef0123456789abcdef"
            "0123456789abcdef"
        ],
    ),
):
    """Download a package only if its bytes match the caller-supplied SHA-256.

    Shares the plain endpoint's single-flight generation. The complete file is
    re-hashed block by block (the origin's header is not trusted), and nothing
    is streamed unless the whole digest matches. The verified file descriptor
    stays pinned for the response, so a concurrent purge + refill cannot swap
    the payload. 412 on mismatch, before any package bytes are sent.
    """
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)

    expected = parse_expected_sha256(x_expected_sha256)
    if expected is None:
        return PlainTextResponse(
            "missing or invalid X-Expected-Sha256 header "
            "(expected 64 hex characters, optional 'sha256:' prefix)",
            status_code=400,
        )

    entry = await cache.attach(key)
    handed_off = False
    try:
        hit = entry.state == "complete"

        # Wait until this generation's bytes are final (complete or closed).
        async with entry.cond:
            await entry.cond.wait_for(lambda: entry.state != "filling")

        if entry.state == "failed":
            status = (
                entry.origin_status
                if entry.origin_status and entry.origin_status != 200
                else 502
            )
            return PlainTextResponse(
                f"origin fetch failed: {entry.error}", status_code=status
            )

        # Readers hold the generation, and purge moves it to a generation-owned
        # name before any replacement can appear. Pin one descriptor and use
        # that same fd both to hash and to send, so a purge/refill cannot swap
        # bytes between verification and delivery.
        try:
            f = await cache.open_pinned(entry)
        except OSError as exc:
            return PlainTextResponse(f"package read failed: {exc}", status_code=502)

        hash_task = asyncio.create_task(asyncio.to_thread(f.run, _hash_file))
        try:
            # Shield the worker: on client disconnect this await is cancelled
            # but the thread keeps reading. Closing the fd or dropping the
            # reader hold while the worker runs could let the generation be
            # reaped (and the descriptor number recycled) under the read, so
            # both are released only once the worker finishes.
            actual, size = await asyncio.shield(hash_task)
        except asyncio.CancelledError:
            handed_off = True  # release happens in the callback below
            hash_task.add_done_callback(
                lambda _t: (f.close(), cache.detach_nowait(entry))
            )
            raise
        except BaseException:
            f.close()  # worker finished (with an error); nothing reading
            raise

        if actual != expected:
            f.close()
            return PlainTextResponse(
                f"checksum mismatch: expected {expected}, received {actual}",
                status_code=412,
            )

        headers = {
            "X-Cache": "HIT" if hit else "MISS",
            "X-Package-Sha256": actual,
            "X-Checksum-Verified": "sha256",
            "Content-Length": str(size),
        }
        handed_off = True
        return PinnedFileResponse(
            cache.stream_pinned(entry, f, size),
            f,
            lambda: cache.detach_nowait(entry),
            media_type=entry.meta.get("content_type", "application/octet-stream"),
            headers=headers,
        )
    finally:
        if not handed_off:
            # Covers failures during wait/hash plus cancellation before the
            # response object takes ownership of the reader hold.
            cache.detach_nowait(entry)


@app.post(
    "/purge/{path:path}",
    summary="Purge one cached path",
    response_description="Whether the path had a cached generation",
)
async def purge_package(path: str):
    """Purge one path. The next download re-fills from the origin; downloads
    already in progress keep reading their old generation to completion."""
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)
    was_cached = await cache.purge(key)
    return {"path": key, "purged": True, "was_cached": was_cached}


@app.get("/cache/status")
async def cache_status():
    async with cache.lock:
        return {k: e.info() for k, e in cache.entries.items()}
