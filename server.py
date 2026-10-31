#!/usr/bin/env python3
"""
Layz Add On – Lossless (FLAC) add-on for BitChord (Powered by Mornye-Python)

* Search directly by song name (keyless Spotify / Mornye / Deezer fallback) or by
  pasted Spotify link / URI / sp:<id> — no spotipy or Spotify API keys required!
* Audio is fetched with Mornye (AsyncMornye) into a temporary server cache and
  streamed through /file/<track_id> (HTTP Range supported, so seeking works)

Environment variables
---------------------
PORT                   default 8080
PUBLIC_URL             public https base URL (e.g. https://addon.example.com)
MORNYE_REGISTRIES      extension registry JSON URL(s)
                       default: https://raw.githubusercontent.com/mornyeapp/Mornye-Extension/main/registry.json
MORNYE_SERVICES        default ext:deezer-web,ext:qobuz-web,ext:tidal-web,ext:amazon-web,deezer,qobuz,amazon,tidal
CACHE_DIR              default <tmp>/layz_mornye
CACHE_TTL              default 3600   (seconds an unused track stays on disk)
CACHE_MAX_MB           default 2048   (max total size of the audio cache)
CACHE_SEARCH_TTL       default 600
MAX_DOWNLOADS          default 2
DOWNLOAD_TIMEOUT       default 150    (keep below the gunicorn --timeout)
FAIL_TTL               default 300
DEBUG_ENDPOINTS        default 1      (set 0 to disable /debug/*)
"""
import asyncio
import json
import logging
import os
import re
import shutil
import tempfile
import threading
import time
import urllib.parse
import urllib.request

from flask import Flask, abort, g, jsonify, request, send_file

# --------------------------------------------------------------------------- #
# Logging
# --------------------------------------------------------------------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True,
)
log = logging.getLogger("layz")

app = Flask(__name__)

MANIFEST = {
    "id": "bitchord-internet-archive-flac",
    "name": "Layz Add On (Mornye)",
    "version": "3.0.0",
    "resources": ["search", "stream"],
    "settings": [
        {
            "key": "quality",
            "type": "select",
            "default": "lossless",
            "options": [
                {"label": "Lossless", "value": "lossless"},
                {"label": "High", "value": "high"},
                {"label": "Low", "value": "low"},
            ],
        }
    ],
}

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
SPOTIFY_PREFIX = "sp:"
SPOTIFY_ID_RE = re.compile(r"^[A-Za-z0-9]{22}$")
SPOTIFY_URL_RE = re.compile(
    r"open\.spotify\.com/(?:intl-[a-z]+/)?track/([A-Za-z0-9]{22})", re.I
)
SPOTIFY_URI_RE = re.compile(r"spotify:track:([A-Za-z0-9]{22})", re.I)
AUDIO_EXTS = (".flac", ".m4a", ".alac", ".wav", ".mp3")
MIME_TYPES = {
    ".flac": "audio/flac",
    ".m4a": "audio/mp4",
    ".alac": "audio/mp4",
    ".wav": "audio/wav",
    ".mp3": "audio/mpeg",
}

DEFAULT_REGISTRY = (
    "https://raw.githubusercontent.com/mornyeapp/Mornye-Extension/main/registry.json"
)


def _int_env(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


CACHE_DIR = os.getenv("CACHE_DIR") or os.path.join(
    tempfile.gettempdir(), "layz_mornye"
)
CACHE_TTL = _int_env("CACHE_TTL", 3600)
CACHE_MAX_BYTES = _int_env("CACHE_MAX_MB", 2048) * 1024 * 1024
CACHE_SEARCH_TTL = _int_env("CACHE_SEARCH_TTL", 600)
MAX_DOWNLOADS = max(_int_env("MAX_DOWNLOADS", 2), 1)
DOWNLOAD_TIMEOUT = _int_env("DOWNLOAD_TIMEOUT", 150)
FAIL_TTL = _int_env("FAIL_TTL", 300)
DEBUG_ENDPOINTS = os.getenv("DEBUG_ENDPOINTS", "1") == "1"

# Support both MORNYE_SERVICES and legacy SPOTIFY_SERVICES
_raw_services = os.getenv(
    "MORNYE_SERVICES",
    os.getenv(
        "SPOTIFY_SERVICES",
        "ext:deezer-web,ext:qobuz-web,ext:tidal-web,ext:amazon-web,deezer,qobuz,amazon,tidal",
    ),
)
MORNYE_SERVICES = [s.strip() for s in _raw_services.split(",") if s.strip()]

_raw_registries = os.getenv("MORNYE_REGISTRIES", DEFAULT_REGISTRY)
MORNYE_REGISTRIES = [r.strip() for r in _raw_registries.split(",") if r.strip()]

os.environ.setdefault("MORNYE_REGISTRIES", ",".join(MORNYE_REGISTRIES))
os.environ["MORNYE_PROGRESS_BARS"] = "0"
os.makedirs(CACHE_DIR, exist_ok=True)


# --------------------------------------------------------------------------- #
# Small in-memory TTL cache
# --------------------------------------------------------------------------- #
class TTLCache:
    def __init__(self, maxsize=500):
        self._data = {}
        self._lock = threading.Lock()
        self._maxsize = maxsize

    def get(self, key):
        with self._lock:
            item = self._data.get(key)
            if not item:
                return None
            expires, value = item
            if expires < time.time():
                self._data.pop(key, None)
                return None
            return value

    def set(self, key, value, ttl):
        with self._lock:
            if len(self._data) >= self._maxsize:
                now = time.time()
                for k in [k for k, (exp, _) in self._data.items() if exp < now]:
                    self._data.pop(k, None)
                if len(self._data) >= self._maxsize:
                    self._data.pop(next(iter(self._data)), None)
            self._data[key] = (time.time() + ttl, value)

    def delete(self, key):
        with self._lock:
            self._data.pop(key, None)


search_cache = TTLCache()
meta_cache = TTLCache()
failure_cache = TTLCache()
token_cache = TTLCache(maxsize=5)

_locks_guard = threading.Lock()
_track_locks = {}
_download_slots = threading.BoundedSemaphore(MAX_DOWNLOADS)


def _lock_for(track_id):
    with _locks_guard:
        return _track_locks.setdefault(track_id, threading.Lock())


# --------------------------------------------------------------------------- #
# Request logging
# --------------------------------------------------------------------------- #
@app.before_request
def _start_timer():
    g.t0 = time.time()


@app.after_request
def _log_request(resp):
    if request.path != "/health":
        log.info(
            "%s %s -> %s (%.0f ms)",
            request.method,
            request.full_path.rstrip("?"),
            resp.status_code,
            (time.time() - getattr(g, "t0", time.time())) * 1000,
        )
    return resp


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def parse_streaminfo(data):
    """Parse FLAC STREAMINFO from the first bytes of a FLAC file."""
    if len(data) < 26 or data[:4] != b"fLaC":
        return None, None
    block_type = data[4] & 0x7F
    block_length = int.from_bytes(data[5:8], "big")
    if block_type != 0 or block_length < 34:
        return None, None

    packed = int.from_bytes(data[18:26], "big")
    sample_rate = packed >> 44
    bit_depth = ((packed >> 36) & 0x1F) + 1
    if not 1000 <= sample_rate <= 768000:
        sample_rate = None
    if not 4 <= bit_depth <= 32:
        bit_depth = None
    return sample_rate, bit_depth


def local_flac_info(path):
    try:
        with open(path, "rb") as fh:
            return parse_streaminfo(fh.read(64))
    except OSError:
        return None, None


def quality_label(sr, bd):
    details = []
    if bd is not None:
        details.append(f"{bd}-bit")
    if sr is not None:
        details.append(f"{sr / 1000:g} kHz")
    hi_res = (bd is not None and bd > 16) or (sr is not None and sr > 48000)
    label = "Hi-Res Lossless" if hi_res else "Lossless"
    return label + (f" · {' / '.join(details)}" if details else "")


def base_url():
    configured = (os.getenv("PUBLIC_URL") or "").strip().rstrip("/")
    if configured:
        return configured
    proto = request.headers.get("X-Forwarded-Proto", request.scheme)
    host = request.headers.get("X-Forwarded-Host", request.host)
    return f"{proto}://{host}"


def extract_spotify_id(text, allow_bare=False):
    text = (text or "").strip()
    for rx in (SPOTIFY_URL_RE, SPOTIFY_URI_RE):
        m = rx.search(text)
        if m:
            return m.group(1)
    if text.lower().startswith(SPOTIFY_PREFIX):
        candidate = text[len(SPOTIFY_PREFIX) :]
        if SPOTIFY_ID_RE.match(candidate):
            return candidate
    if allow_bare and SPOTIFY_ID_RE.match(text):
        return text
    return None


def make_track(track_id, title, artist, album, duration, art, base):
    return {
        "id": f"{SPOTIFY_PREFIX}{track_id}",
        "title": title,
        "artist": artist,
        "album": album or "",
        "duration": duration,
        "artworkURL": art or "",
        "format": "flac",
        "audioQuality": "LOSSLESS",
        "quality": "Lossless",
        "sampleRate": None,
        "bitDepth": None,
        "bitrate": None,
        "streamURL": f"{base}/file/{track_id}.flac",
    }


def _http_get_json(url, headers=None, timeout=10):
    req_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, headers=req_headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


# --------------------------------------------------------------------------- #
# Direct Metadata & Search (No Spotipy Required)
# --------------------------------------------------------------------------- #
def _get_anon_spotify_token():
    """Fetch an anonymous Spotify web player token without Client ID/Secret."""
    cached = token_cache.get("anon_token")
    if cached:
        return cached
    try:
        data = _http_get_json(
            "https://open.spotify.com/get_access_token?reason=transport&productType=web_player"
        )
        token = data.get("accessToken")
        expires_ms = data.get("accessTokenExpirationTimestampMs", 0)
        if token:
            ttl = max(int((expires_ms / 1000) - time.time()) - 30, 60)
            token_cache.set("anon_token", token, ttl)
            return token
    except Exception as exc:
        log.debug("Anonymous Spotify token fetch failed: %r", exc)
    return None


def spotify_metadata(track_id):
    """Fetch (title, artist, album, duration_sec, artwork_url) via Mornye."""
    cached = meta_cache.get(track_id)
    if cached:
        return cached

    meta = None
    try:
        from mornye import AsyncMornye

        async def _meta():
            async with AsyncMornye(
                output_dir=CACHE_DIR, registries=MORNYE_REGISTRIES
            ) as client:
                return await client.get_track_metadata(
                    f"https://open.spotify.com/track/{track_id}"
                )

        m = asyncio.run(_meta())
        artists = (
            ", ".join(m.artists)
            if isinstance(getattr(m, "artists", None), (list, tuple))
            else str(getattr(m, "artists", ""))
        )
        meta = (
            getattr(m, "title", track_id),
            artists,
            getattr(m, "album", "") or "",
            int(getattr(m, "duration_seconds", 0) or 0),
            getattr(m, "cover_url", "") or "",
        )
    except Exception as exc:
        log.warning("Mornye metadata(%s) failed: %r", track_id, exc)

    # Fallback to Spotify oEmbed if Mornye metadata call fails
    if meta is None:
        try:
            oembed = _http_get_json(
                f"https://open.spotify.com/oembed?url=https://open.spotify.com/track/{track_id}"
            )
            meta = (
                oembed.get("title", track_id),
                oembed.get("author_name", "Spotify"),
                "",
                0,
                oembed.get("thumbnail_url", ""),
            )
        except Exception as exc:
            log.warning("Spotify oembed(%s) failed: %r", track_id, exc)

    if meta:
        meta_cache.set(track_id, meta, 24 * 3600)
    return meta


class SearchError(Exception):
    pass


async def _mornye_direct_search(query, limit, base):
    """
    Attempt search using Mornye's native search methods or its built-in
    csv_source catalogue resolver (no Spotify API keys needed).
    """
    from mornye import AsyncMornye

    async with AsyncMornye(
        output_dir=CACHE_DIR,
        services=MORNYE_SERVICES,
        registries=MORNYE_REGISTRIES,
    ) as client:
        for method_name in ("search", "search_tracks"):
            fn = getattr(client, method_name, None)
            if callable(fn):
                res = await fn(query, limit=limit)
                items = res.get("tracks", res) if isinstance(res, dict) else res
                tracks = []
                for item in items or []:
                    tid = extract_spotify_id(
                        str(
                            getattr(item, "url", None)
                            or getattr(item, "id", None)
                            or (item.get("id") if isinstance(item, dict) else "")
                        ),
                        allow_bare=True,
                    )
                    if not tid:
                        continue
                    title = getattr(item, "title", None) or (
                        item.get("title") or item.get("name")
                        if isinstance(item, dict)
                        else ""
                    )
                    artist = getattr(item, "artists", None) or (
                        item.get("artist") if isinstance(item, dict) else ""
                    )
                    if isinstance(artist, (list, tuple)):
                        artist = ", ".join(str(a) for a in artist)
                    album = getattr(item, "album", None) or (
                        item.get("album", "") if isinstance(item, dict) else ""
                    )
                    dur = int(
                        getattr(item, "duration_seconds", 0)
                        or (item.get("duration", 0) if isinstance(item, dict) else 0)
                        or 0
                    )
                    art = getattr(item, "cover_url", None) or (
                        item.get("cover_url", "") if isinstance(item, dict) else ""
                    )
                    tracks.append(
                        make_track(tid, title, str(artist), album, dur, art, base)
                    )
                if tracks:
                    return tracks[:limit]
    return None


def _anon_spotify_search(query, limit, base):
    """Search directly via anonymous Spotify web token (no spotipy required)."""
    token = _get_anon_spotify_token()
    if not token:
        return None
    params = urllib.parse.urlencode({"q": query, "type": "track", "limit": limit})
    data = _http_get_json(
        f"https://api.spotify.com/v1/search?{params}",
        headers={"Authorization": f"Bearer {token}"},
    )
    items = (data.get("tracks") or {}).get("items") or []
    tracks = []
    for item in items:
        if not item or not item.get("id"):
            continue
        images = (item.get("album") or {}).get("images") or []
        tracks.append(
            make_track(
                track_id=item["id"],
                title=item["name"],
                artist=", ".join(a["name"] for a in item.get("artists", [])),
                album=(item.get("album") or {}).get("name", ""),
                duration=int((item.get("duration_ms") or 0) / 1000),
                art=images[0]["url"] if images else "",
                base=base,
            )
        )
    return tracks


def _csv_catalogue_fallback_search(query, limit, base):
    """
    Fallback: Query public Deezer search API for candidate tracks and resolve
    their Spotify track IDs via Mornye's built-in csv_source catalogue matcher.
    """
    from mornye.core import csv_source

    params = urllib.parse.urlencode({"q": query, "limit": min(limit, 10)})
    dz_data = _http_get_json(f"https://api.deezer.com/search?{params}")
    dz_items = dz_data.get("data") or []
    if not dz_items:
        return []

    # Build an in-memory CSV with Title, Artist, Album, Duration to feed Mornye's matcher
    lines = ["Track Name,Artist Name(s),Album Name,Duration (ms)"]
    for item in dz_items:
        t = (item.get("title") or "").replace('"', '""')
        a = ((item.get("artist") or {}).get("name") or "").replace('"', '""')
        alb = ((item.get("album") or {}).get("title") or "").replace('"', '""')
        dur_ms = int(item.get("duration") or 0) * 1000
        lines.append(f'"{t}","{a}","{alb}","{dur_ms}"')

    with tempfile.NamedTemporaryFile(
        "w", suffix=".csv", delete=False, encoding="utf-8"
    ) as tmp:
        tmp.write("\n".join(lines))
        tmp_path = tmp.name

    try:
        doc = csv_source.read_rows(tmp_path)
        resolution = asyncio.run(csv_source.resolve_rows(doc.rows, document=doc))
        tracks = []
        for idx, url in enumerate(getattr(resolution, "urls", []) or []):
            tid = extract_spotify_id(url, allow_bare=True)
            if not tid:
                continue
            src = dz_items[idx] if idx < len(dz_items) else {}
            tracks.append(
                make_track(
                    track_id=tid,
                    title=src.get("title") or tid,
                    artist=(src.get("artist") or {}).get("name") or "",
                    album=(src.get("album") or {}).get("title") or "",
                    duration=int(src.get("duration") or 0),
                    art=(src.get("album") or {}).get("cover_xl") or "",
                    base=base,
                )
            )
        return tracks
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass


def direct_search(query, limit):
    base = base_url()

    # 1) Pasted Spotify Link / URI / sp:<id>
    track_id = extract_spotify_id(query)
    if track_id:
        meta = spotify_metadata(track_id)
        title, artist, album, dur, art = meta or (track_id, "Spotify", "", None, "")
        return [make_track(track_id, title, artist, album, dur, art, base)]

    # 2) Direct text search (cached)
    cache_key = (query.lower(), limit)
    cached = search_cache.get(cache_key)
    if cached is not None:
        log.info("search cache hit: %r", query)
        return cached

    tracks = None

    # Tier A: Mornye native search
    try:
        tracks = asyncio.run(_mornye_direct_search(query, limit, base))
    except Exception as exc:
        log.debug("Mornye native search skipped: %r", exc)

    # Tier B: Keyless Spotify Web API search
    if not tracks:
        try:
            tracks = _anon_spotify_search(query, limit, base)
        except Exception as exc:
            log.warning("Keyless Spotify search failed: %r", exc)

    # Tier C: Public Deezer + Mornye CSV resolver
    if not tracks:
        try:
            tracks = _csv_catalogue_fallback_search(query, limit, base)
        except Exception as exc:
            log.error("Fallback catalogue search failed: %r", exc)
            raise SearchError(f"Direct search failed: {exc}") from exc

    tracks = tracks or []
    search_cache.set(cache_key, tracks, CACHE_SEARCH_TTL)
    return tracks


# --------------------------------------------------------------------------- #
# Audio cache & Mornye Downloader
# --------------------------------------------------------------------------- #
def _find_audio(folder):
    best = None
    for root, _, files in os.walk(folder):
        for name in files:
            if name.lower().endswith(AUDIO_EXTS):
                path = os.path.join(root, name)
                rank = (name.lower().endswith(".flac"), os.path.getsize(path))
                if best is None or rank > best[0]:
                    best = (rank, path)
    return best[1] if best else None


def _valid(path):
    return bool(path) and os.path.isfile(path) and os.path.getsize(path) > 10000


def _dir_size(folder):
    total = 0
    for root, _, files in os.walk(folder):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return total


def cache_entries():
    """[(mtime, size, track_id)] of completed cache folders."""
    out = []
    try:
        names = os.listdir(CACHE_DIR)
    except OSError:
        return out
    for name in names:
        folder = os.path.join(CACHE_DIR, name)
        if name.startswith(".") or not os.path.isdir(folder):
            continue
        try:
            out.append((os.path.getmtime(folder), _dir_size(folder), name))
        except OSError:
            pass
    return out


def evict(protect=None):
    """Delete expired tracks, then oldest tracks until under CACHE_MAX_BYTES."""
    now = time.time()
    entries = sorted(cache_entries())
    kept = []
    for mtime, size, name in entries:
        if name == protect or _lock_for(name).locked():
            kept.append((mtime, size, name))
            continue
        if now - mtime > CACHE_TTL:
            shutil.rmtree(os.path.join(CACHE_DIR, name), ignore_errors=True)
            log.info("cache: expired %s", name)
        else:
            kept.append((mtime, size, name))

    total = sum(size for _, size, _ in kept)
    for mtime, size, name in kept:
        if total <= CACHE_MAX_BYTES:
            break
        if name == protect or _lock_for(name).locked():
            continue
        shutil.rmtree(os.path.join(CACHE_DIR, name), ignore_errors=True)
        total -= size
        log.info("cache: evicted %s (size limit)", name)


def purge_on_start():
    """Remove partial downloads / stale folders left by a previous run."""
    try:
        for name in os.listdir(CACHE_DIR):
            folder = os.path.join(CACHE_DIR, name)
            if name.startswith(".") and os.path.isdir(folder):
                shutil.rmtree(folder, ignore_errors=True)
        evict()
    except OSError:
        pass


def cleanup_loop():
    while True:
        time.sleep(300)
        try:
            evict()
        except Exception:
            log.exception("cache cleanup failed")


async def _mornye_download(track_id, out_dir):
    from mornye import AsyncMornye

    os.environ["MORNYE_PROGRESS_BARS"] = "0"
    async with AsyncMornye(
        output_dir=out_dir,
        services=MORNYE_SERVICES,
        registries=MORNYE_REGISTRIES,
        quality="LOSSLESS",
        allow_fallback=True,
        use_extensions_fallback=True,
    ) as client:
        await client.download_track(f"https://open.spotify.com/track/{track_id}")


def ensure_spotify_file(track_id, retry=False):
    """Return the cached audio path, downloading it if needed. Raises RuntimeError."""
    if not SPOTIFY_ID_RE.match(track_id):
        raise RuntimeError("invalid Spotify track id")

    final_dir = os.path.join(CACHE_DIR, track_id)

    with _lock_for(track_id):
        existing = _find_audio(final_dir) if os.path.isdir(final_dir) else None
        if _valid(existing):
            os.utime(final_dir, None)
            return existing

        if retry:
            failure_cache.delete(track_id)
        else:
            previous = failure_cache.get(track_id)
            if previous:
                raise RuntimeError(f"{previous} (cached failure, add ?retry=1 to retry)")

        tmp_dir = os.path.join(
            CACHE_DIR, f".{track_id}.part-{os.getpid()}-{int(time.time())}"
        )
        os.makedirs(tmp_dir, exist_ok=True)
        started = time.time()
        log.info("download start: %s", track_id)

        try:
            with _download_slots:
                asyncio.run(
                    asyncio.wait_for(
                        _mornye_download(track_id, tmp_dir), DOWNLOAD_TIMEOUT
                    )
                )
        except asyncio.TimeoutError:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            msg = f"download timed out after {DOWNLOAD_TIMEOUT}s"
            failure_cache.set(track_id, msg, min(FAIL_TTL, 60))
            log.error("download %s: %s", track_id, msg)
            raise RuntimeError(msg)
        except Exception as exc:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            msg = f"Mornye failed: {exc}"
            failure_cache.set(track_id, msg, FAIL_TTL)
            log.exception("download %s failed", track_id)
            raise RuntimeError(msg) from exc

        path = _find_audio(tmp_dir)
        if not _valid(path):
            shutil.rmtree(tmp_dir, ignore_errors=True)
            msg = "Lossless file not found on any installed Mornye extension/provider"
            failure_cache.set(track_id, msg, FAIL_TTL)
            log.error("download %s: %s", track_id, msg)
            raise RuntimeError(msg)

        shutil.rmtree(final_dir, ignore_errors=True)
        os.replace(tmp_dir, final_dir)
        os.utime(final_dir, None)

        final_path = _find_audio(final_dir)
        log.info(
            "download done: %s %s (%.1f MB, %.1fs)",
            track_id,
            os.path.basename(final_path),
            os.path.getsize(final_path) / 1048576,
            time.time() - started,
        )

    evict(protect=track_id)
    return final_path


purge_on_start()
threading.Thread(target=cleanup_loop, daemon=True).start()


# --------------------------------------------------------------------------- #
# Endpoint logic
# --------------------------------------------------------------------------- #
def do_search():
    query = (request.args.get("q") or "").strip()
    if not query:
        return jsonify({"tracks": []})

    try:
        limit = min(max(int(request.args.get("limit", "20")), 1), 30)
    except ValueError:
        limit = 20

    try:
        tracks = direct_search(query, limit)
    except SearchError as exc:
        return jsonify({"tracks": [], "error": str(exc)}), 502
    except Exception as exc:
        log.exception("search crashed")
        return jsonify({"tracks": [], "error": str(exc)}), 500

    return jsonify({"tracks": tracks[:limit]})


def do_stream(identifier):
    identifier = (identifier or "").strip()
    if not identifier:
        return jsonify({"error": "missing id"}), 400

    track_id = extract_spotify_id(identifier, allow_bare=True)
    if not track_id:
        return jsonify({"error": "not a Spotify track id"}), 400

    try:
        path = ensure_spotify_file(track_id, retry=request.args.get("retry") == "1")
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 404

    ext = os.path.splitext(path)[1].lower()
    is_flac = ext == ".flac"
    sr, bd = local_flac_info(path) if is_flac else (None, None)
    fmt = "flac" if is_flac else ext.lstrip(".")
    label = quality_label(sr, bd) if is_flac else f"Lossless ({fmt.upper()})"

    return jsonify(
        {
            "url": f"{base_url()}/file/{track_id}{ext}",
            "format": fmt,
            "quality": label,
            "audioQuality": "LOSSLESS",
            "codec": fmt,
            "container": fmt,
            "manifest": "none",
            "encrypted": False,
            "sampleRate": sr,
            "bitDepth": bd,
            "bitrate": None,
            "audioMode": "stereo",
        }
    )


# --------------------------------------------------------------------------- #
# Routes
# --------------------------------------------------------------------------- #
@app.get("/")
def root():
    return jsonify(MANIFEST)


@app.get("/manifest.json")
def manifest():
    return jsonify(MANIFEST)


@app.get("/health")
def health():
    return jsonify(
        {"ok": True, "name": MANIFEST["name"], "version": MANIFEST["version"]}
    )


@app.get("/search")
def search():
    return do_search()


@app.get("/stream/<path:identifier>")
def stream(identifier):
    return do_stream(identifier)


@app.get("/stream")
def stream_query():
    return do_stream(request.args.get("id"))


@app.get("/file/<track_id>")
def serve_spotify_file(track_id):
    """Serve the temporarily stored audio (downloads it on first request)."""
    track_id = track_id.split(".")[0]
    if not SPOTIFY_ID_RE.match(track_id):
        abort(404)

    try:
        path = ensure_spotify_file(track_id, retry=request.args.get("retry") == "1")
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 404

    try:
        os.utime(os.path.dirname(path), None)
    except OSError:
        pass

    ext = os.path.splitext(path)[1].lower()
    return send_file(
        path,
        mimetype=MIME_TYPES.get(ext, "application/octet-stream"),
        conditional=True,
        max_age=0,
    )


# --------------------------------------------------------------------------- #
# Debug endpoints
# --------------------------------------------------------------------------- #
@app.get("/debug/mornye")
@app.get("/debug/spotify")
def debug_mornye():
    if not DEBUG_ENDPOINTS:
        abort(404)

    info = {
        "public_url": os.getenv("PUBLIC_URL") or None,
        "registries": MORNYE_REGISTRIES,
        "services": MORNYE_SERVICES,
    }
    try:
        import mornye

        info["mornye_import"] = "ok"
    except Exception as exc:
        info["mornye_import"] = f"FAILED: {exc!r}"

    try:
        res = direct_search("blinding lights", 5)
        info["test_search"] = f"ok, {len(res)} results"
    except Exception as exc:
        info["test_search"] = f"FAILED: {exc!r}"
    return jsonify(info)


@app.get("/debug/cache")
def debug_cache():
    if not DEBUG_ENDPOINTS:
        abort(404)
    now = time.time()
    entries = cache_entries()
    return jsonify(
        {
            "dir": CACHE_DIR,
            "ttl_seconds": CACHE_TTL,
            "max_mb": CACHE_MAX_BYTES // (1024 * 1024),
            "total_mb": round(sum(s for _, s, _ in entries) / 1048576, 2),
            "tracks": [
                {
                    "id": name,
                    "size_mb": round(size / 1048576, 2),
                    "idle_seconds": int(now - mtime),
                }
                for mtime, size, name in sorted(entries, reverse=True)
            ],
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), threaded=True)
