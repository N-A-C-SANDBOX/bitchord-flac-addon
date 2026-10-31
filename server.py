#!/usr/bin/env python3
"""
Layz Add On – Lossless (FLAC) add-on for BitChord (Powered by Mornye-Python)

* Zero-key direct search (Deezer Public API + Songlink/Odesli Spotify ID resolution
  + Spotify Embed scraper) — does NOT require spotipy or utilities.sflx!
* Automatically bootstraps Mornye extensions (spotify-web, qobuz-web, tidal-web,
  deezer, amazon, utilities) from the configured MORNYE_REGISTRIES on startup.
* Supports both Spotify Track IDs (sp:<22-char-id>) and Deezer Track IDs (dz:<id>)
  resolved on-the-fly to Spotify URLs when downloading.
"""
import asyncio
import concurrent.futures
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
    "version": "3.1.0",
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
DEEZER_PREFIX = "dz:"
SPOTIFY_ID_RE = re.compile(r"^[A-Za-z0-9]{22}$")
DEEZER_ID_RE = re.compile(r"^\d{4,15}$")
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
    "https://raw.githubusercontent.com/moto-costiera/PY-Audio/main/registry.json"
)


def _int_env(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


CACHE_DIR = os.getenv("CACHE_DIR") or os.path.join(
    tempfile.gettempdir(), "layz_mornye"
)
MORNYE_DATA_DIR = os.getenv("MORNYE_DATA_DIR") or os.path.join(
    CACHE_DIR, ".mornye_data"
)
CACHE_TTL = _int_env("CACHE_TTL", 3600)
CACHE_MAX_BYTES = _int_env("CACHE_MAX_MB", 2048) * 1024 * 1024
CACHE_SEARCH_TTL = _int_env("CACHE_SEARCH_TTL", 600)
MAX_DOWNLOADS = max(_int_env("MAX_DOWNLOADS", 2), 1)
DOWNLOAD_TIMEOUT = _int_env("DOWNLOAD_TIMEOUT", 150)
FAIL_TTL = _int_env("FAIL_TTL", 300)
DEBUG_ENDPOINTS = os.getenv("DEBUG_ENDPOINTS", "1") == "1"

_raw_registries = os.getenv("MORNYE_REGISTRIES", DEFAULT_REGISTRY)
MORNYE_REGISTRIES = [r.strip() for r in _raw_registries.split(",") if r.strip()]

_raw_services = os.getenv(
    "MORNYE_SERVICES",
    os.getenv(
        "SPOTIFY_SERVICES",
        "ext:qobuz-py,ext:tidal-py,ext:deezer-py,ext:amazon-py",
    ),
)
MORNYE_SERVICES = [s.strip() for s in _raw_services.split(",") if s.strip()]

os.environ["MORNYE_REGISTRIES"] = ",".join(MORNYE_REGISTRIES)
os.environ["MORNYE_DATA_DIR"] = MORNYE_DATA_DIR
os.environ["MORNYE_PROGRESS_BARS"] = "0"

os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(MORNYE_DATA_DIR, exist_ok=True)


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
dz_to_sp_cache = TTLCache(maxsize=2000)

_locks_guard = threading.Lock()
_track_locks = {}
_download_slots = threading.BoundedSemaphore(MAX_DOWNLOADS)


def _lock_for(track_id):
    with _locks_guard:
        return _track_locks.setdefault(track_id, threading.Lock())


# --------------------------------------------------------------------------- #
# HTTP Helper
# --------------------------------------------------------------------------- #
def _http_get(url, headers=None, timeout=10, as_json=True):
    req_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/html, */*",
    }
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, headers=req_headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        return json.loads(raw) if as_json else raw


# --------------------------------------------------------------------------- #
# Auto-Bootstrap Mornye Extensions (Fixes missing .sflx errors)
# --------------------------------------------------------------------------- #
def bootstrap_mornye_extensions():
    """
    Ensure required extensions (utilities + Python native download providers)
    are installed in Mornye's extension directory from the configured registries.
    """
    try:
        from mornye.extensions import ExtensionManager

        em = ExtensionManager()
        installed = set()
        for method in ("list_installed", "installed_ids"):
            fn = getattr(em, method, None)
            if not callable(fn):
                continue
            try:
                res = fn()
            except TypeError:
                # Some versions require an argument; skip and try next method
                continue
            if isinstance(res, dict):
                installed.update(k for k in res.keys() if k)
            elif isinstance(res, (list, tuple, set)):
                for item in res:
                    if isinstance(item, str) and item:
                        installed.add(item)
                    elif isinstance(item, dict):
                        installed.add(item.get("id") or item.get("name") or "")
                    else:
                        installed.add(
                            getattr(item, "id", None)
                            or getattr(item, "name", None)
                            or ""
                        )
            installed.discard("")

        # Prefer the Python-native providers from the PY-Audio registry
        wanted = [
            "utilities",
            "qobuz-py",
            "tidal-py",
            "deezer-py",
            "amazon-py",
            "soundcloud-py",
            "youtube-py",
        ]
        for reg_url in MORNYE_REGISTRIES:
            try:
                reg_data = _http_get(reg_url, timeout=12, as_json=True)
                ext_list = (
                    reg_data.get("extensions", reg_data)
                    if isinstance(reg_data, dict)
                    else reg_data
                )
                available_ids = {
                    (e.get("id") or e.get("name")): e
                    for e in (ext_list or [])
                    if isinstance(e, dict)
                }
            except Exception as exc:
                log.warning("Could not fetch registry %s: %r", reg_url, exc)
                available_ids = {}

            for ext_id in wanted:
                if ext_id in installed:
                    continue
                if available_ids and ext_id not in available_ids:
                    continue
                try:
                    log.info("Installing Mornye extension %r from %s...", ext_id, reg_url)
                    em.install(ext_id, registry_url=reg_url)
                    installed.add(ext_id)
                except Exception as exc:
                    log.debug("Extension %r install skipped: %r", ext_id, exc)
    except Exception as exc:
        log.warning("Mornye extension bootstrap warning: %r", exc)


# Run extension bootstrap in background thread so gunicorn starts immediately
threading.Thread(target=bootstrap_mornye_extensions, daemon=True).start()


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


def extract_track_ref(text, allow_bare=False):
    """
    Returns ('sp', '<22-char-spotify-id>') or ('dz', '<deezer-id>') or (None, None).
    Accepts: Spotify URL/URI, sp:<id>, dz:<id>, dz-<id> (cache key form), bare IDs.
    """
    text = (text or "").strip()
    for rx in (SPOTIFY_URL_RE, SPOTIFY_URI_RE):
        m = rx.search(text)
        if m:
            return "sp", m.group(1)
    lower = text.lower()
    if lower.startswith(SPOTIFY_PREFIX):
        candidate = text[len(SPOTIFY_PREFIX) :]
        if SPOTIFY_ID_RE.match(candidate):
            return "sp", candidate
        # Nested forms: sp:dz:123 or sp:dz-123
        if candidate.lower().startswith(DEEZER_PREFIX):
            dz_cand = candidate[len(DEEZER_PREFIX) :]
            if DEEZER_ID_RE.match(dz_cand):
                return "dz", dz_cand
        if candidate.lower().startswith("dz-"):
            dz_cand = candidate[3:]
            if DEEZER_ID_RE.match(dz_cand):
                return "dz", dz_cand
    if lower.startswith(DEEZER_PREFIX):
        candidate = text[len(DEEZER_PREFIX) :]
        if DEEZER_ID_RE.match(candidate):
            return "dz", candidate
    # Cache-key form produced by resolve_download_target when Spotify mapping fails
    if lower.startswith("dz-"):
        candidate = text[3:]
        if DEEZER_ID_RE.match(candidate):
            return "dz", candidate
    if allow_bare:
        if SPOTIFY_ID_RE.match(text):
            return "sp", text
        if DEEZER_ID_RE.match(text):
            return "dz", text
    return None, None


def make_track(track_ref, title, artist, album, duration, art, base):
    """
    track_ref can be either a 22-char Spotify ID or 'dz:<deezer_id>'.
    """
    clean_ref = track_ref[len(SPOTIFY_PREFIX) :] if track_ref.startswith(SPOTIFY_PREFIX) else track_ref
    return {
        "id": f"{SPOTIFY_PREFIX}{clean_ref}",
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
        "streamURL": f"{base}/file/{clean_ref}.flac",
    }


# --------------------------------------------------------------------------- #
# Direct Search & ID Resolution (No Spotipy, No utilities.sflx needed!)
# --------------------------------------------------------------------------- #
def resolve_deezer_to_spotify_id(dz_id):
    """
    Convert a Deezer track ID into a 22-character Spotify Track ID using:
    1) Songlink / Odesli API
    2) Deezer ISRC -> MusicBrainz -> Spotify relation
    3) Title/artist web search on open.spotify.com
    Returns None when no mapping is found (caller may fall back to Deezer URL).
    """
    dz_id = str(dz_id).strip()
    cached = dz_to_sp_cache.get(dz_id)
    if cached:
        return cached

    # Method 1: Odesli / Songlink API
    try:
        dz_url = urllib.parse.quote(f"https://www.deezer.com/track/{dz_id}", safe="")
        data = _http_get(
            f"https://api.song.link/v1-alpha.1/links?url={dz_url}", timeout=8
        )
        sp_info = (data.get("linksByPlatform") or {}).get("spotify") or {}
        sp_url = sp_info.get("url") or ""
        _, sp_id = extract_track_ref(sp_url)
        if sp_id:
            dz_to_sp_cache.set(dz_id, sp_id, 24 * 3600)
            return sp_id
    except Exception as exc:
        log.debug("Songlink lookup for dz:%s failed: %r", dz_id, exc)

    dz_meta = None
    # Method 2: Deezer metadata + ISRC via MusicBrainz
    try:
        dz_meta = _http_get(f"https://api.deezer.com/track/{dz_id}", timeout=6)
        isrc = (dz_meta.get("isrc") or "").strip()
        if isrc:
            mb = _http_get(
                f"https://musicbrainz.org/ws/2/recording?query=isrc:{urllib.parse.quote(isrc)}&fmt=json",
                headers={"User-Agent": "LayzAddon/3.1 (https://github.com/layz)"},
                timeout=10,
            )
            recs = mb.get("recordings") or []
            if recs:
                rid = recs[0].get("id")
                if rid:
                    det = _http_get(
                        f"https://musicbrainz.org/ws/2/recording/{rid}?inc=url-rels&fmt=json",
                        headers={"User-Agent": "LayzAddon/3.1 (https://github.com/layz)"},
                        timeout=10,
                    )
                    for rel in det.get("relations") or []:
                        url = ((rel.get("url") or {}).get("resource") or "")
                        _, sp_id = extract_track_ref(url)
                        if sp_id:
                            dz_to_sp_cache.set(dz_id, sp_id, 24 * 3600)
                            return sp_id
    except Exception as exc:
        log.debug("ISRC/MusicBrainz lookup for dz:%s failed: %r", dz_id, exc)

    # Method 3: title/artist DuckDuckGo scrape
    try:
        if dz_meta is None:
            dz_meta = _http_get(f"https://api.deezer.com/track/{dz_id}", timeout=6)
        title = dz_meta.get("title") or ""
        artist = (dz_meta.get("artist") or {}).get("name") or ""
        if title and artist:
            q = urllib.parse.quote(f'site:open.spotify.com/track "{title}" "{artist}"')
            html = _http_get(
                f"https://html.duckduckgo.com/html/?q={q}", timeout=8, as_json=False
            )
            m = SPOTIFY_URL_RE.search(html)
            if m:
                sp_id = m.group(1)
                dz_to_sp_cache.set(dz_id, sp_id, 24 * 3600)
                return sp_id
    except Exception as exc:
        log.debug("Web lookup for dz:%s failed: %r", dz_id, exc)

    return None


def spotify_metadata(track_id):
    """Fetch (title, artist, album, duration_sec, artwork_url) without utilities.sflx."""
    cached = meta_cache.get(track_id)
    if cached:
        return cached

    meta = None

    # 1) Spotify Embed page JSON (__NEXT_DATA__) — works with zero keys & zero extensions!
    try:
        html = _http_get(
            f"https://open.spotify.com/embed/track/{track_id}",
            timeout=8,
            as_json=False,
        )
        m = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">([^<]+)</script>',
            html,
        )
        if m:
            next_data = json.loads(m.group(1))
            entity = (
                ((next_data.get("props") or {}).get("pageProps") or {})
                .get("state", {})
                .get("data", {})
                .get("entity")
            ) or {}
            if entity.get("name") or entity.get("title"):
                title = entity.get("name") or entity.get("title")
                artists = ", ".join(
                    a.get("name", "")
                    for a in (entity.get("artists") or [])
                    if a.get("name")
                )
                images = (entity.get("visualIdentity") or {}).get("image") or []
                art = images[0].get("url") if images else ""
                dur = int((entity.get("duration") or 0) / 1000)
                meta = (title, artists or "Spotify", "", dur, art)
    except Exception as exc:
        log.debug("Spotify embed scrape(%s) failed: %r", track_id, exc)

    # 2) Spotify oEmbed API (always public & keyless)
    if meta is None:
        try:
            oembed = _http_get(
                f"https://open.spotify.com/oembed?url=https://open.spotify.com/track/{track_id}",
                timeout=8,
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


def direct_search(query, limit):
    """
    Search tracks directly without spotipy or utilities.sflx:
    - If query is a Spotify URL / URI / ID, return its metadata immediately.
    - Otherwise query the public Deezer Search API (instant, rich metadata, high-res covers).
      Each result is returned with `dz:<deezer_id>` (or resolved Spotify ID if cached),
      and `/stream` or `/file` resolves `dz:<deezer_id>` to Spotify/lossless on demand!
    """
    base = base_url()

    # 1) Pasted Spotify Link / URI / sp:<id>
    kind, ref_id = extract_track_ref(query)
    if kind == "sp":
        meta = spotify_metadata(ref_id)
        title, artist, album, dur, art = meta or (ref_id, "Spotify", "", None, "")
        return [make_track(ref_id, title, artist, album, dur, art, base)]

    # 2) Direct text search via Deezer Public API (cached)
    cache_key = (query.lower(), limit)
    cached = search_cache.get(cache_key)
    if cached is not None:
        log.info("search cache hit: %r", query)
        return cached

    try:
        params = urllib.parse.urlencode({"q": query, "limit": limit})
        dz_data = _http_get(f"https://api.deezer.com/search?{params}", timeout=10)
        dz_items = dz_data.get("data") or []
    except Exception as exc:
        log.error("Deezer direct search(%r) failed: %r", query, exc)
        raise SearchError(f"Direct search failed: {exc}") from exc

    tracks = []
    for item in dz_items:
        dz_id = item.get("id")
        if not dz_id:
            continue
        sp_id = dz_to_sp_cache.get(str(dz_id))
        track_ref = sp_id if sp_id else f"{DEEZER_PREFIX}{dz_id}"
        album_obj = item.get("album") or {}
        artist_obj = item.get("artist") or {}
        art = (
            album_obj.get("cover_xl")
            or album_obj.get("cover_big")
            or album_obj.get("cover_medium")
            or ""
        )
        tracks.append(
            make_track(
                track_ref=track_ref,
                title=item.get("title") or str(dz_id),
                artist=artist_obj.get("name") or "Unknown Artist",
                album=album_obj.get("title") or "",
                duration=int(item.get("duration") or 0),
                art=art,
                base=base,
            )
        )

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
            if name.startswith(".") and name != ".mornye_data" and os.path.isdir(folder):
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


async def _mornye_download(source_url, out_dir):
    from mornye import AsyncMornye

    os.environ["MORNYE_PROGRESS_BARS"] = "0"
    # Note: current AsyncMornye does not accept use_extensions_fallback
    # (docs mention it, but the client constructor does not). Fallback
    # across the listed services is still the default behaviour.
    async with AsyncMornye(
        output_dir=out_dir,
        services=MORNYE_SERVICES,
        registries=MORNYE_REGISTRIES,
        quality="LOSSLESS",
        allow_fallback=True,
    ) as client:
        await client.download_track(source_url)


def resolve_download_target(identifier):
    """
    Accepts sp:<id>, dz:<id>, or bare ID.

    Returns (cache_key, source_url) where:
      - cache_key is used for on-disk cache folders (22-char Spotify ID or dz-<id>)
      - source_url is what Mornye will download (Spotify preferred, Deezer fallback)
    """
    kind, ref_id = extract_track_ref(identifier, allow_bare=True)
    if kind == "sp":
        return ref_id, f"https://open.spotify.com/track/{ref_id}"
    if kind == "dz":
        sp_id = resolve_deezer_to_spotify_id(ref_id)
        if sp_id:
            return sp_id, f"https://open.spotify.com/track/{sp_id}"
        # No Spotify mapping — download via Deezer URL (deezer-py / other providers
        # match by metadata). Cache under a stable dz- key.
        cache_key = f"dz-{ref_id}"
        return cache_key, f"https://www.deezer.com/track/{ref_id}"
    raise RuntimeError("Invalid track identifier")


def ensure_spotify_file(identifier, retry=False):
    """Return (cache_key, cached_audio_path), downloading if needed."""
    cache_key, source_url = resolve_download_target(identifier)

    final_dir = os.path.join(CACHE_DIR, cache_key)

    with _lock_for(cache_key):
        existing = _find_audio(final_dir) if os.path.isdir(final_dir) else None
        if _valid(existing):
            os.utime(final_dir, None)
            return cache_key, existing

        if retry:
            failure_cache.delete(cache_key)
        else:
            previous = failure_cache.get(cache_key)
            if previous:
                raise RuntimeError(f"{previous} (cached failure, add ?retry=1 to retry)")

        tmp_dir = os.path.join(
            CACHE_DIR, f".{cache_key}.part-{os.getpid()}-{int(time.time())}"
        )
        os.makedirs(tmp_dir, exist_ok=True)
        started = time.time()
        log.info("download start: %s (%s)", cache_key, source_url)

        try:
            with _download_slots:
                asyncio.run(
                    asyncio.wait_for(
                        _mornye_download(source_url, tmp_dir), DOWNLOAD_TIMEOUT
                    )
                )
        except asyncio.TimeoutError:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            msg = f"download timed out after {DOWNLOAD_TIMEOUT}s"
            failure_cache.set(cache_key, msg, min(FAIL_TTL, 60))
            log.error("download %s: %s", cache_key, msg)
            raise RuntimeError(msg)
        except Exception as exc:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            msg = f"Mornye failed: {exc}"
            failure_cache.set(cache_key, msg, FAIL_TTL)
            log.exception("download %s failed", cache_key)
            raise RuntimeError(msg) from exc

        path = _find_audio(tmp_dir)
        if not _valid(path):
            shutil.rmtree(tmp_dir, ignore_errors=True)
            msg = "Lossless file not found on any installed Mornye extension/provider"
            failure_cache.set(cache_key, msg, FAIL_TTL)
            log.error("download %s: %s", cache_key, msg)
            raise RuntimeError(msg)

        shutil.rmtree(final_dir, ignore_errors=True)
        os.replace(tmp_dir, final_dir)
        os.utime(final_dir, None)

        final_path = _find_audio(final_dir)
        log.info(
            "download done: %s %s (%.1f MB, %.1fs)",
            cache_key,
            os.path.basename(final_path),
            os.path.getsize(final_path) / 1048576,
            time.time() - started,
        )

    evict(protect=cache_key)
    return cache_key, final_path


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

    try:
        track_id, path = ensure_spotify_file(
            identifier, retry=request.args.get("retry") == "1"
        )
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


@app.get("/file/<path:track_ref>")
def serve_spotify_file(track_ref):
    """Serve the temporarily stored audio (downloads it on first request)."""
    track_ref = re.sub(r"\.(flac|m4a|alac|wav|mp3)$", "", track_ref, flags=re.I)

    try:
        _, path = ensure_spotify_file(
            track_ref, retry=request.args.get("retry") == "1"
        )
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
        from mornye.extensions import ExtensionManager

        info["mornye_import"] = "ok"
        em = ExtensionManager()
        for m in ("list_installed", "installed_ids", "get_installed"):
            if hasattr(em, m):
                info["installed_extensions"] = str(getattr(em, m)())
                break
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
