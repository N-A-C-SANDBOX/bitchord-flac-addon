#!/usr/bin/env python3
"""
Layz Add On – Internet Archive FLAC + Spotify FLAC (via SpotiFLAC)

Spotify tracks are downloaded on the server into a temporary folder, served
through /file/<track_id> (with HTTP Range support for seeking) and removed
automatically after CACHE_TTL seconds.

Environment variables
---------------------
PORT                   Port to listen on                       (default 8080)
PUBLIC_URL             Public base URL of this server          (e.g. https://addon.example.com)
                       Falls back to the incoming request host if not set.
SPOTIFY_CLIENT_ID      Optional – enables Spotify text search through spotipy
SPOTIFY_CLIENT_SECRET  Optional – same as above
CACHE_DIR              Temp folder for downloads               (default <system tmp>/layz_spotify)
CACHE_TTL              Seconds to keep a downloaded file       (default 3600)
SPOTIFY_SERVICES       Comma list of SpotiFLAC providers       (default deezer,qobuz,amazon,tidal)

Install
-------
pip install flask requests SpotiFLAC spotipy
"""
import asyncio
import os
import re
import shutil
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

import requests
from flask import Flask, abort, jsonify, request, send_file

app = Flask(__name__)

MANIFEST = {
    "id": "bitchord-internet-archive-flac",
    "name": "Layz Add On",
    "version": "1.7.0",
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
        },
        {
            "key": "source",
            "type": "select",
            "default": "all",
            "options": [
                {"label": "All sources", "value": "all"},
                {"label": "Spotify (FLAC)", "value": "spotify"},
                {"label": "Internet Archive", "value": "archive"},
            ],
        },
    ],
}

IA_SEARCH = "https://archive.org/advancedsearch.php"
IA_METADATA = "https://archive.org/metadata/{}"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Layz-Add-On/1.7 BitChord"})

# --------------------------------------------------------------------------- #
# Spotify / temporary storage configuration
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

CACHE_DIR = os.getenv("CACHE_DIR") or os.path.join(tempfile.gettempdir(), "layz_spotify")
CACHE_TTL = int(os.getenv("CACHE_TTL", "3600"))
SPOTIFY_SERVICES = [
    s.strip()
    for s in os.getenv("SPOTIFY_SERVICES", "deezer,qobuz,amazon,tidal").split(",")
    if s.strip()
]
os.makedirs(CACHE_DIR, exist_ok=True)

_locks_guard = threading.Lock()
_track_locks = {}


def _lock_for(track_id):
    with _locks_guard:
        return _track_locks.setdefault(track_id, threading.Lock())


# --------------------------------------------------------------------------- #
# Generic helpers
# --------------------------------------------------------------------------- #
def num(value):
    if value in (None, ""):
        return None
    match = re.search(r"\d+(?:\.\d+)?", str(value))
    if not match:
        return None
    number = float(match.group(0))
    return int(number) if number.is_integer() else number


def first(data, *keys):
    for key in keys:
        if data.get(key) not in (None, ""):
            return data[key]
    return None


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


def flac_info(url):
    """Read FLAC STREAMINFO from the first bytes without downloading the file."""
    try:
        response = SESSION.get(
            url,
            headers={"Range": "bytes=0-63"},
            timeout=(5, 12),
            stream=True,
        )
        response.raise_for_status()
        data = response.raw.read(64)
        response.close()
        return parse_streaminfo(data)
    except requests.RequestException:
        return None, None


def local_flac_info(path):
    try:
        with open(path, "rb") as fh:
            return parse_streaminfo(fh.read(64))
    except OSError:
        return None, None


def quality(sr, bd):
    # This addon only returns real FLAC, so its protocol tier is LOSSLESS.
    return "LOSSLESS"


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
    return configured or request.host_url.rstrip("/")


# --------------------------------------------------------------------------- #
# Internet Archive (unchanged behaviour)
# --------------------------------------------------------------------------- #
def inspect(identifier, item):
    name = str(item.get("name", ""))
    url = (
        f"https://archive.org/download/{quote(identifier, safe='')}/"
        f"{quote(name, safe='')}"
    )

    sr = num(first(item, "sample_rate", "samplerate", "sampleRate"))
    bd = num(first(item, "bit_depth", "bitdepth", "bitDepth"))

    # Only probe the actual FLAC header when IA metadata does not already tell us.
    if sr is None or bd is None:
        detected_sr, detected_bd = flac_info(url)
        sr = sr if sr is not None else detected_sr
        bd = bd if bd is not None else detected_bd

    return {
        "url": url,
        "filename": name,
        "size": num(item.get("size")),
        "length": num(item.get("length")),
        "bitrate": num(first(item, "bitrate", "bit_rate", "bitRate")),
        "sampleRate": sr,
        "bitDepth": bd,
        "quality": quality(sr, bd),
    }


def get_flac(identifier):
    response = SESSION.get(
        IA_METADATA.format(quote(identifier, safe="")),
        timeout=(5, 20),
    )
    response.raise_for_status()
    data = response.json()

    candidates = []
    for item in data.get("files", []):
        name = str(item.get("name", ""))
        if not name.lower().endswith(".flac"):
            continue
        candidates.append((name, item))

    if not candidates:
        return None

    # Prefer the largest FLAC first, then inspect the best few in parallel.
    candidates.sort(key=lambda pair: num(pair[1].get("size")) or 0, reverse=True)
    candidates = candidates[:12]

    inspected = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(inspect, identifier, item) for _, item in candidates]
        for future in as_completed(futures):
            try:
                inspected.append(future.result())
            except Exception:
                pass

    if not inspected:
        return None

    inspected.sort(
        key=lambda item: (
            int((item.get("bitDepth") or 0) > 16),
            item.get("bitDepth") or 0,
            item.get("sampleRate") or 0,
            item.get("size") or 0,
        ),
        reverse=True,
    )
    return inspected[0]


def artist_hint(query, title, creator):
    # IA metadata is inconsistent. Keep the real creator when available.
    if isinstance(creator, list):
        return str(creator[0]) if creator else "Internet Archive"
    if creator:
        return str(creator)
    return "Internet Archive"


def make_track(doc, flac):
    identifier = str(doc["identifier"])
    title = str(doc.get("title") or flac["filename"])
    creator = doc.get("creator")
    tier = flac.get("quality", "LOSSLESS")

    return {
        "id": identifier,
        "title": title,
        "artist": artist_hint("", title, creator),
        "album": str(doc.get("album") or ""),
        "duration": flac.get("length"),
        "artworkURL": f"https://archive.org/services/img/{quote(identifier, safe='')}",
        "format": "flac",
        "audioQuality": tier,
        "quality": quality_label(flac.get("sampleRate"), flac.get("bitDepth")),
        "sampleRate": flac.get("sampleRate"),
        "bitDepth": flac.get("bitDepth"),
        "bitrate": flac.get("bitrate"),
        "streamURL": flac["url"],
    }


def archive_search(query, limit):
    try:
        response = SESSION.get(
            IA_SEARCH,
            params={
                "q": f"(mediatype:audio) AND ({query})",
                "fl[]": ["identifier", "title", "creator", "album", "year"],
                "rows": min(limit * 4, 80),
                "page": 1,
                "output": "json",
            },
            timeout=(5, 20),
        )
        response.raise_for_status()
        docs = response.json().get("response", {}).get("docs", [])
    except (requests.RequestException, ValueError):
        return []

    tracks = []
    # Resolve several IA items concurrently so BitChord does not wait through
    # a serial metadata request for every non-FLAC search hit.
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {}
        for doc in docs:
            identifier = doc.get("identifier")
            if identifier:
                futures[pool.submit(get_flac, str(identifier))] = doc

        for future in as_completed(futures):
            doc = futures[future]
            try:
                flac = future.result()
            except Exception:
                continue
            if not flac:
                continue

            tracks.append(make_track(doc, flac))
            if len(tracks) >= limit:
                break
    return tracks


# --------------------------------------------------------------------------- #
# Spotify: metadata, search, temporary download
# --------------------------------------------------------------------------- #
_sp_client = None
_sp_client_failed = False


def get_spotipy():
    """Lazy spotipy client (client-credentials). Returns None if not configured."""
    global _sp_client, _sp_client_failed
    if _sp_client is not None or _sp_client_failed:
        return _sp_client

    cid = os.getenv("SPOTIFY_CLIENT_ID")
    secret = os.getenv("SPOTIFY_CLIENT_SECRET")
    if not cid or not secret:
        _sp_client_failed = True
        return None
    try:
        import spotipy
        from spotipy.oauth2 import SpotifyClientCredentials

        _sp_client = spotipy.Spotify(
            auth_manager=SpotifyClientCredentials(client_id=cid, client_secret=secret)
        )
    except Exception:
        _sp_client_failed = True
        _sp_client = None
    return _sp_client


def extract_spotify_id(text):
    text = (text or "").strip()
    for rx in (SPOTIFY_URL_RE, SPOTIFY_URI_RE):
        m = rx.search(text)
        if m:
            return m.group(1)
    if text.lower().startswith(SPOTIFY_PREFIX):
        candidate = text[len(SPOTIFY_PREFIX):]
        if SPOTIFY_ID_RE.match(candidate):
            return candidate
    return None


def spotify_track_dict(track_id, title, artist, album, duration, art, stream_base):
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
        # Lazy URL: the file is fetched from the lossless providers on first
        # request and kept in the server's temp folder afterwards.
        "streamURL": f"{stream_base}/file/{track_id}",
    }


def spotify_metadata(track_id):
    """Return (title, artist, album, duration_sec, artwork_url). Spotipy first, SpotiFLAC second."""
    sp = get_spotipy()
    if sp is not None:
        try:
            info = sp.track(track_id)
            images = (info.get("album") or {}).get("images") or []
            return (
                info["name"],
                ", ".join(a["name"] for a in info.get("artists", [])),
                (info.get("album") or {}).get("name", ""),
                int(info.get("duration_ms", 0) / 1000),
                images[0]["url"] if images else "",
            )
        except Exception:
            pass

    try:
        from SpotiFLAC import AsyncSpotiFLAC

        async def _meta():
            async with AsyncSpotiFLAC(output_dir=CACHE_DIR) as client:
                return await client.get_track_metadata(
                    f"https://open.spotify.com/track/{track_id}"
                )

        meta = asyncio.run(_meta())
        return (
            meta.title,
            str(meta.artists),
            "",
            int(meta.duration_seconds or 0),
            meta.cover_url or "",
        )
    except Exception:
        return None


def spotify_search(query, limit):
    base = base_url()

    # 1) Direct link / URI / sp:<id>
    track_id = extract_spotify_id(query)
    if track_id:
        meta = spotify_metadata(track_id)
        if meta:
            title, artist, album, dur, art = meta
        else:
            title, artist, album, dur, art = track_id, "Spotify", "", None, ""
        return [spotify_track_dict(track_id, title, artist, album, dur, art, base)]

    # 2) Text search (needs SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET)
    sp = get_spotipy()
    if sp is None:
        return []
    try:
        items = sp.search(q=query, type="track", limit=min(limit, 50))["tracks"]["items"]
    except Exception:
        return []

    tracks = []
    for item in items:
        images = (item.get("album") or {}).get("images") or []
        tracks.append(
            spotify_track_dict(
                item["id"],
                item["name"],
                ", ".join(a["name"] for a in item.get("artists", [])),
                (item.get("album") or {}).get("name", ""),
                int(item.get("duration_ms", 0) / 1000),
                images[0]["url"] if images else "",
                base,
            )
        )
    return tracks


def _find_audio(folder):
    best = None
    for root, _, files in os.walk(folder):
        for name in files:
            if name.lower().endswith(AUDIO_EXTS):
                path = os.path.join(root, name)
                # Prefer FLAC over other containers, then the biggest file
                rank = (name.lower().endswith(".flac"), os.path.getsize(path))
                if best is None or rank > best[0]:
                    best = (rank, path)
    return best[1] if best else None


def _valid(path):
    return bool(path) and os.path.isfile(path) and os.path.getsize(path) > 10000


async def _spotiflac_download(track_id, out_dir):
    from SpotiFLAC import AsyncSpotiFLAC

    os.environ["SPOTIFLAC_PROGRESS_BARS"] = "0"
    async with AsyncSpotiFLAC(
        output_dir=out_dir,
        services=SPOTIFY_SERVICES,
        quality="LOSSLESS",
        allow_fallback=True,
        use_extensions_fallback=True,
    ) as client:
        await client.download_track(f"https://open.spotify.com/track/{track_id}")


def ensure_spotify_file(track_id):
    """
    Make sure the track is present in the temporary cache and return its path.
    Raises RuntimeError if no lossless file could be fetched.
    """
    if not SPOTIFY_ID_RE.match(track_id):
        raise RuntimeError("invalid Spotify track id")

    track_dir = os.path.join(CACHE_DIR, track_id)

    with _lock_for(track_id):  # one download per track at a time
        existing = _find_audio(track_dir) if os.path.isdir(track_dir) else None
        if _valid(existing):
            os.utime(track_dir, None)  # keep it alive
            return existing

        shutil.rmtree(track_dir, ignore_errors=True)
        os.makedirs(track_dir, exist_ok=True)

        try:
            asyncio.run(_spotiflac_download(track_id, track_dir))
        except Exception as exc:
            shutil.rmtree(track_dir, ignore_errors=True)
            raise RuntimeError(f"SpotiFLAC failed: {exc}") from exc

        path = _find_audio(track_dir)
        if not _valid(path):
            shutil.rmtree(track_dir, ignore_errors=True)
            raise RuntimeError("Lossless file not found on any provider")

        os.utime(track_dir, None)
        return path


def cleanup_loop():
    """Delete cached tracks that have not been used for CACHE_TTL seconds."""
    while True:
        time.sleep(300)
        try:
            now = time.time()
            for name in os.listdir(CACHE_DIR):
                folder = os.path.join(CACHE_DIR, name)
                if not os.path.isdir(folder):
                    continue
                lock = _lock_for(name)
                if lock.locked():  # download in progress
                    continue
                # Folder mtime is refreshed on every serve (see /file route)
                if now - os.path.getmtime(folder) > CACHE_TTL:
                    shutil.rmtree(folder, ignore_errors=True)
        except Exception:
            pass


threading.Thread(target=cleanup_loop, daemon=True).start()


# --------------------------------------------------------------------------- #
# Endpoints logic
# --------------------------------------------------------------------------- #
def do_search():
    query = (request.args.get("q") or "").strip()
    if not query:
        return jsonify({"tracks": []})

    # BitChord does not require a limit parameter; it trims the returned rows.
    try:
        limit = min(max(int(request.args.get("limit", "20")), 1), 30)
    except ValueError:
        limit = 20

    source = (request.args.get("source") or "all").lower()
    tracks = []

    if source in ("all", "spotify"):
        try:
            tracks.extend(spotify_search(query, limit))
        except Exception:
            pass

    # A pasted Spotify link should not also hit Internet Archive.
    if source in ("all", "archive") and not extract_spotify_id(query):
        remaining = max(limit - len(tracks), 0) if source == "all" else limit
        if remaining:
            try:
                tracks.extend(archive_search(query, remaining))
            except Exception as exc:
                if not tracks:
                    return jsonify({"tracks": [], "error": str(exc)}), 502

    return jsonify({"tracks": tracks[:limit]})


def do_stream_spotify(track_id):
    try:
        path = ensure_spotify_file(track_id)
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 404

    ext = os.path.splitext(path)[1].lower()
    is_flac = ext == ".flac"
    sr, bd = local_flac_info(path) if is_flac else (None, None)
    label = quality_label(sr, bd) if is_flac else f"Lossless ({ext.lstrip('.').upper()})"
    fmt = "flac" if is_flac else ext.lstrip(".")

    return jsonify({
        "url": f"{base_url()}/file/{track_id}",
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
    })


def do_stream(identifier):
    identifier = (identifier or "").strip()
    if not identifier:
        return jsonify({"error": "missing id"}), 400

    # Spotify items: "sp:<22-char id>" (also accepts a raw link)
    spotify_id = extract_spotify_id(identifier)
    if spotify_id:
        return do_stream_spotify(spotify_id)

    try:
        flac = get_flac(identifier)
    except requests.RequestException as exc:
        return jsonify({"error": str(exc)}), 502

    if not flac:
        return jsonify({"error": "No FLAC file found for this item"}), 404

    return jsonify({
        "url": flac["url"],
        "format": "flac",
        "quality": quality_label(flac.get("sampleRate"), flac.get("bitDepth")),
        "audioQuality": flac.get("quality", "LOSSLESS"),
        "codec": "flac",
        "container": "flac",
        "manifest": "none",
        "encrypted": False,
        "sampleRate": flac.get("sampleRate"),
        "bitDepth": flac.get("bitDepth"),
        "bitrate": flac.get("bitrate"),
        "audioMode": "stereo",
    })


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
    return jsonify({"ok": True, "name": MANIFEST["name"], "version": MANIFEST["version"]})


@app.get("/search")
def search():
    return do_search()


@app.get("/stream/<path:identifier>")
def stream(identifier):
    return do_stream(identifier)


# Compatibility with older addon examples that used /stream?id=...
@app.get("/stream")
def stream_query():
    return do_stream(request.args.get("id"))


@app.get("/file/<track_id>")
def serve_spotify_file(track_id):
    """Serve the temporarily stored Spotify audio (downloads it on first hit)."""
    track_id = track_id.split(".")[0]  # allow /file/<id>.flac
    if not SPOTIFY_ID_RE.match(track_id):
        abort(404)

    try:
        path = ensure_spotify_file(track_id)
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 404

    # Refresh the TTL while the track is being played
    try:
        os.utime(os.path.dirname(path), None)
    except OSError:
        pass

    ext = os.path.splitext(path)[1].lower()
    return send_file(
        path,
        mimetype=MIME_TYPES.get(ext, "application/octet-stream"),
        conditional=True,  # enables Range requests / seeking
        max_age=0,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")), threaded=True)
