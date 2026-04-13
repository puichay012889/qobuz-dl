import logging
import os
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Tuple

import requests
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from pathvalidate import sanitize_filename, sanitize_filepath
from tqdm import tqdm

import qobuz_dl.metadata as metadata
from qobuz_dl.color import OFF, GREEN, RED, YELLOW, CYAN
from qobuz_dl.exceptions import NonStreamable

QL_DOWNGRADE = "FormatRestrictedByFormatAvailability"
# used in case of error
DEFAULT_FORMATS = {
    "MP3": [
        "{artist} - {album} ({year}) [MP3]",
        "{tracknumber}. {tracktitle}",
    ],
    "Unknown": [
        "{artist} - {album}",
        "{tracknumber}. {tracktitle}",
    ],
}

DEFAULT_FOLDER = "{artist} - {album} ({year}) [{bit_depth}B-{sampling_rate}kHz]"
DEFAULT_TRACK = "{tracknumber}. {tracktitle}"

logger = logging.getLogger(__name__)


class Download:
    def __init__(
        self,
        client,
        item_id: str,
        path: str,
        quality: int,
        embed_art: bool = False,
        albums_only: bool = False,
        downgrade_quality: bool = False,
        cover_og_quality: bool = False,
        no_cover: bool = False,
        folder_format=None,
        track_format=None,
    ):
        self.client = client
        self.item_id = item_id
        self.path = path
        self.quality = quality
        self.albums_only = albums_only
        self.embed_art = embed_art
        self.downgrade_quality = downgrade_quality
        self.cover_og_quality = cover_og_quality
        self.no_cover = no_cover
        self.folder_format = folder_format or DEFAULT_FOLDER
        self.track_format = track_format or DEFAULT_TRACK
        self.concurrent_downloads = 1  # set by caller; > 1 enables parallel mode
        self._count_lock = threading.Lock()  # protects the tmp-file counter

    def download_id_by_type(self, track=True):
        if not track:
            self.download_release()
        else:
            self.download_track()

    def download_release(self):
        count = 0
        meta = self.client.get_album_meta(self.item_id)

        if not meta.get("streamable"):
            raise NonStreamable("This release is not streamable")

        if self.albums_only and (
            meta.get("release_type") != "album"
            or meta.get("artist").get("name") == "Various Artists"
        ):
            logger.info(f'{OFF}Ignoring Single/EP/VA: {meta.get("title", "n/a")}')
            return

        album_title = _get_title(meta)

        format_info = self._get_format(meta)
        file_format, quality_met, bit_depth, sampling_rate = format_info

        if not self.downgrade_quality and not quality_met:
            logger.info(
                f"{OFF}Skipping {album_title} as it doesn't meet quality requirement"
            )
            return

        logger.info(
            f"\n{YELLOW}Downloading: {album_title}\nQuality: {file_format}"
            f" ({bit_depth}/{sampling_rate})\n"
        )
        album_attr = self._get_album_attr(
            meta, album_title, file_format, bit_depth, sampling_rate
        )
        folder_format, track_format = _clean_format_str(
            self.folder_format, self.track_format, file_format
        )
        sanitized_title = sanitize_filepath(folder_format.format(**album_attr))
        dirn = os.path.join(self.path, sanitized_title)
        os.makedirs(dirn, exist_ok=True)

        if self.no_cover:
            logger.info(f"{OFF}Skipping cover")
        else:
            _get_extra(meta["image"]["large"], dirn, og_quality=self.cover_og_quality)

        if "goodies" in meta:
            try:
                _get_extra(meta["goodies"][0]["url"], dirn, "booklet.pdf")
            except:  # noqa
                pass
        media_numbers = [track["media_number"] for track in meta["tracks"]["items"]]
        is_multiple = True if len([*{*media_numbers}]) > 1 else False

        tracks = meta["tracks"]["items"]

        if self.concurrent_downloads > 1:
            self._download_tracks_parallel(tracks, dirn, meta, is_multiple)
        else:
            self._download_tracks_sequential(tracks, dirn, meta, is_multiple)

        logger.info(f"{GREEN}Completed")

    def _download_tracks_sequential(self, tracks, dirn, meta, is_multiple):
        """Original one-at-a-time download loop."""
        for count, i in enumerate(tracks):
            parse = self.client.get_track_url(i["id"], fmt_id=self.quality)
            if "sample" not in parse and parse["sampling_rate"]:
                is_mp3 = True if int(self.quality) == 5 else False
                self._download_and_tag(
                    dirn, count, parse, i, meta, False, is_mp3,
                    i["media_number"] if is_multiple else None,
                )
            else:
                logger.info(f"{OFF}Demo. Skipping")

    def _download_tracks_parallel(self, tracks, dirn, meta, is_multiple):
        """Parallel download using ThreadPoolExecutor."""
        counter = [0]  # mutable container for atomic-style increment

        def _worker(i):
            with self._count_lock:
                count = counter[0]
                counter[0] += 1
            try:
                parse = self.client.get_track_url(i["id"], fmt_id=self.quality)
                if "sample" not in parse and parse["sampling_rate"]:
                    is_mp3 = True if int(self.quality) == 5 else False
                    self._download_and_tag(
                        dirn, count, parse, i, meta, False, is_mp3,
                        i["media_number"] if is_multiple else None,
                    )
                else:
                    logger.info(f"{OFF}Demo. Skipping")
            except Exception as exc:
                track_title = i.get("title", i.get("id", "unknown"))
                logger.error(f"{RED}Failed to download '{track_title}': {exc}")

        with ThreadPoolExecutor(max_workers=self.concurrent_downloads) as pool:
            futures = {pool.submit(_worker, i): i for i in tracks}
            for future in as_completed(futures):
                future.result()  # re-raises so outer code can catch if needed

    def download_track(self):
        parse = self.client.get_track_url(self.item_id, self.quality)

        if "sample" not in parse and parse["sampling_rate"]:
            meta = self.client.get_track_meta(self.item_id)
            track_title = _get_title(meta)
            artist = _safe_get(meta, "performer", "name")
            logger.info(f"\n{YELLOW}Downloading: {artist} - {track_title}")
            format_info = self._get_format(meta, is_track_id=True, track_url_dict=parse)
            file_format, quality_met, bit_depth, sampling_rate = format_info

            folder_format, track_format = _clean_format_str(
                self.folder_format, self.track_format, str(bit_depth)
            )

            if not self.downgrade_quality and not quality_met:
                logger.info(
                    f"{OFF}Skipping {track_title} as it doesn't "
                    "meet quality requirement"
                )
                return
            track_attr = self._get_track_attr(
                meta, track_title, bit_depth, sampling_rate
            )
            sanitized_title = sanitize_filepath(folder_format.format(**track_attr))

            dirn = os.path.join(self.path, sanitized_title)
            os.makedirs(dirn, exist_ok=True)
            if self.no_cover:
                logger.info(f"{OFF}Skipping cover")
            else:
                _get_extra(
                    meta["album"]["image"]["large"],
                    dirn,
                    og_quality=self.cover_og_quality,
                )
            is_mp3 = True if int(self.quality) == 5 else False
            self._download_and_tag(
                dirn,
                1,
                parse,
                meta,
                meta,
                True,
                is_mp3,
                False,
            )
        else:
            logger.info(f"{OFF}Demo. Skipping")
        logger.info(f"{GREEN}Completed")

    def _download_and_tag(
        self,
        root_dir,
        tmp_count,
        track_url_dict,
        track_metadata,
        album_or_track_metadata,
        is_track,
        is_mp3,
        multiple=None,
    ):
        extension = ".mp3" if is_mp3 else ".flac"

        if "url" not in track_url_dict and "url_template" not in track_url_dict:
            logger.info(f"{OFF}Track not available for download")
            return

        if multiple:
            root_dir = os.path.join(root_dir, f"Disc {multiple}")
            os.makedirs(root_dir, exist_ok=True)

        filename = os.path.join(root_dir, f".{tmp_count:02}.tmp")

        # Determine the filename
        track_title = track_metadata.get("title")
        artist = _safe_get(track_metadata, "performer", "name")
        filename_attr = self._get_filename_attr(artist, track_metadata, track_title)

        # track_format is a format string
        # e.g. '{tracknumber}. {artist} - {tracktitle}'
        formatted_path = sanitize_filename(self.track_format.format(**filename_attr))
        final_file = os.path.join(root_dir, formatted_path)[:250] + extension

        if os.path.isfile(final_file):
            logger.info(f"{OFF}{track_title} was already downloaded")
            return

        if "url" in track_url_dict:
            try:
                # 1. FAST PATH: direct URL download
                tqdm_download(track_url_dict["url"], filename, filename)
            except (ConnectionError, requests.exceptions.ChunkedEncodingError):
                # Akamai block detected — tqdm_download normalizes all streaming
                # errors to ConnectionError, but keep ChunkedEncodingError as
                # a safety net in case it escapes tqdm_download somehow.
                logger.info(
                    f"{YELLOW}Akamai block detected on '{track_title}'. "
                    "Switching to segmented download..."
                )
                # Clean up partial file before retry
                if os.path.isfile(filename):
                    os.remove(filename)
                track_id = track_metadata.get("id")
                track_url_dict = self.client.get_track_url(
                    track_id, int(self.quality), force_segments=True
                )
                tqdm_download_segments(track_url_dict, filename, filename)
        else:
            # url_template already present — go straight to segmented download
            tqdm_download_segments(track_url_dict, filename, filename)
        tag_function = metadata.tag_mp3 if is_mp3 else metadata.tag_flac
        try:
            tag_function(
                filename,
                root_dir,
                final_file,
                track_metadata,
                album_or_track_metadata,
                is_track,
                self.embed_art,
            )
        except Exception as e:
            logger.error(f"{RED}Error tagging the file: {e}", exc_info=True)

    @staticmethod
    def _get_filename_attr(artist, track_metadata, track_title):
        return {
            "artist": artist,
            "albumartist": _safe_get(
                track_metadata, "album", "artist", "name", default=artist
            ),
            "bit_depth": track_metadata["maximum_bit_depth"],
            "sampling_rate": track_metadata["maximum_sampling_rate"],
            "tracktitle": track_title,
            "version": track_metadata.get("version"),
            "tracknumber": f"{track_metadata['track_number']:02}",
        }

    @staticmethod
    def _get_track_attr(meta, track_title, bit_depth, sampling_rate):
        return {
            "album": sanitize_filename(meta["album"]["title"]),
            "artist": sanitize_filename(meta["album"]["artist"]["name"]),
            "tracktitle": track_title,
            "year": meta["album"]["release_date_original"].split("-")[0],
            "bit_depth": bit_depth,
            "sampling_rate": sampling_rate,
        }

    @staticmethod
    def _get_album_attr(meta, album_title, file_format, bit_depth, sampling_rate):
        return {
            "artist": sanitize_filename(meta["artist"]["name"]),
            "album": sanitize_filename(album_title),
            "year": meta["release_date_original"].split("-")[0],
            "format": file_format,
            "bit_depth": bit_depth,
            "sampling_rate": sampling_rate,
        }

    def _get_format(self, item_dict, is_track_id=False, track_url_dict=None):
        quality_met = True
        if int(self.quality) == 5:
            return ("MP3", quality_met, None, None)
        track_dict = item_dict
        if not is_track_id:
            track_dict = item_dict["tracks"]["items"][0]

        try:
            new_track_dict = (
                self.client.get_track_url(track_dict["id"], fmt_id=self.quality)
                if not track_url_dict
                else track_url_dict
            )
            restrictions = new_track_dict.get("restrictions")
            if isinstance(restrictions, list):
                if any(
                    restriction.get("code") == QL_DOWNGRADE
                    for restriction in restrictions
                ):
                    quality_met = False

            return (
                "FLAC",
                quality_met,
                new_track_dict["bit_depth"],
                new_track_dict["sampling_rate"],
            )
        except (KeyError, requests.exceptions.HTTPError):
            return ("Unknown", quality_met, None, None)


def tqdm_download(url, fname, desc, max_retries=3):
    """Download *url* to *fname* with automatic retry, exponential backoff,
    and HTTP Range-based resume.

    On each retry attempt, checks if a partial file exists and sends a
    Range header to resume from that byte offset instead of restarting.
    If the server doesn't support Range requests, falls back gracefully
    to a full restart.

    After all retries are exhausted, raises ConnectionError which triggers
    the Akamai segmented download fallback in _download_and_tag.
    """
    for attempt in range(max_retries):
        # On retry, try to resume from partial file rather than restart
        resume_from = (
            os.path.getsize(fname)
            if attempt > 0 and os.path.isfile(fname)
            else 0
        )
        try:
            _tqdm_download_once(url, fname, desc, resume_from=resume_from)
            return  # success
        except ConnectionError as exc:
            if attempt < max_retries - 1:
                wait = 2 ** attempt  # 1 s, 2 s, 4 s
                logger.info(
                    f"{YELLOW}Download interrupted (attempt {attempt + 1}/{max_retries}). "
                    f"Retrying in {wait}s..."
                    + (f" Resuming from {resume_from / 1024 / 1024:.1f} MB." if resume_from else "")
                )
                time.sleep(wait)
            else:
                # All retries exhausted — clean up and propagate to Akamai fallback
                if os.path.isfile(fname):
                    os.remove(fname)
                raise


def _tqdm_download_once(url, fname, desc, resume_from=0):
    """Single download attempt with optional byte-range resume."""
    headers = {}
    if resume_from:
        headers["Range"] = f"bytes={resume_from}-"

    try:
        r = requests.get(url, allow_redirects=True, stream=True, headers=headers)
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Failed to start download for {fname}: {e}") from e

    # If server returns 200 instead of 206, it doesn't support Range — restart
    if resume_from and r.status_code == 200:
        logger.info(f"{YELLOW}Server doesn't support resume. Restarting download...")
        resume_from = 0
        if os.path.isfile(fname):
            os.remove(fname)

    content_length = int(r.headers.get("content-length", 0))
    total = content_length + resume_from
    download_size = resume_from
    file_mode = "ab" if resume_from else "wb"

    try:
        with open(fname, file_mode) as file, tqdm(
            total=total,
            initial=resume_from,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            desc=desc,
            bar_format=CYAN + "{n_fmt}/{total_fmt} /// {desc}",
        ) as bar:
            for data in r.iter_content(chunk_size=1024):
                size = file.write(data)
                bar.update(size)
                download_size += size
    except requests.exceptions.RequestException as e:
        # Catches ChunkedEncodingError (IncompleteRead), ConnectionError, etc.
        raise ConnectionError(f"Download interrupted for {fname}: {e}") from e

    if total and total != download_size:
        raise ConnectionError("File download was interrupted for " + fname)

def tqdm_download_segments(track_url_dict, fname, desc):
    """Download an Akamai-segmented track, decrypt each segment with AES-CTR,
    write a concatenated MP4, then remux to FLAC via ffmpeg."""
    tmp_fname = fname + ".mp4"
    segment_uuid = None
    total = 0
    for segment in range(track_url_dict["n_segments"] + 1):
        r = requests.head(
            track_url_dict["url_template"].replace("$SEGMENT$", str(segment)),
            allow_redirects=True,
        )
        r.raise_for_status()
        total += int(r.headers.get("content-length", 0))

    try:
        with open(tmp_fname, "wb") as file, tqdm(
            total=total,
            unit="iB",
            unit_scale=True,
            unit_divisor=1024,
            desc=desc,
            bar_format=CYAN + "{n_fmt}/{total_fmt} /// {desc}",
        ) as bar:
            for segment in range(track_url_dict["n_segments"] + 1):
                r = requests.get(
                    track_url_dict["url_template"].replace("$SEGMENT$", str(segment)),
                    allow_redirects=True,
                    stream=True,
                )
                r.raise_for_status()
                segment_total = int(r.headers.get("content-length", 0))
                segment_size = 0
                segment_data = bytearray()
                for data in r.iter_content(chunk_size=1024):
                    segment_data.extend(data)
                    size = len(data)
                    bar.update(size)
                    segment_size += size
                r.close()

                if segment_total and segment_total != segment_size:
                    raise ConnectionError("Segment download interrupted for " + fname)
                if segment == 1:
                    segment_uuid = _get_qobuz_segment_uuid(segment_data)
                    if segment_uuid is None:
                        raise ConnectionError(
                            "Cannot find Qobuz segment UUID for " + fname
                        )
                file.write(
                    _decrypt_qobuz_segment(
                        segment_data, track_url_dict["raw_key"], segment_uuid
                    )
                )

        remux = subprocess.run(
            ["ffmpeg", "-nostdin", "-v", "error", "-y",
             "-i", tmp_fname, "-c:a", "copy", "-f", "flac", fname],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        if remux.returncode != 0:
            raise ConnectionError(
                "ffmpeg remux failed for {}: {}".format(
                    fname, remux.stderr.strip() or "unknown error"
                )
            )
    finally:
        if os.path.isfile(tmp_fname):
            os.remove(tmp_fname)


def _get_qobuz_segment_uuid(segment_data):
    pos = 0
    while pos + 24 <= len(segment_data):
        size = int.from_bytes(segment_data[pos : pos + 4], "big")
        if size <= 0 or pos + size > len(segment_data):
            break
        if bytes(segment_data[pos + 4 : pos + 8]) == b"uuid":
            return bytes(segment_data[pos + 8 : pos + 24])
        pos += size
    return None


def _decrypt_qobuz_segment(segment_data, raw_key, segment_uuid):
    if segment_uuid is None:
        return bytes(segment_data)

    buf = bytearray(segment_data)
    pos = 0
    while pos + 8 <= len(buf):
        size = int.from_bytes(buf[pos : pos + 4], "big")
        if size <= 0 or pos + size > len(buf):
            break
        if (
            bytes(buf[pos + 4 : pos + 8]) == b"uuid"
            and bytes(buf[pos + 8 : pos + 24]) == segment_uuid
        ):
            pointer = pos + 28
            data_end = pos + int.from_bytes(buf[pointer : pointer + 4], "big")
            pointer += 4
            counter_len = buf[pointer]
            pointer += 1
            frame_count = int.from_bytes(buf[pointer : pointer + 3], "big")
            pointer += 3
            for _ in range(frame_count):
                frame_len = int.from_bytes(buf[pointer : pointer + 4], "big")
                pointer += 6
                flags = int.from_bytes(buf[pointer : pointer + 2], "big")
                pointer += 2
                frame_start = data_end
                frame_end = frame_start + frame_len
                data_end = frame_end
                if flags:
                    counter = bytes(buf[pointer : pointer + counter_len]) + (
                        b"\x00" * (16 - counter_len)
                    )
                    decryptor = Cipher(
                        algorithms.AES(raw_key), modes.CTR(counter)
                    ).decryptor()
                    buf[frame_start:frame_end] = decryptor.update(
                        bytes(buf[frame_start:frame_end])
                    ) + decryptor.finalize()
                pointer += counter_len
        pos += size
    return bytes(buf)


def _get_description(item: dict, track_title, multiple=None):
    downloading_title = f"{track_title} "
    f'[{item["bit_depth"]}/{item["sampling_rate"]}]'
    if multiple:
        downloading_title = f"[Disc {multiple}] {downloading_title}"
    return downloading_title


def _get_title(item_dict):
    album_title = item_dict["title"]
    version = item_dict.get("version")
    if version:
        album_title = (
            f"{album_title} ({version})"
            if version.lower() not in album_title.lower()
            else album_title
        )
    return album_title


def _get_extra(item, dirn, extra="cover.jpg", og_quality=False):
    extra_file = os.path.join(dirn, extra)
    if os.path.isfile(extra_file):
        logger.info(f"{OFF}{extra} was already downloaded")
        return
    tqdm_download(
        item.replace("_600.", "_org.") if og_quality else item,
        extra_file,
        extra,
    )


def _clean_format_str(folder: str, track: str, file_format: str) -> Tuple[str, str]:
    """Cleans up the format strings, avoids errors
    with MP3 files.
    """
    final = []
    for i, fs in enumerate((folder, track)):
        if fs.endswith(".mp3"):
            fs = fs[:-4]
        elif fs.endswith(".flac"):
            fs = fs[:-5]
        fs = fs.strip()

        # default to pre-chosen string if format is invalid
        if file_format in ("MP3", "Unknown") and (
            "bit_depth" in fs or "sampling_rate" in fs
        ):
            default = DEFAULT_FORMATS[file_format][i]
            logger.error(
                f"{RED}invalid format string for format {file_format}"
                f". defaulting to {default}"
            )
            fs = default
        final.append(fs)

    return tuple(final)


def _safe_get(d: dict, *keys, default=None):
    """A replacement for chained `get()` statements on dicts:
    >>> d = {'foo': {'bar': 'baz'}}
    >>> _safe_get(d, 'baz')
    None
    >>> _safe_get(d, 'foo', 'bar')
    'baz'
    """
    curr = d
    res = default
    for key in keys:
        res = curr.get(key, default)
        if res == default or not hasattr(res, "__getitem__"):
            return res
        else:
            curr = res
    return res
