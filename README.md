# qobuz-dl

Search, explore and download Lossless and Hi-Res music from [Qobuz](https://www.qobuz.com/).

> A modernized fork with OAuth authentication, parallel downloads, quality auto-fallback, and resilient download infrastructure.

## Features

### Core
- 🎵 Download **FLAC** (16-bit, 24-bit) and **MP3 320** from Qobuz
- 📀 Download albums, tracks, artists, playlists, and labels
- 🔍 **Interactive mode** — search and explore from your terminal
- 🍀 **Lucky mode** — instant download from search query
- 📝 Download URLs from text files or last.fm playlists

### Authentication
- 🔐 **OAuth login** (recommended) — secure browser-based authentication
- 🔑 **Token-based login** — use `user_id` + `user_auth_token` from web player
- 📧 Email + password login (deprecated, kept for compatibility)
- ♻️ **Auto-refresh** — expired tokens and stale app bundles are automatically recovered

### Download Engine
- ⚡ **Parallel downloads** — download multiple tracks simultaneously (`-w 3`)
- 🔄 **Retry with exponential backoff** — automatic retry on connection failures
- 📥 **Resume downloads** — interrupted downloads resume from where they left off (HTTP Range)
- 🛡️ **Akamai bypass** — automatic fallback to segmented download when throttled
- 📊 **Quality auto-fallback** — if Hi-Res isn't available, automatically tries lower qualities (27 → 7 → 6 → 5)
- 🐢 **Speed limiting** — cap download bandwidth (`--limit-rate 5M`)
- ✅ **FLAC integrity check** — validates downloaded files with `flac -t`
- 🔁 **Smart skip** — re-downloads corrupt/incomplete files instead of skipping them

### User Experience
- 📊 **Rich progress bars** — speed (MB/s), ETA, and track info: `[03/12] Artist - Title`
- 📋 **Download summary** — color-coded report: `✓ 10 downloaded  ⚠ 2 skipped  ✗ 1 failed`
- 💬 **Descriptive skip messages** — explains *why* a track was skipped (e.g. "purchase required", "not available in your region")
- 📈 **Batch progress** — shows overall progress for large collections: `[Album 3/15] Title`
- 🔇 **Verbosity control** — `--verbose` for debug output, `--quiet` for errors only

### Organization
- 📁 Customizable folder and track naming patterns
- 💿 Multi-disc album support
- 🏷️ Extended metadata tagging (composer, ISRC, label, genre, copyright)
- 🖼️ Cover art download and embedding
- 📜 M3U playlist generation
- 🗄️ Duplicate detection via portable database

## Getting Started

> **Requirement:** An active Qobuz subscription.

### External Dependencies (Highly Recommended)

`qobuz-dl` uses external system binaries for advanced features:
- **`ffmpeg`**: Required for re-muxing Akamai-bypassed streams. If missing, segmented downloads will fail.
- **`flac`**: Used to perform integrity checks (`flac -t`) on downloaded files to automatically detect and re-download corrupt FLACs.

**Ubuntu / Debian:**
```bash
sudo apt update && sudo apt install ffmpeg flac
```

**macOS (via Homebrew):**
```bash
brew install ffmpeg flac
```

### Install

> **Note for Debian/Ubuntu users:** Modern Linux distributions restrict global `pip` installations (PEP 668). We recommend using `pipx` or a virtual environment (`venv`).

#### Global Install with pipx (Linux / macOS - Recommended)
```bash
# 1. Install pipx (if not already installed)
sudo apt install pipx
pipx ensurepath

# 2. Install qobuz-dl from this local repository
pipx install .
```

#### Virtual Environment (Linux / macOS - Alternative)
```bash
# 1. Create and activate a virtual environment
python3 -m venv qobuz-env
source qobuz-env/bin/activate

# 2. Install qobuz-dl from this local repository
pip install .
```

#### Windows
```bash
pip install windows-curses
pip install .
```

### Upgrading

When new changes are available in this repository, you can update your local installation:

```bash
# 1. Pull the latest code
git pull origin main

# 2. Reinstall depending on your method:
pipx install --force .         # If you used pipx
pip install --upgrade .        # If you used venv / Windows (ensure venv is active)
```

### Initial Setup

```bash
# Create config file (interactive wizard)
qobuz-dl -r
```

The wizard will ask you to choose an authentication method:

```
Choose authentication method:
  [1] OAuth (recommended — opens Qobuz login in browser)
  [2] Token (user_id + user_auth_token from web player)
  [3] Email + Password (deprecated — may not work)
```

For OAuth (recommended), complete authentication with:

```bash
qobuz-dl oauth
```

## Usage Examples

### Download Mode (`dl`)

Download an album in Hi-Res quality:
```bash
qobuz-dl dl https://play.qobuz.com/album/qxjbxh1dc3xyb -q 27
```

Download with 3 parallel workers and speed limit:
```bash
qobuz-dl dl https://play.qobuz.com/album/qxjbxh1dc3xyb -w 3 --limit-rate 5M
```

Download to a Windows drive from WSL using Linux staging (recommended):
```bash
qobuz-dl dl https://play.qobuz.com/album/qxjbxh1dc3xyb -d "/mnt/c/Users/<you>/Music/Qobuz" --staging-dir auto
```

Download multiple URLs to a custom directory:
```bash
qobuz-dl dl https://play.qobuz.com/artist/2038380 https://play.qobuz.com/album/ip8qjy1m6dakc -d "Some pop from 2020"
```

Download from a text file containing URLs:
```bash
qobuz-dl dl urls.txt
```

Download a label's catalog with embedded cover art:
```bash
qobuz-dl dl https://play.qobuz.com/label/7526 --embed-art
```

Download an artist's discography (albums only, skip singles/EPs):
```bash
qobuz-dl dl https://play.qobuz.com/artist/2528676 --albums-only -s
```

Download a last.fm playlist:
```bash
qobuz-dl dl https://www.last.fm/user/vitiko98/playlists/11887574 -q 27
```

> **Tip:** last.fm supports importing playlists from Spotify, Apple Music, and YouTube. Visit `https://www.last.fm/user/<your-profile>/playlists`.

### Interactive Mode (`fun`)

```bash
qobuz-dl fun -l 10
```

Search, browse results interactively, and queue downloads — all from your terminal.

### Lucky Mode (`lucky`)

```bash
# Download first album result
qobuz-dl lucky playboi carti die lit

# Download first 5 artist results
qobuz-dl lucky joy division -n 5 --type artist

# Download first 3 tracks in MP3 320
qobuz-dl lucky eric dolphy remastered --type track -n 3 -q 5
```

### Debug & Troubleshooting

```bash
# Verbose mode — see API calls, retries, fallback decisions
qobuz-dl -v dl https://play.qobuz.com/album/...

# Quiet mode — errors only
qobuz-dl -Q dl https://play.qobuz.com/album/...

# View current config
qobuz-dl -sc

# Reset config file
qobuz-dl -r

# Reset download database
qobuz-dl -p
```

## Configuration

All CLI flags can be set as persistent defaults in the config file (`~/.config/qobuz-dl/config.ini` on Linux).

Run `qobuz-dl -sc` to view your current config, or `qobuz-dl -r` to regenerate it with the interactive wizard.

With `staging_dir = auto`, Linux builds will process tracks in `~/.cache/qobuz-dl/staging` when the destination is under `/mnt/*`, then move the finalized album to the target directory in one finalization pass.

### Config Reference

| Config Key | CLI Flag | Default | Description |
|---|---|---|---|
| `default_folder` | `-d` | `Qobuz Downloads` | Download directory |
| `staging_dir` | `--staging-dir` | `auto` | Temp processing dir before final move (`off` to disable) |
| `default_quality` | `-q` | `6` | Audio quality (5/6/7/27) |
| `default_limit` | `-l` | `20` | Search result limit (fun mode) |
| `folder_format` | `-ff` | `{albumartist}/{album}...` | Album folder naming pattern |
| `track_format` | `-tf` | `{tracknumber} - {tracktitle}` | Track file naming pattern |
| `albums_only` | `--albums-only` | `false` | Skip singles/EPs/VA |
| `no_m3u` | `--no-m3u` | `false` | Skip .m3u generation |
| `no_fallback` | `--no-fallback` | `false` | Disable quality fallback |
| `embed_art` | `-e` | `false` | Embed cover art in files |
| `og_cover` | `--og-cover` | `false` | Original quality cover art |
| `no_cover` | `--no-cover` | `false` | Skip cover art download |
| `no_database` | `--no-db` | `false` | Don't track downloads in DB |
| `smart_discography` | `-s` | `false` | Filter spam in discographies |
| `workers` | `-w` | `0` | DL threads (1 = sequential, 0 = auto-scale) |
| `limit_rate` | `--limit-rate` | *(unlimited)* | Speed limit (e.g. `5M`, `500K`) |
| `lucky_type` | `-t` | `album` | Lucky mode search type |
| `lucky_number` | `-n` | `1` | Lucky mode result count |

### Quality Values

| Value | Format | Description |
|---|---|---|
| `5` | MP3 | 320 kbps |
| `6` | FLAC | 16-bit / 44.1 kHz (CD quality) |
| `7` | FLAC | 24-bit ≤ 96 kHz |
| `27` | FLAC | 24-bit > 96 kHz (Hi-Res) |

### Naming Pattern Keys

Available variables for `folder_format` and `track_format`:

| Key | Example |
|---|---|
| `{artist}` | Pink Floyd |
| `{albumartist}` | Pink Floyd |
| `{album}` | The Dark Side of the Moon |
| `{year}` | 1973 |
| `{tracktitle}` | Time |
| `{tracknumber}` | 04 |
| `{bit_depth}` | 24 |
| `{sampling_rate}` | 96 |
| `{version}` | Remastered |

## Module Usage

```python
import logging
from qobuz_dl.core import QobuzDL

logging.basicConfig(level=logging.INFO)

qobuz = QobuzDL()
qobuz.get_tokens()  # get 'app_id' and 'secrets' attrs
qobuz.initialize_client(email, password, qobuz.app_id, qobuz.secrets)

qobuz.handle_url("https://play.qobuz.com/album/va4j3hdlwaubc")
```

## Architecture

```
qobuz_dl/
├── cli.py          # CLI entry point, config management, logging setup
├── commands.py     # Argument parser definitions
├── core.py         # QobuzDL orchestrator (URL routing, batch downloads)
├── qopy.py         # Qobuz API client (auth, track URLs, metadata)
├── downloader.py   # Download engine (parallel, retry, resume, fallback)
├── metadata.py     # FLAC/MP3 tagging (mutagen)
├── bundle.py       # App ID / secrets extraction from Qobuz web player
├── color.py        # Terminal color codes
├── db.py           # Download history database
└── spoofbuz.py     # Bundle spoofer utilities
```

## Credits

`qobuz-dl` is inspired by the discontinued Qo-DL-Reborn. This tool uses modules originally from Qo-DL: `qopy` and `spoofer`, both written by Sorrow446 and DashLt.

## Disclaimer

- This tool was written for educational purposes. I will not be responsible if you use this program in bad faith. By using it, you are accepting the [Qobuz API Terms of Use](https://static.qobuz.com/apps/api/QobuzAPI-TermsofUse.pdf).
- `qobuz-dl` is not affiliated with Qobuz.
