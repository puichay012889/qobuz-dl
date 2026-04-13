import configparser
import hashlib
import logging
import glob
import os
import sys

from qobuz_dl.bundle import Bundle
from qobuz_dl.color import GREEN, RED, YELLOW
from qobuz_dl.commands import qobuz_dl_args
from qobuz_dl.core import QobuzDL
from qobuz_dl.downloader import DEFAULT_FOLDER, DEFAULT_TRACK
from qobuz_dl.exceptions import InvalidAppSecretError

logger = logging.getLogger(__name__)

if os.name == "nt":
    OS_CONFIG = os.environ.get("APPDATA")
else:
    OS_CONFIG = os.path.join(os.environ["HOME"], ".config")

CONFIG_PATH = os.path.join(OS_CONFIG, "qobuz-dl")
CONFIG_FILE = os.path.join(CONFIG_PATH, "config.ini")
QOBUZ_DB = os.path.join(CONFIG_PATH, "qobuz_dl.db")


def _reset_config(config_file, use_token=False):
    logging.info(f"{YELLOW}Creating config file: {config_file}")

    # --- Auth method selection ---
    if not use_token:
        print(
            "\nChoose authentication method:\n"
            "  [1] OAuth (recommended — opens Qobuz login in browser)\n"
            "  [2] Token (user_id + user_auth_token from web player)\n"
            "  [3] Email + Password (deprecated — may not work)\n"
        )
        choice = input("Enter 1, 2, or 3 (default: 1): ").strip() or "1"
    else:
        choice = "2"  # --token flag passed

    if choice == "1":
        email, password, user_id, user_auth_token = "", "", "", ""
        logging.info(
            f"{YELLOW}OAuth selected. After setup, run:\n"
            f"  qobuz-dl oauth\n"
            f"to complete authentication."
        )
    elif choice == "2":
        email, password = "", ""
        user_id = input("Enter your Qobuz user_id (from web player localStorage):\n- ")
        user_auth_token = input("Enter your Qobuz user_auth_token (from web player localStorage):\n- ")
    else:
        email = input("Enter your email:\n- ")
        raw_pw = input("Enter your password\n- ")
        password = hashlib.md5(raw_pw.encode("utf-8")).hexdigest()
        user_id, user_auth_token = "", ""

    # --- Download settings ---
    default_folder = (
        input("Folder for downloads (leave empty for default 'Qobuz Downloads')\n- ")
        or "Qobuz Downloads"
    )
    default_quality = (
        input(
            "Download quality (5, 6, 7, 27) "
            "[320, LOSSLESS, 24B <96KHZ, 24B >96KHZ]"
            "\n(leave empty for default '6')\n- "
        )
        or "6"
    )

    # --- Fetch tokens ---
    logging.info(f"{YELLOW}Getting tokens. Please wait...")
    bundle = Bundle()
    app_id = str(bundle.get_app_id())
    secrets = ",".join(bundle.get_secrets().values())
    private_key = bundle.get_private_key() or ""

    # --- Write config with descriptive comments ---
    with open(config_file, "w") as f:
        f.write(
            "# =============================================================\n"
            "# qobuz-dl configuration file\n"
            "# All options here serve as defaults and can be overridden\n"
            "# by CLI flags. Run 'qobuz-dl -sc' to view this file.\n"
            "# =============================================================\n"
            "\n"
            "[DEFAULT]\n"
            "\n"
            "# -----------------------------------------------------------\n"
            "# Authentication\n"
            "# Choose ONE method: OAuth (recommended), Token, or Email.\n"
            "# For OAuth: leave all blank, then run 'qobuz-dl oauth'.\n"
            "# For Token: set user_id + user_auth_token (from browser).\n"
            "# For Email: set email + password (MD5-hashed, deprecated).\n"
            "# -----------------------------------------------------------\n"
            f"email = {email}\n"
            f"password = {password}\n"
            f"user_id = {user_id}\n"
            f"user_auth_token = {user_auth_token}\n"
            "\n"
            "# -----------------------------------------------------------\n"
            "# Download Settings\n"
            "# -----------------------------------------------------------\n"
            "\n"
            "# Where to save downloaded files (absolute or relative path)\n"
            f"default_folder = {default_folder}\n"
            "\n"
            "# Audio quality: 5=MP3 320, 6=FLAC 16-bit, 7=FLAC 24-bit≤96kHz,\n"
            "# 27=FLAC 24-bit >96kHz (Hi-Res)\n"
            f"default_quality = {default_quality}\n"
            "\n"
            "# Max search results in interactive mode (qobuz-dl fun)\n"
            "default_limit = 20\n"
            "\n"
            "# -----------------------------------------------------------\n"
            "# File Naming Patterns\n"
            "# Available keys: {artist}, {albumartist}, {album}, {year},\n"
            "#   {sampling_rate}, {bit_depth}, {tracktitle}, {tracknumber},\n"
            "#   {version}\n"
            "# -----------------------------------------------------------\n"
            "\n"
            "# Folder name pattern for albums\n"
            f"folder_format = {DEFAULT_FOLDER}\n"
            "\n"
            "# Track file name pattern\n"
            f"track_format = {DEFAULT_TRACK}\n"
            "\n"
            "# -----------------------------------------------------------\n"
            "# Behavior Flags  (true / false)\n"
            "# -----------------------------------------------------------\n"
            "\n"
            "# Skip singles, EPs, and Various Artists releases\n"
            "albums_only = false\n"
            "\n"
            "# Don't create .m3u playlist files\n"
            "no_m3u = false\n"
            "\n"
            "# Disable quality auto-fallback (skip instead of downgrading)\n"
            "no_fallback = false\n"
            "\n"
            "# Download cover art in original resolution (larger files)\n"
            "og_cover = false\n"
            "\n"
            "# Embed cover art into audio files\n"
            "embed_art = false\n"
            "\n"
            "# Don't download cover art at all\n"
            "no_cover = false\n"
            "\n"
            "# Don't track downloaded IDs in the database\n"
            "no_database = false\n"
            "\n"
            "# Filter out spam/duplicate albums in artist discography\n"
            "smart_discography = false\n"
            "\n"
            "# -----------------------------------------------------------\n"
            "# Performance\n"
            "# -----------------------------------------------------------\n"
            "\n"
            "# Number of parallel track downloads per album (1 = sequential)\n"
            "workers = 1\n"
            "\n"
            "# Download speed limit. Examples: 5M (5 MB/s), 500K (500 KB/s)\n"
            "# Leave empty for unlimited.\n"
            "limit_rate = \n"
            "\n"
            "# -----------------------------------------------------------\n"
            "# Lucky Mode Defaults (qobuz-dl lucky)\n"
            "# -----------------------------------------------------------\n"
            "\n"
            "# Type of items to search: album, artist, track, playlist\n"
            "lucky_type = album\n"
            "\n"
            "# Number of results to download\n"
            "lucky_number = 1\n"
            "\n"
            "# -----------------------------------------------------------\n"
            "# Internal / Auto-generated (do not edit unless you know\n"
            "# what you're doing — regenerate with 'qobuz-dl -r')\n"
            "# -----------------------------------------------------------\n"
            f"app_id = {app_id}\n"
            f"secrets = {secrets}\n"
            f"private_key = {private_key}\n"
        )

    logging.info(
        f"{GREEN}Config file created: {config_file}\n"
        "All options are documented with comments.\n"
        "Edit the file anytime, or override with CLI flags.\n"
        "Run 'qobuz-dl -sc' to view your current config."
    )


def _remove_leftovers(directory):
    directory = os.path.join(directory, "**", ".*.tmp")
    for i in glob.glob(directory, recursive=True):
        try:
            os.remove(i)
        except:  # noqa
            pass


def _handle_commands(qobuz, arguments):
    try:
        if arguments.command == "dl":
            qobuz.download_list_of_urls(arguments.SOURCE)
        elif arguments.command == "lucky":
            query = " ".join(arguments.QUERY)
            qobuz.lucky_type = arguments.type
            qobuz.lucky_limit = arguments.number
            qobuz.lucky_mode(query)
        elif arguments.command == "oauth":
            qobuz.handle_oauth_login(arguments.code)
        else:
            qobuz.interactive_limit = arguments.limit
            qobuz.interactive()

    except KeyboardInterrupt:
        logging.info(
            f"{RED}Interrupted by user\n{YELLOW}Already downloaded items will "
            "be skipped if you try to download the same releases again."
        )

    finally:
        _remove_leftovers(qobuz.directory)


def _initial_checks():
    if not os.path.isdir(CONFIG_PATH) or not os.path.isfile(CONFIG_FILE):
        os.makedirs(CONFIG_PATH, exist_ok=True)
        _reset_config(CONFIG_FILE)

    if len(sys.argv) < 2:
        sys.exit(qobuz_dl_args().print_help())


def main():
    _initial_checks()

    # Parse args early to get verbosity flags before any logging
    # (use a pre-parse to extract -v/-Q before full config-aware parse)
    pre_parser = qobuz_dl_args()
    pre_args, _ = pre_parser.parse_known_args()

    # Configure logging level based on flags
    if getattr(pre_args, "verbose", False):
        log_level = logging.DEBUG
        log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    elif getattr(pre_args, "quiet", False):
        log_level = logging.ERROR
        log_format = "%(message)s"
    else:
        log_level = logging.INFO
        log_format = "%(message)s"

    logging.basicConfig(level=log_level, format=log_format, force=True)

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    try:
        email = config["DEFAULT"].get("email", "")
        password = config["DEFAULT"].get("password", "")
        user_id = config["DEFAULT"].get("user_id", "")
        user_auth_token = config["DEFAULT"].get("user_auth_token", "")
        default_folder = config["DEFAULT"]["default_folder"]
        default_limit = config["DEFAULT"]["default_limit"]
        default_quality = config["DEFAULT"]["default_quality"]
        no_m3u = config.getboolean("DEFAULT", "no_m3u")
        albums_only = config.getboolean("DEFAULT", "albums_only")
        no_fallback = config.getboolean("DEFAULT", "no_fallback")
        og_cover = config.getboolean("DEFAULT", "og_cover")
        embed_art = config.getboolean("DEFAULT", "embed_art")
        no_cover = config.getboolean("DEFAULT", "no_cover")
        no_database = config.getboolean("DEFAULT", "no_database")
        app_id = config["DEFAULT"]["app_id"]
        smart_discography = config.getboolean("DEFAULT", "smart_discography")
        folder_format = config["DEFAULT"]["folder_format"]
        track_format = config["DEFAULT"]["track_format"]
        cfg_workers = config["DEFAULT"].get("workers", "1")
        cfg_limit_rate = config["DEFAULT"].get("limit_rate", "")
        cfg_lucky_type = config["DEFAULT"].get("lucky_type", "album")
        cfg_lucky_number = config["DEFAULT"].get("lucky_number", "1")

        secrets = [
            secret for secret in config["DEFAULT"]["secrets"].split(",") if secret
        ]
        private_key = config["DEFAULT"].get("private_key", "")

        # --- Config Validation ---
        valid_qualities = {"5", "6", "7", "27"}
        if default_quality not in valid_qualities:
            logging.warning(
                f"{YELLOW}Invalid quality '{default_quality}' in config. "
                f"Must be one of {valid_qualities}. Falling back to 6 (FLAC)."
            )
            default_quality = "6"

        if not app_id:
            logging.warning(
                f"{YELLOW}app_id is empty in config. "
                "Run 'qobuz-dl -r' to regenerate."
            )

        if not secrets:
            logging.warning(
                f"{YELLOW}No secrets found in config. "
                "Run 'qobuz-dl -r' to regenerate."
            )

        has_creds = (email and password) or (user_id and user_auth_token)
        if not has_creds:
            logging.info(
                f"{YELLOW}No credentials configured. "
                "Run 'qobuz-dl -r' or 'qobuz-dl oauth' to set up authentication."
            )

        arguments = qobuz_dl_args(
            default_quality, default_limit, default_folder,
            default_lucky_type=cfg_lucky_type,
            default_lucky_number=int(cfg_lucky_number),
        ).parse_args()
    except (KeyError, UnicodeDecodeError, configparser.Error) as error:
        arguments = qobuz_dl_args().parse_args()
        if not arguments.reset:
            sys.exit(
                f"{RED}Your config file is corrupted: {error}! "
                "Run 'qobuz-dl -r' to fix this."
            )

    if arguments.reset:
        sys.exit(_reset_config(CONFIG_FILE))

    if arguments.show_config:
        print(f"Configuation: {CONFIG_FILE}\nDatabase: {QOBUZ_DB}\n---")
        with open(CONFIG_FILE, "r") as f:
            print(f.read())
        sys.exit()

    if arguments.purge:
        try:
            os.remove(QOBUZ_DB)
        except FileNotFoundError:
            pass
        sys.exit(f"{GREEN}The database was deleted.")

    qobuz = QobuzDL(
        arguments.directory,
        arguments.quality,
        arguments.embed_art or embed_art,
        ignore_singles_eps=arguments.albums_only or albums_only,
        no_m3u_for_playlists=arguments.no_m3u or no_m3u,
        quality_fallback=not arguments.no_fallback or not no_fallback,
        cover_og_quality=arguments.og_cover or og_cover,
        no_cover=arguments.no_cover or no_cover,
        downloads_db=None if no_database or arguments.no_db else QOBUZ_DB,
        folder_format=arguments.folder_format or folder_format,
        track_format=arguments.track_format or track_format,
        smart_discography=arguments.smart_discography or smart_discography,
        concurrent_downloads=getattr(arguments, "workers", None) or int(cfg_workers),
    )

    # Apply download speed limit: CLI flag overrides config
    limit_rate = getattr(arguments, "limit_rate", None) or cfg_limit_rate or None
    if limit_rate:
        from qobuz_dl import downloader
        rate_str = limit_rate.strip().upper()
        try:
            if rate_str.endswith("M"):
                downloader._rate_limit_bps = float(rate_str[:-1]) * 1024 * 1024
            elif rate_str.endswith("K"):
                downloader._rate_limit_bps = float(rate_str[:-1]) * 1024
            else:
                downloader._rate_limit_bps = float(rate_str)
            logger.info(f"Download speed limited to {limit_rate}/s")
        except ValueError:
            logger.warning(f"{YELLOW}Invalid --limit-rate value: {limit_rate}. Ignoring.")

    if arguments.command == "oauth":
        if not app_id:
            bundle = Bundle()
            app_id = str(bundle.get_app_id())
            secrets = [s for s in bundle.get_secrets().values() if s]
            private_key = bundle.get_private_key() or ""
        qobuz.app_id = app_id
        qobuz.secrets = secrets
        qobuz.private_key = private_key
        qobuz.handle_oauth_login(arguments.code)
        return

    if user_id and user_auth_token:
        _init_client(qobuz, "token", user_id, user_auth_token, app_id, secrets, config)
    elif email and password:
        _init_client(qobuz, "password", email, password, app_id, secrets, config)
    else:
        logger.error(f"{RED}No credentials found. Run 'qobuz-dl -r' to set up.")
        return

    _handle_commands(qobuz, arguments)


def _init_client(qobuz, method, primary, secondary, app_id, secrets, config):
    """Initialize the Qobuz client, auto-refreshing the bundle on stale secrets."""
    try:
        if method == "token":
            qobuz.initialize_client_with_token(primary, secondary, app_id, secrets)
        else:
            qobuz.initialize_client(primary, secondary, app_id, secrets)
    except InvalidAppSecretError:
        logger.info(
            f"{YELLOW}App secrets are stale. Refreshing from Qobuz web player..."
        )
        try:
            bundle = Bundle()
            new_app_id = str(bundle.get_app_id())
            new_secrets = [s for s in bundle.get_secrets().values() if s]
            new_private_key = bundle.get_private_key() or ""
            # persist the new tokens to config.ini so next run is fast
            _update_bundle_in_config(new_app_id, new_secrets, new_private_key)
            logger.info(f"{GREEN}Bundle refreshed. Retrying...")
            if method == "token":
                qobuz.initialize_client_with_token(
                    primary, secondary, new_app_id, new_secrets
                )
            else:
                qobuz.initialize_client(primary, secondary, new_app_id, new_secrets)
        except Exception as e:
            logger.error(
                f"{RED}Bundle refresh failed: {e}. "
                "Run 'qobuz-dl -r' to reset your config."
            )
            raise


def _update_bundle_in_config(app_id, secrets, private_key):
    """Persist refreshed bundle values to config.ini without wiping other settings."""
    import configparser
    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_FILE)
    cfg["DEFAULT"]["app_id"] = app_id
    cfg["DEFAULT"]["secrets"] = ",".join(secrets)
    cfg["DEFAULT"]["private_key"] = private_key
    with open(CONFIG_FILE, "w") as f:
        cfg.write(f)
    logger.info(f"{GREEN}config.ini updated with new bundle tokens.")


if __name__ == "__main__":
    sys.exit(main())
