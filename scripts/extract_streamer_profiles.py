#!/usr/bin/env python3
"""Export public Twitch profiles selected from ClickHouse. Python 3.10+, no dependencies."""

import argparse
from contextlib import closing
import csv
from datetime import datetime, timezone
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import shlex
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request


HELIX = "https://api.twitch.tv/helix/"
OAUTH = "https://id.twitch.tv/oauth2/"
SECTIONS = (
    "followers", "videos", "clips", "schedule", "teams", "emotes", "badges",
    "chat_settings", "extensions", "chat_color", "cheermotes", "games",
)
CSV_FIELDS = (
    "requested_login", "status", "collected_at", "id", "login", "display_name",
    "description", "broadcaster_type", "created_at", "channel_url", "follower_count",
    "profile_image_url", "offline_image_url", "live_thumbnail_url", "is_live",
    "viewer_count", "title", "game_name", "language", "tags", "bio_links",
    "observed_first_at", "observed_last_at", "observed_peak_viewers", "errors",
)


def now():
    return datetime.now(timezone.utc).isoformat()


def log(message):
    print(message, file=sys.stderr, flush=True)


def load_env(path):
    """Read simple dotenv assignments without executing shell code or expanding variables."""
    values = {}
    if path.is_file():
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].lstrip()
            key, separator, value = line.partition("=")
            if not separator or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key.strip()):
                raise ValueError(f"Invalid dotenv assignment at line {number}")
            value = value.strip()
            if value.startswith(("'", '"')):
                try:
                    parts = shlex.split(value, comments=True)
                except ValueError:
                    raise ValueError(f"Invalid dotenv quoting at line {number}") from None
                if len(parts) != 1:
                    raise ValueError(f"Invalid dotenv value at line {number}")
                value = parts[0]
            else:
                value = re.split(r"\s+#", value, maxsplit=1)[0].rstrip()
            values[key.strip()] = value
    return {**values, **os.environ}


class APIError(RuntimeError):
    def __init__(self, endpoint, status, detail=""):
        self.status = status
        super().__init__(f"{endpoint}: HTTP {status}" if status else f"{endpoint}: {detail}")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    # Never forward Twitch or database credentials to a redirect destination.
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class HTTP:
    def __init__(self, timeout=30, retries=4, interval=0.12):
        self.timeout = timeout
        self.retries = retries
        self.interval = interval
        self.next_request = 0.0
        self.opener = urllib.request.build_opener(NoRedirect())

    def request(self, url, *, label, headers=None, data=None, image=False):
        headers = {"User-Agent": "poglytics-profile-export/1.0", **(headers or {})}
        for attempt in range(self.retries + 1):
            time.sleep(max(0, self.next_request - time.time()))
            self.next_request = time.time() + self.interval
            retry_headers = {}
            try:
                request = urllib.request.Request(url, headers=headers, data=data)
                with self.opener.open(request, timeout=self.timeout) as response:
                    if response.headers.get("Ratelimit-Remaining") == "0":
                        self.next_request = max(
                            self.next_request, float(response.headers.get("Ratelimit-Reset", 0)) + 1
                        )
                    if image:
                        content_type = response.headers.get_content_type()
                        if not content_type.startswith("image/"):
                            raise APIError(label, 0, "response is not an image")
                        body = response.read(10 * 1024 * 1024 + 1)
                        if len(body) > 10 * 1024 * 1024:
                            raise APIError(label, 0, "image exceeds 10 MiB")
                        return body, content_type
                    return response.read()
            except urllib.error.HTTPError as exc:
                status = exc.code
                retry_headers = dict(exc.headers)
                exc.close()
                error = APIError(label, status)
                if status != 429 and not 500 <= status <= 599:
                    raise error from None
            except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as exc:
                # Do not include URLs, request bodies, headers, or credentials in errors.
                error = APIError(label, 0, f"network failure ({type(exc).__name__})")
            if attempt == self.retries:
                raise error from None
            retry_headers = {k.lower(): v for k, v in retry_headers.items()}
            delay = min(2 ** attempt, 30)
            for header, absolute in (("ratelimit-reset", True), ("retry-after", False)):
                try:
                    delay = max(delay, float(retry_headers[header]) - (time.time() if absolute else 0) + 1)
                except (KeyError, ValueError):
                    pass
            log(f"{label}: retry {attempt + 1}/{self.retries} in {delay:.1f}s")
            self.next_request = max(self.next_request, time.time() + delay)

    def json(self, url, **kwargs):
        try:
            result = json.loads(self.request(url, **kwargs))
        except (ValueError, UnicodeError):
            raise APIError(kwargs["label"], 0, "invalid JSON response") from None
        if not isinstance(result, dict):
            raise APIError(kwargs["label"], 0, "expected a JSON object")
        return result


class Twitch:
    def __init__(self, http, env):
        self.http = http
        self.client_id = env.get("TWITCH_CLIENT_ID") or env.get("CLIENT_ID", "")
        self.secret = env.get("TWITCH_CLIENT_SECRET") or env.get("CLIENT_SECRET", "")
        self.token = env.get("TWITCH_ACCESS_TOKEN", "").removeprefix("oauth:")
        self.supplied_token = bool(self.token)
        self.expires_at = 0
        self.validate_at = 0
        if not self.client_id or not (self.secret or self.token):
            raise ValueError("Set CLIENT_ID and CLIENT_SECRET (or TWITCH_ACCESS_TOKEN) in .env/environment")

    def authenticate(self, force=False):
        if not self.supplied_token and (force or time.time() >= self.expires_at):
            body = urllib.parse.urlencode({
                "client_id": self.client_id, "client_secret": self.secret,
                "grant_type": "client_credentials",
            }).encode()
            response = self.http.json(
                OAUTH + "token", label="Twitch authentication", data=body,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            self.token = response.get("access_token", "")
            if not self.token:
                raise APIError("Twitch authentication", 0, "missing access token")
            self.expires_at = time.time() + response.get("expires_in", 3600) - 60
            self.validate_at = 0
        if time.time() >= self.validate_at:
            response = self.http.json(
                OAUTH + "validate", label="Twitch token validation",
                headers={"Authorization": "OAuth " + self.token},
            )
            if response.get("client_id") != self.client_id:
                raise ValueError("TWITCH_ACCESS_TOKEN belongs to a different CLIENT_ID")
            self.validate_at = time.time() + min(3600, max(1, response.get("expires_in", 3600) - 60))

    def get(self, endpoint, params):
        self.authenticate()
        for attempt in range(2):
            try:
                response = self.http.json(
                    HELIX + endpoint + "?" + urllib.parse.urlencode(params, doseq=True),
                    label="Twitch " + endpoint,
                    headers={"Client-Id": self.client_id, "Authorization": "Bearer " + self.token},
                )
                if "data" not in response and endpoint != "channels/followers":
                    raise APIError(endpoint, 0, "missing data in response")
                return response
            except APIError as exc:
                if exc.status != 401 or self.supplied_token or attempt:
                    raise
                self.authenticate(force=True)


def validate_login(login):
    login = login.strip().lower()
    if not re.fullmatch(r"[a-z0-9_]{1,25}", login):
        raise ValueError("Input contains an invalid Twitch login; use login names, not URLs")
    return login


def source_rows(args, env, http):
    if args.logins or args.logins_file:
        logins = args.logins or []
        if args.logins_file:
            with args.logins_file.open(encoding="utf-8-sig", newline="") as source:
                if args.logins_file.suffix.lower() == ".csv":
                    reader = csv.DictReader(source)
                    field = next((f for f in ("user_login", "login") if f in (reader.fieldnames or [])), None)
                    if not field:
                        raise ValueError("CSV must have a user_login or login header")
                    logins = [row[field] for row in reader]
                else:
                    logins = [line.strip() for line in source if line.strip() and not line.startswith("#")]
        logins = sorted({validate_login(login) for login in logins})
        return [{"user_login": login} for login in (logins[:args.limit] if args.limit else logins)]

    database = args.database or env.get("CLICKHOUSE_DATABASE") or env.get("DB_NAME", "default")
    for identifier in (database, args.table):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
            raise ValueError("Database and table names must be simple SQL identifiers")
    url = args.clickhouse_url or env.get("CLICKHOUSE_HTTP_URL")
    if not url:
        host = env.get("DB_HOST", "localhost")
        if ":" in host and not host.startswith("["):
            host = "[" + host + "]"
        url = f"http://{host}:{env.get('CLICKHOUSE_HTTP_PORT', '8123')}"
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.query or parsed.fragment:
        raise ValueError("CLICKHOUSE_HTTP_URL must be an HTTP(S) URL without credentials, query, or fragment")
    if parsed.port in (9000, 9440):
        raise ValueError("Use the ClickHouse HTTP port (usually 8123 or HTTPS 8443), not its native port")
    columns = "DISTINCT lower(user_login) AS user_login"
    group = ""
    if args.history:
        columns = """lower(user_login) AS user_login,
            count() AS observed_snapshot_count,
            uniqExact(stream_id) AS observed_stream_count,
            min(snapshot_time) AS observed_first_at,
            max(snapshot_time) AS observed_last_at,
            max(viewer_count) AS observed_peak_viewers,
            round(avg(viewer_count), 2) AS observed_mean_viewers,
            groupUniqArray(game_name) AS observed_games,
            argMax(thumbnail_url, snapshot_time) AS last_observed_thumbnail_url"""
        group = "GROUP BY lower(user_login)"
    query = f"""SELECT {columns} FROM `{database}`.`{args.table}`
        WHERE language = {{language:String}} AND user_login != ''
        {group} ORDER BY user_login"""
    if args.limit:
        query += f" LIMIT {args.limit}"
    query += " FORMAT JSONEachRow"
    parameters = urllib.parse.urlencode({"param_language": args.language, "readonly": "1"})
    body = http.request(
        url.rstrip("/") + "/?" + parameters, label="ClickHouse selection",
        data=query.encode(), headers={
            "Content-Type": "text/plain; charset=utf-8",
            "X-ClickHouse-User": env.get("CLICKHOUSE_USER") or env.get("DB_USER", "default"),
            "X-ClickHouse-Key": env.get("CLICKHOUSE_PASSWORD", env.get("DB_PASSWORD", "")),
        },
    )
    try:
        rows = [json.loads(line) for line in body.splitlines() if line.strip()]
        for row in rows:
            row["user_login"] = validate_login(row["user_login"])
    except (ValueError, KeyError, TypeError):
        raise ValueError("ClickHouse returned invalid rows; check database/table and HTTP configuration") from None
    return rows


def paginated(api, endpoint, params, limit, max_pages, *, schedule=False):
    """Keep already collected pages on failure, and expose every truncation boundary."""
    result = {"data": [], "pages": 0, "has_more": False, "next_cursor": None}
    seen_cursors = set()
    seen_ids = set()
    for _ in range(max_pages):
        page_size = min(25 if schedule else 100, limit - len(result["data"])) if limit else 100
        query = {**params, "first": page_size}
        if result["next_cursor"]:
            query["after"] = result["next_cursor"]
        try:
            response = api.get(endpoint, query)
        except APIError as exc:
            if schedule and exc.status == 404 and result["pages"] == 0:
                result["availability"] = "no_schedule"
                return result
            result["error"] = str(exc)
            return result
        data = response["data"]
        if schedule:
            result["schedule"] = {key: value for key, value in data.items() if key != "segments"}
            data = data.get("segments") or []
        for item in data:
            item_id = item.get("id")
            if item_id is None or item_id not in seen_ids:
                result["data"].append(item)
                seen_ids.add(item_id)
        result["pages"] += 1
        cursor = response.get("pagination", {}).get("cursor")
        result["next_cursor"] = cursor or None
        result["has_more"] = bool(cursor)
        if not cursor or (limit and len(result["data"]) >= limit):
            return result
        if cursor in seen_cursors:
            result["error"] = "API repeated a pagination cursor"
            return result
        seen_cursors.add(cursor)
    result["stopped_at_page_limit"] = result["has_more"]
    return result


def image_url(url):
    # VOD thumbnails use %{width}; live thumbnails and game art use {width}.
    for key, value in (("width", "1280"), ("height", "720")):
        url = url.replace("%{" + key + "}", value).replace("{" + key + "}", value)
    return url


def image_candidates(record):
    urls = set()
    def visit(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if isinstance(child, str) and (key.endswith("image_url") or key in ("thumbnail_url", "box_art_url", "banner")):
                    if child.startswith("https://"):
                        urls.add(image_url(child))
                elif isinstance(child, (dict, list)):
                    visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)
    # Emote/badge/cheermote variants stay in the JSON; downloading all sizes is wasteful.
    for key in ("user", "stream", "videos", "clips", "teams", "games", "source"):
        visit(record.get(key))
    return sorted(urls)


def download_images(http, record, output):
    assets = output / "images"
    assets.mkdir(exist_ok=True)
    record["downloaded_images"] = []
    for url in record["image_urls"]:
        name = hashlib.sha256(url.encode()).hexdigest()
        existing = next(assets.glob(name + ".*"), None)
        try:
            if existing is None or existing.suffix == ".tmp":
                body, content_type = http.request(url, label="Image download", image=True)
                extension = {"image/jpeg": ".jpg", "image/png": ".png", "image/webp": ".webp", "image/gif": ".gif"}.get(content_type, ".img")
                existing = assets / (name + extension)
                temporary = assets / (name + ".tmp")
                temporary.write_bytes(body)
                temporary.replace(existing)
            record["downloaded_images"].append({"url": url, "path": str(existing.relative_to(output))})
        except (APIError, OSError) as exc:
            record["errors"].append({"section": "images", "message": str(exc), "url": url})


def collect_profile(api, row, user, batch, options, output):
    record = {
        "schema_version": 1, "requested_login": row["user_login"], "collected_at": now(),
        "source": row, "user": user, "errors": [], "follower_count": None,
    }
    if user is None:
        record["status"] = "not_found"
        return record
    user_id = user["id"]
    record["channel_url"] = "https://www.twitch.tv/" + user["login"]
    record["bio_links"] = sorted({url.rstrip(".,;!?)\u3002") for url in re.findall(r"https?://[^\s<>\"\]]+", user.get("description", ""))})
    for key in ("channel", "stream"):
        values, error = batch[key]
        record[key] = values.get(user_id)
        if error:
            record["errors"].append({"section": key, "message": error})
    record["is_live"] = None if batch["stream"][1] else bool(record["stream"])
    if record["channel"] is None and not batch["channel"][1]:
        record["errors"].append({"section": "channel", "message": "Channel not returned by Twitch"})
    endpoints = {
        "teams": ("teams/channel", "broadcaster_id"),
        "emotes": ("chat/emotes", "broadcaster_id"),
        "badges": ("chat/badges", "broadcaster_id"),
        "chat_settings": ("chat/settings", "broadcaster_id"),
        "extensions": ("users/extensions", "user_id"),
        "chat_color": ("chat/color", "user_id"),
        "cheermotes": ("bits/cheermotes", "broadcaster_id"),
    }
    for section in options["sections"]:
        try:
            if section == "followers":
                response = api.get("channels/followers", {"broadcaster_id": user_id, "first": 1})
                if not isinstance(response.get("total"), int):
                    raise APIError("followers", 0, "missing total follower count")
                record["follower_count"] = response["total"]
            elif section in ("videos", "clips", "schedule"):
                params = {"user_id" if section == "videos" else "broadcaster_id": user_id}
                if section == "videos":
                    params.update(sort="time", period="all", type="all")
                record[section] = paginated(
                    api, section, params, options[section + "_limit"], options["max_pages"],
                    schedule=section == "schedule",
                )
                if record[section].get("error"):
                    record["errors"].append({"section": section, "message": record[section]["error"]})
            elif section == "games":
                game_id = (record.get("channel") or {}).get("game_id")
                record[section] = api.get("games", {"id": game_id}) if game_id else {"data": []}
            else:
                endpoint, parameter = endpoints[section]
                record[section] = api.get(endpoint, {parameter: user_id})
                if section == "cheermotes":
                    record[section]["data"] = [item for item in record[section]["data"] if item.get("type") == "channel_custom"]
        except APIError as exc:
            record["errors"].append({"section": section, "message": str(exc)})
    record["image_urls"] = image_candidates(record)
    if options["download_images"]:
        download_images(api.http, record, output)
    record["status"] = "partial" if record["errors"] else "ok"
    return record


def summary(record):
    user = record.get("user") or {}
    channel = record.get("channel") or {}
    stream = record.get("stream") or {}
    source = record.get("source") or {}
    result = {key: record.get(key) for key in CSV_FIELDS}
    for key in ("id", "login", "display_name", "description", "broadcaster_type", "created_at", "profile_image_url", "offline_image_url"):
        result[key] = user.get(key)
    result.update(
        live_thumbnail_url=image_url(stream.get("thumbnail_url", "")),
        viewer_count=stream.get("viewer_count"), title=channel.get("title"),
        game_name=channel.get("game_name"), language=channel.get("broadcaster_language"),
        tags=channel.get("tags"),
    )
    for key in ("observed_first_at", "observed_last_at", "observed_peak_viewers"):
        result[key] = source.get(key)
    for key, value in result.items():
        if isinstance(value, (list, dict)):
            result[key] = json.dumps(value, ensure_ascii=False)
        elif isinstance(value, str) and value.lstrip().startswith(("=", "+", "-", "@")):
            result[key] = "'" + value  # Keep untrusted bios/titles inert in spreadsheet apps.
    return result


def export(db, output):
    json_path = output / "profiles.jsonl.tmp"
    csv_path = output / "profiles.csv.tmp"
    with json_path.open("w", encoding="utf-8") as json_file, csv_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for (payload,) in db.execute("SELECT profile FROM profiles WHERE profile IS NOT NULL ORDER BY login"):
            json_file.write(payload + "\n")
            writer.writerow(summary(json.loads(payload)))
    json_path.replace(output / "profiles.jsonl")
    csv_path.replace(output / "profiles.csv")
    counts = dict(db.execute("SELECT status, count(*) FROM profiles GROUP BY status"))
    manifest = json.loads(db.execute("SELECT value FROM metadata WHERE key = 'run'").fetchone()[0])
    manifest.update(exported_at=now(), counts=counts)
    temporary = output / "manifest.json.tmp"
    temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(output / "manifest.json")
    return counts


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--logins", nargs="+", help="explicit login names; skip ClickHouse")
    source.add_argument("--logins-file", type=Path, help="one login per line, or CSV with user_login/login header")
    parser.add_argument("--clickhouse-url", help="HTTP(S) endpoint; defaults to CLICKHOUSE_HTTP_URL or DB_HOST:8123")
    parser.add_argument("--database", help="defaults to CLICKHOUSE_DATABASE, DB_NAME, then default")
    parser.add_argument("--table", default="stream_snapshots")
    parser.add_argument("--language", default="fr")
    parser.add_argument("--history", action="store_true", help="also aggregate the matching language's stored stream snapshots")
    parser.add_argument("--limit", type=int, default=0, help="maximum selected logins; 0 selects all")
    parser.add_argument("--output", type=Path, default=Path("data/streamer_profiles") / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
    parser.add_argument("--resume", action="store_true", help="reuse the output's saved selection/settings; retry pending/partial profiles")
    parser.add_argument("--export-only", action="store_true", help="rebuild JSONL/CSV from --output, without network access")
    parser.add_argument("--dry-run", action="store_true", help="select and count logins without contacting Twitch or writing output")
    parser.add_argument("--sections", default=",".join(SECTIONS), help="comma-separated extra sections; use followers for a quick core export")
    parser.add_argument("--videos-limit", type=int, default=20, help="recent videos per channel; 0 follows all available pages")
    parser.add_argument("--clips-limit", type=int, default=20, help="top clips per channel; 0 follows all available pages (Twitch caps results)")
    parser.add_argument("--schedule-limit", type=int, default=20, help="upcoming schedule segments per channel (must be positive)")
    parser.add_argument("--max-pages", type=int, default=100, help="pagination safety limit per section and channel")
    parser.add_argument("--download-images", action="store_true", help="download profile/banner/stream/video/clip/team/game images")
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--request-interval", type=float, default=0.12, help="minimum seconds between HTTP requests; also obey Twitch rate limits")
    args = parser.parse_args(argv)
    if min(args.limit, args.videos_limit, args.clips_limit, args.retries, args.request_interval) < 0 or min(args.schedule_limit, args.max_pages, args.timeout) <= 0:
        parser.error("limits/retries/interval must be nonnegative; schedule-limit/max-pages/timeout must be positive")
    args.sections = list(dict.fromkeys(filter(None, args.sections.split(","))))
    if set(args.sections) - set(SECTIONS):
        parser.error("unknown section; choose from " + ",".join(SECTIONS))
    if args.history and (args.logins or args.logins_file):
        parser.error("--history requires ClickHouse selection")
    if args.dry_run and (args.resume or args.export_only):
        parser.error("--dry-run cannot be combined with --resume or --export-only")
    return args


def run(args):
    checkpoint = args.output / "checkpoint.sqlite3"
    if args.export_only:
        if not checkpoint.is_file():
            raise ValueError("No checkpoint found in --output")
        with closing(sqlite3.connect(checkpoint)) as db:
            counts = export(db, args.output)
        log(f"Exported {counts} to {args.output}")
        return 0
    env = load_env(args.env_file)
    http = HTTP(args.timeout, args.retries, args.request_interval)
    if args.dry_run:
        rows = source_rows(args, env, http)
        print(json.dumps({"selected_count": len(rows), "sample_logins": [row["user_login"] for row in rows[:10]]}, indent=2))
        return 0
    if args.resume:
        if not checkpoint.is_file():
            raise ValueError("No checkpoint found; start a new export without --resume")
    elif args.output.exists() and any(args.output.iterdir()):
        raise ValueError("Output directory is not empty; use --resume or choose a new --output")
    options = {key: getattr(args, key) for key in ("sections", "videos_limit", "clips_limit", "schedule_limit", "max_pages", "download_images")}
    rows = None if args.resume else source_rows(args, env, http)
    args.output.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(checkpoint)) as db:
        if not args.resume:
            db.executescript("""
                CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE profiles (
                    login TEXT PRIMARY KEY, source TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending', profile TEXT
                );
            """)
            manifest = {"schema_version": 1, "started_at": now(), "options": options,
                        "selection": {"kind": "logins" if args.logins or args.logins_file else "clickhouse",
                                      "language": args.language, "history": args.history, "limit": args.limit}}
            with db:
                db.execute("INSERT INTO metadata VALUES ('run', ?)", (json.dumps(manifest),))
                db.executemany("INSERT INTO profiles (login, source) VALUES (?, ?)",
                               [(row["user_login"], json.dumps(row, ensure_ascii=False)) for row in rows])
        else:
            manifest = json.loads(db.execute("SELECT value FROM metadata WHERE key = 'run'").fetchone()[0])
            options = manifest["options"]
            log("Resuming with the original selection and collection options")
        pending = db.execute("SELECT login, source FROM profiles WHERE status IN ('pending', 'partial') ORDER BY login").fetchall()
        log(f"{len(pending)} profiles to collect; output: {args.output}")
        code = 0
        try:
            if pending:
                api = Twitch(http, env)
                api.authenticate()
            for offset in range(0, len(pending), 100):
                group = pending[offset:offset + 100]
                # A failed lookup must never be mistaken for 100 nonexistent accounts.
                response = api.get("users", {"login": [login for login, _ in group]})
                users = {user["login"].lower(): user for user in response["data"]}
                ids = [user["id"] for user in users.values()]
                batch = {}
                for key, endpoint, parameter, id_key in (
                    ("channel", "channels", "broadcaster_id", "broadcaster_id"),
                    ("stream", "streams", "user_id", "user_id"),
                ):
                    try:
                        params = {parameter: ids}
                        if key == "stream":
                            params["first"] = 100
                        response = api.get(endpoint, params) if ids else {"data": []}
                        batch[key] = ({item[id_key]: item for item in response["data"]}, None)
                    except APIError as exc:
                        batch[key] = ({}, str(exc))
                for index, (login, payload) in enumerate(group, offset + 1):
                    record = collect_profile(api, json.loads(payload), users.get(login), batch, options, args.output)
                    with db:
                        db.execute("UPDATE profiles SET status = ?, profile = ? WHERE login = ?",
                                   (record["status"], json.dumps(record, ensure_ascii=False), login))
                    log(f"[{index}/{len(pending)}] {login}: {record['status']} ({len(record['errors'])} errors)")
                export(db, args.output)
        except KeyboardInterrupt:
            log("Interrupted; completed profiles are saved. Use --resume with the same --output.")
            code = 130
        finally:
            counts = export(db, args.output)
            log(f"Saved {counts} to {args.output}")
        return code or (2 if counts.get("partial") or counts.get("pending") else 0)


def main(argv=None):
    try:
        return run(arguments(argv))
    except (APIError, ValueError, OSError, sqlite3.Error) as exc:
        log(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
