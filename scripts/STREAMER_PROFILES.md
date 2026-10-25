# Standalone Twitch profile export

`extract_streamer_profiles.py` runs with **Python 3.10+ and no packages to install**.
It reads ClickHouse and Twitch, then writes local files. It does not change the
scraper or write to your source database. You can copy this one script elsewhere.

## Run

From the repository root, start with a small export:

```bash
python3 scripts/extract_streamer_profiles.py \
  --database default --limit 10 \
  --output data/french-streamers-sample
```

Collect all logins matching your query, with additional snapshot statistics:

```bash
python3 scripts/extract_streamer_profiles.py \
  --database default --history \
  --output data/french-streamers
```

The selection is equivalent to:

```sql
SELECT DISTINCT user_login
FROM default.stream_snapshots
WHERE language = 'fr'
```

Logins are lowercased, deduplicated, and sorted. Empty names are excluded. `fr`
means observed streaming in French; it does not establish French nationality.
`--language`, `--table`, and `--database` change the selection.

## Credentials and ClickHouse connection

The script reads `.env` in the current directory, or `--env-file /path/to/.env`.
Exported environment variables take precedence. Dotenv assignments are parsed as
data: shell commands and variable interpolation are not evaluated.

- Twitch: existing `CLIENT_ID` and `CLIENT_SECRET`. The script obtains and renews
  an app token automatically. `TWITCH_CLIENT_ID` / `TWITCH_CLIENT_SECRET` are also
  accepted. An existing `TWITCH_ACCESS_TOKEN` may be supplied instead of a secret;
  it must belong to the client ID and must be replaced when it expires.
- ClickHouse: existing `DB_HOST`, `DB_USER`, `DB_PASSWORD`, and `DB_NAME`.
  `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, and `CLICKHOUSE_DATABASE` override them.
- The connection uses **HTTP port 8123**, independently of the Go scraper's
  native `DB_PORT=9000`. Set `CLICKHOUSE_HTTP_URL` or `--clickhouse-url` for another
  address, HTTPS (often port 8443), or a custom port. `CLICKHOUSE_HTTP_PORT` also
  changes the default port. TLS certificates are verified.

For example:

```bash
python3 scripts/extract_streamer_profiles.py \
  --clickhouse-url https://your-clickhouse-host:8443 \
  --database default --limit 10 --dry-run
```

`--dry-run` executes only the read-only selection and prints a count and sample.
The ClickHouse account needs permission to select from `stream_snapshots`.
If HTTP access is unavailable, export the query's results to a CSV with a
`user_login` header, or a text file with one login per line:

```bash
python3 scripts/extract_streamer_profiles.py \
  --logins-file french_logins.csv --output data/french-streamers

# Also useful for testing without ClickHouse:
python3 scripts/extract_streamer_profiles.py \
  --logins squeezie zerator --output data/twitch-sample
```

## Collected information

All extra sections below are enabled by default. Complete returned objects are
stored in JSON, including fields that aren't flattened into the CSV.

| Section | Information |
| --- | --- |
| User | ID, login, display name, short bio, creation date, account and broadcaster type, avatar and offline image |
| Channel | Current/last title and category, language, tags, content labels, branded-content flag |
| Live stream | Online/offline status, viewers, start time, title, category, tags, maturity flag, thumbnail |
| Followers | Total follower count |
| Videos | Recent public VOD/upload/highlight metadata, descriptions, duration, views, URLs and thumbnails |
| Clips | Top clips by views, titles, dates, creator credit, video references, URLs and thumbnails |
| Schedule | Upcoming segments and vacation information, when published |
| Teams | Membership, team descriptions, logos and banners |
| Emotes, badges, cheermotes | Channel emotes and badges, custom cheermotes, image variants and metadata |
| Chat | Public chat settings and the broadcaster's chat color |
| Extensions | Public active extension configuration |
| Games | Current/last category metadata and box art |
| Derived fields | Channel URL, explicit HTTP(S) links in the bio, resolved image URLs |
| Optional history | First/last observation, snapshot and distinct-stream counts, peak and sample-mean viewers, observed categories, last thumbnail |

The history aggregates use only snapshots matching the selected language. They
describe your observations, not lifetime audience figures or a time-weighted
average. They do not estimate hours streamed. `--history` scans and aggregates
matching rows, so it can be expensive on a large table.

The API fields and authentication rules are documented in the
[Twitch reference](https://dev.twitch.tv/docs/api/reference/). The script validates
tokens and follows Twitch's
[rate-limit headers](https://dev.twitch.tv/docs/api/guide/#twitch-rate-limits).

## Coverage and collection size

By default, each channel gets up to **20 recent videos, 20 top clips, and 20
upcoming schedule segments**. Pagination metadata (`pages`, `has_more`,
`next_cursor`, and errors) makes limits visible. These are snapshots of what the
API returns, not an archival guarantee.

```bash
# Broader video/clip coverage, 100 upcoming schedule segments, image files:
python3 scripts/extract_streamer_profiles.py \
  --database default --history \
  --videos-limit 0 --clips-limit 0 --schedule-limit 100 \
  --download-images --output data/french-streamers-expanded
```

Zero follows available video/clip pages up to `--max-pages` (default 100 per
section per channel). Schedule limits must stay positive because recurring
schedules can keep generating future segments. Twitch limits clip pagination to
approximately 1,000 results for one query; zero does not remove that API limit.

The full collection uses roughly 12 calls per channel plus batched core lookups;
extra pages and images add requests. Large exports can take hours. For a fast
profile/bio/images/follower-count export, use `--sections followers`. Choose any
subset of `followers,videos,clips,schedule,teams,emotes,badges,chat_settings,extensions,chat_color,cheermotes,games`.
User, channel, and live-stream lookups always run.

Image URLs are always saved. `--download-images` also stores avatars, offline
images, selected stream/video/clip thumbnails, team art and category art under
`images/`, deduplicated by URL. Emote/badge/cheermote size variants remain URLs in
the JSON. Missing/expired CDN images are reported individually; they do not
discard the profile. Individual downloads are capped at 10 MiB.

## Outputs and recovery

Each output directory contains:

- `profiles.jsonl`: one full JSON object per collected login, including section errors.
- `profiles.csv`: a flat summary for analysis. Arrays are JSON strings; unknown
  values are empty. Potential spreadsheet formulas are escaped in the CSV only.
- `checkpoint.sqlite3`: durable selection and per-profile results, committed
  after each profile.
- `manifest.json`: collection options, timestamps and status counts.
- `images/`: optional downloads with source URL/path mappings in each profile.

Exports refresh after every batch of 100 and on normal exit or Ctrl-C. A hard
kill can leave JSONL/CSV behind the checkpoint; rebuild them without network:

```bash
python3 scripts/extract_streamer_profiles.py \
  --output data/french-streamers --export-only
```

Resume interruptions or retry partially failed profiles:

```bash
python3 scripts/extract_streamer_profiles.py \
  --output data/french-streamers --resume
```

Resume uses the **original selection and collection options**, without querying
ClickHouse again. Successful and not-found profiles are skipped; pending and
partial profiles are collected again. A retry replaces that profile's previous
attempt. Use a new output directory to refresh successful profiles, select newly
observed streamers, change sections/limits, or add image downloads. Run only one
extractor at a time per output directory.

Statuses are `ok`, `partial`, `not_found`, and `pending` (manifest/checkpoint only).
Not-found means the login lookup returned no account; it may be renamed or
unavailable. A failed lookup does not mark accounts not-found. Failed follower
requests stay `null`, not zero; failed stream lookups stay unknown, not offline.
A missing schedule is normal absence. Batch core data is fetched before the
per-channel sections, so the fields are not all measured at the same instant.

Exit codes: `0` finished, `2` partial/pending results, `1` fatal/configuration
failure, `130` interrupted. HTTP 429/5xx and network failures are retried with
backoff. No tokens or passwords are written to the export.

## What this cannot provide

The short bio is available; full custom About panels, their artwork, and a
complete social-links list are not exposed through Helix. `bio_links` includes
only links explicitly present in the short bio. The script does not use Twitch's
undocumented website GraphQL endpoints.

Private email addresses, revenue, complete subscriber counts, and follower
identities require permissions beyond this public-profile collection. Follower
**totals** work with an app token (verified in the live smoke test).
The raw user `view_count` is deprecated and must not be interpreted as lifetime
views. The channel `delay` field can be zero because its real value is restricted.

## Verify locally

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s scripts -p 'test_extract_streamer_profiles.py' -v
```

The tests use simulated responses and temporary directories; they do not contact
Twitch or ClickHouse. Live validation should use a small `--limit` or `--logins`
sample before starting a large export.
