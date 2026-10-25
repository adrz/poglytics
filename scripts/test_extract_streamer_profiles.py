"""Offline regression checks for profile collection, recovery, and API boundaries."""

import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
import urllib.error
import urllib.parse

import extract_streamer_profiles as profiles


class ExtractorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_dotenv_is_data_and_exported_values_win(self):
        env = self.root / "config.env"
        env.write_text("DB_NAME=first\nDB_NAME=second\nCLIENT_SECRET='a#b c'\n"
                       "export CLIENT_ID=from_file\nLITERAL=$(do-not-execute) # comment\n")
        with patch.dict(os.environ, {"CLIENT_ID": "from_environment"}, clear=True):
            result = profiles.load_env(env)
        self.assertEqual(result["DB_NAME"], "second")
        self.assertEqual(result["CLIENT_SECRET"], "a#b c")
        self.assertEqual(result["CLIENT_ID"], "from_environment")
        self.assertEqual(result["LITERAL"], "$(do-not-execute)")

    def test_clickhouse_selection_is_readonly_parameterized_and_uses_http(self):
        args = profiles.arguments(["--database", "default", "--limit", "3", "--history"])
        http = Mock()
        http.request.return_value = b'{"user_login":"ALPHA","observed_peak_viewers":42}\n'
        rows = profiles.source_rows(args, {"DB_HOST": "localhost", "DB_PORT": "9000", "DB_PASSWORD": "secret"}, http)
        url = http.request.call_args.args[0]
        kwargs = http.request.call_args.kwargs
        self.assertEqual(urllib.parse.urlsplit(url).port, 8123)
        self.assertEqual(urllib.parse.parse_qs(urllib.parse.urlsplit(url).query), {"readonly": ["1"], "param_language": ["fr"]})
        self.assertIn(b"`default`.`stream_snapshots`", kwargs["data"])
        self.assertIn(b"language = {language:String}", kwargs["data"])
        self.assertIn(b"LIMIT 3 FORMAT JSONEachRow", kwargs["data"])
        self.assertNotIn("secret", url)
        self.assertEqual(rows[0]["user_login"], "alpha")
        args.table = "stream_snapshots; DROP TABLE x"
        with self.assertRaises(ValueError):
            profiles.source_rows(args, {}, http)

    def test_pagination_keeps_pages_when_later_request_fails(self):
        api = Mock()
        api.get.side_effect = [
            {"data": [{"id": "1"}], "pagination": {"cursor": "next"}},
            profiles.APIError("videos", 503),
        ]
        result = profiles.paginated(api, "videos", {"user_id": "1"}, 0, 100)
        self.assertEqual(result["data"], [{"id": "1"}])
        self.assertTrue(result["has_more"])
        self.assertIn("503", result["error"])
        self.assertEqual(api.get.call_args.args[1]["after"], "next")

    def test_pagination_limits_and_repeated_cursor(self):
        api = Mock()
        api.get.return_value = {"data": [{"id": "1"}], "pagination": {"cursor": "same"}}
        result = profiles.paginated(api, "clips", {"broadcaster_id": "1"}, 0, 10)
        self.assertEqual(len(result["data"]), 1)
        self.assertIn("repeated", result["error"])
        api.get.reset_mock()
        result = profiles.paginated(api, "clips", {"broadcaster_id": "1"}, 1, 10)
        self.assertEqual(api.get.call_count, 1)
        self.assertEqual(api.get.call_args.args[1]["first"], 1)
        self.assertTrue(result["has_more"])

    def test_schedule_uses_25_item_pages_and_404_is_absence(self):
        api = Mock()
        api.get.return_value = {"data": {"segments": [{"id": "1"}], "vacation": None}, "pagination": {}}
        result = profiles.paginated(api, "schedule", {"broadcaster_id": "1"}, 50, 10, schedule=True)
        self.assertEqual(api.get.call_args.args[1]["first"], 25)
        self.assertIn("vacation", result["schedule"])
        api.get.side_effect = profiles.APIError("schedule", 404)
        result = profiles.paginated(api, "schedule", {}, 20, 10, schedule=True)
        self.assertNotIn("error", result)
        self.assertEqual(result["availability"], "no_schedule")

    def test_rate_limit_is_retried_without_disclosing_credentials(self):
        http = profiles.HTTP(retries=1, interval=0)
        response = Mock()
        response.headers = {}
        response.read.return_value = b'{"data": []}'
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        rejected = urllib.error.HTTPError("https://example.invalid/private", 429, "limited", {"Ratelimit-Reset": "110"}, io.BytesIO(b"secret"))
        http.opener = Mock()
        http.opener.open.side_effect = [rejected, response]
        with patch.object(profiles.time, "time", return_value=100), patch.object(profiles.time, "sleep") as sleep, patch.object(profiles, "log") as log:
            self.assertEqual(http.json("https://example.invalid/", label="Twitch users"), {"data": []})
        self.assertEqual(sleep.call_args_list[-1].args[0], 11)
        self.assertNotIn("secret", str(log.call_args_list))
        self.assertEqual(http.opener.open.call_count, 2)

    def test_app_token_refreshes_once_on_401(self):
        http = Mock()
        def respond(url, **kwargs):
            if url.endswith("token"):
                return {"access_token": "secret-token", "expires_in": 3600}
            if url.endswith("validate"):
                return {"client_id": "client", "expires_in": 3600}
            if respond.reject:
                respond.reject = False
                raise profiles.APIError("users", 401)
            return {"data": []}
        respond.reject = True
        http.json.side_effect = respond
        api = profiles.Twitch(http, {"CLIENT_ID": "client", "CLIENT_SECRET": "secret"})
        self.assertEqual(api.get("users", {"login": "alpha"}), {"data": []})
        token_calls = [call for call in http.json.call_args_list if call.args[0].endswith("token")]
        self.assertEqual(len(token_calls), 2)
        self.assertNotIn("secret", token_calls[0].args[0])

    def test_failed_follower_and_stream_requests_are_not_zero_or_offline(self):
        api = Mock()
        api.get.side_effect = profiles.APIError("followers", 403)
        record = profiles.collect_profile(
            api, {"user_login": "alpha"}, {"id": "1", "login": "alpha", "description": "À bientôt https://example.com"},
            {"channel": ({"1": {}}, None), "stream": ({}, "HTTP 503")},
            {"sections": ["followers"], "download_images": False}, self.root,
        )
        self.assertIsNone(record["follower_count"])
        self.assertIsNone(record["is_live"])
        self.assertEqual(record["status"], "partial")
        self.assertEqual(record["bio_links"], ["https://example.com"])

    def test_image_urls_and_csv_are_usable(self):
        record = {"user": {"description": "=HYPERLINK(\"bad\")", "profile_image_url": "https://example.com/avatar.png"},
                  "videos": {"data": [{"thumbnail_url": "https://example.com/%{width}x%{height}.jpg"}]},
                  "stream": {"thumbnail_url": "https://example.com/{width}x{height}.jpg"}, "errors": []}
        urls = profiles.image_candidates(record)
        self.assertEqual(urls, ["https://example.com/1280x720.jpg", "https://example.com/avatar.png"])
        self.assertTrue(profiles.summary(record)["description"].startswith("'="))
        self.assertTrue(record["user"]["description"].startswith("="))

    def test_interrupt_resume_and_export_without_database_or_auth(self):
        output = self.root / "output"
        args = profiles.arguments(["--logins", "alpha", "bravo", "missing", "--sections", "followers", "--output", str(output)])
        fake = FakeTwitch()
        fake.interrupt = True
        with patch.object(profiles, "Twitch", return_value=fake), patch.object(profiles, "log"):
            self.assertEqual(profiles.run(args), 130)
        first = json.loads((output / "manifest.json").read_text())
        self.assertEqual(first["counts"], {"ok": 1, "pending": 2})
        fake.interrupt = False
        fake.requested_logins.clear()
        resume = profiles.arguments(["--resume", "--output", str(output)])
        with patch.object(profiles, "Twitch", return_value=fake), patch.object(profiles, "source_rows", side_effect=AssertionError("database contacted during resume")), patch.object(profiles, "log"):
            self.assertEqual(profiles.run(resume), 0)
        self.assertEqual(fake.requested_logins, ["bravo", "missing"])
        records = [json.loads(line) for line in (output / "profiles.jsonl").read_text().splitlines()]
        self.assertEqual(len(records), 3)
        self.assertEqual(records[0]["follower_count"], 0)
        self.assertEqual(records[2]["status"], "not_found")
        export_args = profiles.arguments(["--export-only", "--output", str(output)])
        with patch.object(profiles, "load_env", side_effect=AssertionError("credentials loaded")), patch.object(profiles, "log"):
            self.assertEqual(profiles.run(export_args), 0)

    def test_batches_do_not_classify_streams_beyond_default_page_as_offline(self):
        output = self.root / "batch"
        logins = [f"user{i:03}" for i in range(101)]
        args = profiles.arguments(["--logins", *logins, "--sections", "followers", "--output", str(output)])
        fake = FakeTwitch()
        with patch.object(profiles, "Twitch", return_value=fake), patch.object(profiles, "log"):
            self.assertEqual(profiles.run(args), 0)
        records = [json.loads(line) for line in (output / "profiles.jsonl").read_text().splitlines()]
        self.assertEqual(len(records), 101)
        self.assertTrue(all(record["is_live"] for record in records))
        self.assertEqual(fake.batch_sizes, [100, 1])

    def test_user_lookup_failure_preserves_pending_rows(self):
        output = self.root / "failed"
        args = profiles.arguments(["--logins", "alpha", "--output", str(output)])
        fake = FakeTwitch()
        with patch.object(profiles, "Twitch", return_value=fake), patch.object(fake, "get", side_effect=profiles.APIError("users", 503)), patch.object(profiles, "log"):
            with self.assertRaises(profiles.APIError):
                profiles.run(args)
        self.assertEqual((output / "profiles.jsonl").read_text(), "")
        self.assertEqual(json.loads((output / "manifest.json").read_text())["counts"], {"pending": 1})


class FakeTwitch:
    def __init__(self):
        self.interrupt = False
        self.requested_logins = []
        self.batch_sizes = []

    def authenticate(self):
        pass

    def get(self, endpoint, params):
        if endpoint == "users":
            self.requested_logins.extend(params["login"])
            self.batch_sizes.append(len(params["login"]))
            return {"data": [{"id": login, "login": login, "description": "Français"} for login in params["login"] if login != "missing"]}
        if endpoint == "channels":
            return {"data": [{"broadcaster_id": uid} for uid in params["broadcaster_id"]]}
        if endpoint == "streams":
            return {"data": [{"user_id": uid} for uid in params["user_id"][:params.get("first", 20)]]}
        if endpoint == "channels/followers":
            if self.interrupt and params["broadcaster_id"] == "bravo":
                raise KeyboardInterrupt
            return {"total": 0, "data": []}
        raise AssertionError("Unexpected endpoint: " + endpoint)


if __name__ == "__main__":
    unittest.main()
