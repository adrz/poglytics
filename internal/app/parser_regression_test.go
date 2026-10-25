package app

import (
	"testing"
	"time"
)

func TestTaggedEventPayloads(t *testing.T) {
	for _, tt := range []struct {
		name, raw, kind, message, target string
	}{
		{"timeout", "@ban-duration=60 :tmi.twitch.tv CLEARCHAT #room :viewer\r\n", "timeout", "", "viewer"},
		{"delete", "@login=viewer :tmi.twitch.tv CLEARMSG #room :deleted text\r\n", "delete_message", "deleted text", ""},
		{"subscription", "@msg-id=sub :tmi.twitch.tv USERNOTICE #room : hello  \r\n", "subscription", " hello  ", ""},
		{"subscription without text", "@msg-id=sub :tmi.twitch.tv USERNOTICE #room\r\n", "subscription", "", ""},
		{"notice", "@msg-id=msg_ratelimit :tmi.twitch.tv NOTICE #room :Slow down\r\n", "notice", "Slow down", ""},
		{"host", "@example=value :tmi.twitch.tv HOSTTARGET #room :destination 42\r\n", "host_target", "", "destination"},
		{"clear", "@room-id=123 :tmi.twitch.tv CLEARCHAT #room\r\n", "clear_chat", "", ""},
		{"channel in tag", "@reply-parent-msg-body=#room :viewer!viewer@host PRIVMSG #room :actual text\r\n", "text_message", "actual text", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := (&IRCConnection{}).parseIRCMessage(tt.raw)
			if got == nil || got.MessageType != tt.kind || got.Message != tt.message || got.TargetUser != tt.target {
				t.Fatalf("unexpected parsed event: %+v", got)
			}
			if got.RawMessage != tt.raw {
				t.Fatal("raw message was modified")
			}
		})
	}
}

func TestIRCTagEscapesAreDecodedOnce(t *testing.T) {
	got := parseTags(`@literal=\\s;unknown=\x;trailing=end\;flag;empty=`)
	for key, want := range map[string]string{"literal": `\s`, "unknown": "x", "trailing": "end", "flag": "", "empty": ""} {
		if value, ok := got[key]; !ok || value != want {
			t.Errorf("tag %s = %q (present=%v), want %q", key, value, ok, want)
		}
	}
}

func TestIRCEventUsesServerTimestamp(t *testing.T) {
	msg := (&IRCConnection{}).parseIRCMessage("@tmi-sent-ts=1642720582342 :viewer!viewer@host PRIVMSG #room :hello\r\n")
	if want := time.UnixMilli(1642720582342); msg == nil || !msg.Timestamp.Equal(want) {
		t.Fatalf("expected server timestamp, got %+v", msg)
	}
}
