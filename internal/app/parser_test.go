package app

import (
	"reflect"
	"testing"
)

func TestParseTags(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected map[string]string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: map[string]string{},
		},
		{
			name:  "simple tags",
			input: "@key1=value1;key2=value2",
			expected: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		},
		{
			name:  "tags with @ prefix",
			input: "@key1=value1;key2=value2",
			expected: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		},
		{
			name:  "escaped values",
			input: "@key1=value\\swith\\:semicolon;key2=value\\\\with\\\\backslash",
			expected: map[string]string{
				"key1": "value with;semicolon",
				"key2": "value\\with\\backslash",
			},
		},
		{
			name:  "empty value",
			input: "@key1=;key2=value2",
			expected: map[string]string{
				"key1": "",
				"key2": "value2",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseTags(tt.input)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("parseTags() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestParsePRIVMSG(t *testing.T) {
	tests := []struct {
		name       string
		rawMessage string
		tags       map[string]string
		parts      []string
		commandIdx int
		expectNil  bool
		expected   *ChatMessage
	}{
		{
			name:       "invalid parts length",
			rawMessage: ":user!user@user.tmi.twitch.tv PRIVMSG #channel",
			tags:       map[string]string{},
			parts:      []string{":user!user@user.tmi.twitch.tv", "PRIVMSG", "#channel"},
			commandIdx: 1,
			expectNil:  true,
		},
		{
			name:       "valid PRIVMSG",
			rawMessage: ":user!user@user.tmi.twitch.tv PRIVMSG #channel :Hello world!",
			tags: map[string]string{
				"user-id":      "12345",
				"display-name": "UserName",
				"color":        "#FF0000",
				"badges":       "subscriber/1,moderator/1",
				"bits":         "100",
			},
			parts:      []string{":user!user@user.tmi.twitch.tv", "PRIVMSG", "#channel", ":Hello world!"},
			commandIdx: 1,
			expectNil:  false,
			expected: &ChatMessage{
				MessageType: "text_message",
				Channel:     "#channel",
				Message:     "Hello world!",
				Nickname:    "user",
				UserID:      "12345",
				DisplayName: "UserName",
				Color:       "#FF0000",
				Badges:      []string{"subscriber/1", "moderator/1"},
				BitsAmount:  100,
				Tags: map[string]string{
					"user-id":      "12345",
					"display-name": "UserName",
					"color":        "#FF0000",
					"badges":       "subscriber/1,moderator/1",
					"bits":         "100",
				},
				RawMessage: ":user!user@user.tmi.twitch.tv PRIVMSG #channel :Hello world!",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parsePRIVMSG(tt.rawMessage, tt.tags, tt.parts, tt.commandIdx)
			if tt.expectNil {
				if result != nil {
					t.Errorf("parsePRIVMSG() expected nil, got %v", result)
				}
				return
			}

			if result == nil {
				t.Errorf("parsePRIVMSG() expected non-nil, got nil")
				return
			}

			// Compare fields, ignoring Timestamp
			if result.MessageType != tt.expected.MessageType ||
				result.Channel != tt.expected.Channel ||
				result.Message != tt.expected.Message ||
				result.Nickname != tt.expected.Nickname ||
				result.UserID != tt.expected.UserID ||
				result.DisplayName != tt.expected.DisplayName ||
				result.Color != tt.expected.Color ||
				!reflect.DeepEqual(result.Badges, tt.expected.Badges) ||
				result.BitsAmount != tt.expected.BitsAmount ||
				!reflect.DeepEqual(result.Tags, tt.expected.Tags) ||
				result.RawMessage != tt.expected.RawMessage {
				t.Errorf("parsePRIVMSG() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestParseCLEARCHAT(t *testing.T) {
	tests := []struct {
		name       string
		rawMessage string
		tags       map[string]string
		parts      []string
		commandIdx int
		expectNil  bool
		expected   *ChatMessage
	}{
		{
			name:       "invalid parts length",
			rawMessage: ":tmi.twitch.tv CLEARCHAT #channel",
			tags:       map[string]string{},
			parts:      []string{":tmi.twitch.tv", "CLEARCHAT"},
			commandIdx: 1,
			expectNil:  true,
		},
		{
			name:       "chat clear",
			rawMessage: ":tmi.twitch.tv CLEARCHAT #channel",
			tags:       map[string]string{},
			parts:      []string{":tmi.twitch.tv", "CLEARCHAT", "#channel"},
			commandIdx: 1,
			expectNil:  false,
			expected: &ChatMessage{
				MessageType: "ban",
				Channel:     "#channel",
				Tags:        map[string]string{},
				RawMessage:  ":tmi.twitch.tv CLEARCHAT #channel",
			},
		},
		{
			name:       "permanent ban",
			rawMessage: ":tmi.twitch.tv CLEARCHAT #channel :baduser",
			tags: map[string]string{
				"ban-reason": "Violation of rules",
			},
			parts:      []string{":tmi.twitch.tv", "CLEARCHAT", "#channel", ":baduser"},
			commandIdx: 1,
			expectNil:  false,
			expected: &ChatMessage{
				MessageType: "ban",
				Channel:     "#channel",
				TargetUser:  "baduser",
				BanReason:   "Violation of rules",
				Tags: map[string]string{
					"ban-reason": "Violation of rules",
				},
				RawMessage: ":tmi.twitch.tv CLEARCHAT #channel :baduser",
			},
		},
		{
			name:       "timeout",
			rawMessage: ":tmi.twitch.tv CLEARCHAT #channel :baduser",
			tags: map[string]string{
				"ban-duration": "600",
				"ban-reason":   "Spam",
			},
			parts:      []string{":tmi.twitch.tv", "CLEARCHAT", "#channel", ":baduser"},
			commandIdx: 1,
			expectNil:  false,
			expected: &ChatMessage{
				MessageType: "timeout",
				Channel:     "#channel",
				TargetUser:  "baduser",
				BanDuration: 600,
				BanReason:   "Spam",
				Tags: map[string]string{
					"ban-duration": "600",
					"ban-reason":   "Spam",
				},
				RawMessage: ":tmi.twitch.tv CLEARCHAT #channel :baduser",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseCLEARCHAT(tt.rawMessage, tt.tags, tt.parts, tt.commandIdx)
			if tt.expectNil {
				if result != nil {
					t.Errorf("parseCLEARCHAT() expected nil, got %v", result)
				}
				return
			}

			if result == nil {
				t.Errorf("parseCLEARCHAT() expected non-nil, got nil")
				return
			}

			// Compare fields, ignoring Timestamp
			if result.MessageType != tt.expected.MessageType ||
				result.Channel != tt.expected.Channel ||
				result.TargetUser != tt.expected.TargetUser ||
				result.BanDuration != tt.expected.BanDuration ||
				result.BanReason != tt.expected.BanReason ||
				!reflect.DeepEqual(result.Tags, tt.expected.Tags) ||
				result.RawMessage != tt.expected.RawMessage {
				t.Errorf("parseCLEARCHAT() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestParseCLEARMSG(t *testing.T) {
	tests := []struct {
		name       string
		rawMessage string
		tags       map[string]string
		parts      []string
		commandIdx int
		expectNil  bool
		expected   *ChatMessage
	}{
		{
			name:       "invalid parts length",
			rawMessage: ":tmi.twitch.tv CLEARMSG #channel",
			tags:       map[string]string{},
			parts:      []string{":tmi.twitch.tv", "CLEARMSG"},
			commandIdx: 1,
			expectNil:  true,
		},
		{
			name:       "valid CLEARMSG",
			rawMessage: ":tmi.twitch.tv CLEARMSG #channel :This message was deleted",
			tags: map[string]string{
				"target-msg-id": "abc-123-def",
				"login":         "baduser",
			},
			parts:      []string{":tmi.twitch.tv", "CLEARMSG", "#channel", ":This message was deleted"},
			commandIdx: 1,
			expectNil:  false,
			expected: &ChatMessage{
				MessageType:     "delete_message",
				Channel:         "#channel",
				Message:         "This message was deleted",
				TargetMessageID: "abc-123-def",
				Nickname:        "baduser",
				Tags: map[string]string{
					"target-msg-id": "abc-123-def",
					"login":         "baduser",
				},
				RawMessage: ":tmi.twitch.tv CLEARMSG #channel :This message was deleted",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseCLEARMSG(tt.rawMessage, tt.tags, tt.parts, tt.commandIdx)
			if tt.expectNil {
				if result != nil {
					t.Errorf("parseCLEARMSG() expected nil, got %v", result)
				}
				return
			}

			if result == nil {
				t.Errorf("parseCLEARMSG() expected non-nil, got nil")
				return
			}

			// Compare fields, ignoring Timestamp
			if result.MessageType != tt.expected.MessageType ||
				result.Channel != tt.expected.Channel ||
				result.Message != tt.expected.Message ||
				result.TargetMessageID != tt.expected.TargetMessageID ||
				result.Nickname != tt.expected.Nickname ||
				!reflect.DeepEqual(result.Tags, tt.expected.Tags) ||
				result.RawMessage != tt.expected.RawMessage {
				t.Errorf("parseCLEARMSG() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestParseUSERNOTICE(t *testing.T) {
	tests := []struct {
		name       string
		rawMessage string
		tags       map[string]string
		parts      []string
		commandIdx int
		expectNil  bool
		expected   *ChatMessage
	}{
		{
			name:       "invalid parts length",
			rawMessage: ":tmi.twitch.tv USERNOTICE #channel",
			tags:       map[string]string{},
			parts:      []string{":tmi.twitch.tv", "USERNOTICE"},
			commandIdx: 1,
			expectNil:  true,
		},
		{
			name:       "subscription",
			rawMessage: ":tmi.twitch.tv USERNOTICE #channel :Great stream!",
			tags: map[string]string{
				"user-id":                     "12345",
				"display-name":                "UserName",
				"system-msg":                  "UserName subscribed at Tier 1.",
				"msg-id":                      "sub",
				"msg-param-sub-plan":          "1000",
				"msg-param-sub-plan-name":     "Channel Subscription",
				"msg-param-cumulative-months": "3",
				"msg-param-months":            "1",
				"msg-param-streak-months":     "1",
				"login":                       "username",
			},
			parts:      []string{":tmi.twitch.tv", "USERNOTICE", "#channel", ":Great stream!"},
			commandIdx: 1,
			expectNil:  false,
			expected: &ChatMessage{
				MessageType:      "subscription",
				Channel:          "#channel",
				Message:          "Great stream!",
				UserID:           "12345",
				DisplayName:      "UserName",
				SystemMessage:    "UserName subscribed at Tier 1.",
				Nickname:         "username",
				SubPlan:          "1000",
				SubPlanName:      "Channel Subscription",
				CumulativeMonths: 3,
				Months:           1,
				StreakMonths:     1,
				Tags: map[string]string{
					"user-id":                     "12345",
					"display-name":                "UserName",
					"system-msg":                  "UserName subscribed at Tier 1.",
					"msg-id":                      "sub",
					"msg-param-sub-plan":          "1000",
					"msg-param-sub-plan-name":     "Channel Subscription",
					"msg-param-cumulative-months": "3",
					"msg-param-months":            "1",
					"msg-param-streak-months":     "1",
					"login":                       "username",
				},
				RawMessage: ":tmi.twitch.tv USERNOTICE #channel :Great stream!",
			},
		},
		{
			name:       "raid",
			rawMessage: ":tmi.twitch.tv USERNOTICE #channel",
			tags: map[string]string{
				"user-id":               "12345",
				"display-name":          "RaiderName",
				"system-msg":            "5 raiders from RaiderName have joined!",
				"msg-id":                "raid",
				"msg-param-login":       "raidername",
				"msg-param-viewerCount": "5",
				"login":                 "raidername",
			},
			parts:      []string{":tmi.twitch.tv", "USERNOTICE", "#channel"},
			commandIdx: 1,
			expectNil:  false,
			expected: &ChatMessage{
				MessageType:   "raid",
				Channel:       "#channel",
				UserID:        "12345",
				DisplayName:   "RaiderName",
				SystemMessage: "5 raiders from RaiderName have joined!",
				Nickname:      "raidername",
				RaiderName:    "raidername",
				ViewerCount:   5,
				Tags: map[string]string{
					"user-id":               "12345",
					"display-name":          "RaiderName",
					"system-msg":            "5 raiders from RaiderName have joined!",
					"msg-id":                "raid",
					"msg-param-login":       "raidername",
					"msg-param-viewerCount": "5",
					"login":                 "raidername",
				},
				RawMessage: ":tmi.twitch.tv USERNOTICE #channel",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseUSERNOTICE(tt.rawMessage, tt.tags, tt.parts, tt.commandIdx)
			if tt.expectNil {
				if result != nil {
					t.Errorf("parseUSERNOTICE() expected nil, got %v", result)
				}
				return
			}

			if result == nil {
				t.Errorf("parseUSERNOTICE() expected non-nil, got nil")
				return
			}

			// Compare fields, ignoring Timestamp
			if result.MessageType != tt.expected.MessageType ||
				result.Channel != tt.expected.Channel ||
				result.Message != tt.expected.Message ||
				result.UserID != tt.expected.UserID ||
				result.DisplayName != tt.expected.DisplayName ||
				result.SystemMessage != tt.expected.SystemMessage ||
				result.Nickname != tt.expected.Nickname ||
				result.SubPlan != tt.expected.SubPlan ||
				result.SubPlanName != tt.expected.SubPlanName ||
				result.CumulativeMonths != tt.expected.CumulativeMonths ||
				result.Months != tt.expected.Months ||
				result.StreakMonths != tt.expected.StreakMonths ||
				result.RaiderName != tt.expected.RaiderName ||
				result.ViewerCount != tt.expected.ViewerCount ||
				!reflect.DeepEqual(result.Tags, tt.expected.Tags) ||
				result.RawMessage != tt.expected.RawMessage {
				t.Errorf("parseUSERNOTICE() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestParseNOTICE(t *testing.T) {
	tests := []struct {
		name       string
		rawMessage string
		tags       map[string]string
		parts      []string
		commandIdx int
		expectNil  bool
		expected   *ChatMessage
	}{
		{
			name:       "invalid parts length",
			rawMessage: ":tmi.twitch.tv NOTICE #channel",
			tags:       map[string]string{},
			parts:      []string{":tmi.twitch.tv", "NOTICE"},
			commandIdx: 1,
			expectNil:  true,
		},
		{
			name:       "valid NOTICE",
			rawMessage: ":tmi.twitch.tv NOTICE #channel :This is a system notice",
			tags: map[string]string{
				"msg-id": "host_on",
			},
			parts:      []string{":tmi.twitch.tv", "NOTICE", "#channel", ":This is a system notice"},
			commandIdx: 1,
			expectNil:  false,
			expected: &ChatMessage{
				MessageType:     "notice",
				Channel:         "#channel",
				Message:         "This is a system notice",
				SystemMessage:   "This is a system notice",
				NoticeMessageID: "host_on",
				Tags: map[string]string{
					"msg-id": "host_on",
				},
				RawMessage: ":tmi.twitch.tv NOTICE #channel :This is a system notice",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseNOTICE(tt.rawMessage, tt.tags, tt.parts, tt.commandIdx)
			if tt.expectNil {
				if result != nil {
					t.Errorf("parseNOTICE() expected nil, got %v", result)
				}
				return
			}

			if result == nil {
				t.Errorf("parseNOTICE() expected non-nil, got nil")
				return
			}

			// Compare fields, ignoring Timestamp
			if result.MessageType != tt.expected.MessageType ||
				result.Channel != tt.expected.Channel ||
				result.Message != tt.expected.Message ||
				result.SystemMessage != tt.expected.SystemMessage ||
				result.NoticeMessageID != tt.expected.NoticeMessageID ||
				!reflect.DeepEqual(result.Tags, tt.expected.Tags) ||
				result.RawMessage != tt.expected.RawMessage {
				t.Errorf("parseNOTICE() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestParseHOSTTARGET(t *testing.T) {
	tests := []struct {
		name       string
		rawMessage string
		tags       map[string]string
		parts      []string
		commandIdx int
		expectNil  bool
		expected   *ChatMessage
	}{
		{
			name:       "invalid parts length",
			rawMessage: ":tmi.twitch.tv HOSTTARGET #channel",
			tags:       map[string]string{},
			parts:      []string{":tmi.twitch.tv", "HOSTTARGET"},
			commandIdx: 1,
			expectNil:  true,
		},
		{
			name:       "host start",
			rawMessage: ":tmi.twitch.tv HOSTTARGET #channel :targetchannel 5",
			tags:       map[string]string{},
			parts:      []string{":tmi.twitch.tv", "HOSTTARGET", "#channel", ":targetchannel", "5"},
			commandIdx: 1,
			expectNil:  false,
			expected: &ChatMessage{
				MessageType: "host_target",
				Channel:     "#channel",
				TargetUser:  "targetchannel",
				ViewerCount: 5,
				Tags:        map[string]string{},
				RawMessage:  ":tmi.twitch.tv HOSTTARGET #channel :targetchannel 5",
			},
		},
		{
			name:       "host end",
			rawMessage: ":tmi.twitch.tv HOSTTARGET #channel :- 0",
			tags:       map[string]string{},
			parts:      []string{":tmi.twitch.tv", "HOSTTARGET", "#channel", ":-", "0"},
			commandIdx: 1,
			expectNil:  false,
			expected: &ChatMessage{
				MessageType: "host_target",
				Channel:     "#channel",
				TargetUser:  "-",
				ViewerCount: 0,
				Tags:        map[string]string{},
				RawMessage:  ":tmi.twitch.tv HOSTTARGET #channel :- 0",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseHOSTTARGET(tt.rawMessage, tt.tags, tt.parts, tt.commandIdx)
			if tt.expectNil {
				if result != nil {
					t.Errorf("parseHOSTTARGET() expected nil, got %v", result)
				}
				return
			}

			if result == nil {
				t.Errorf("parseHOSTTARGET() expected non-nil, got nil")
				return
			}

			// Compare fields, ignoring Timestamp
			if result.MessageType != tt.expected.MessageType ||
				result.Channel != tt.expected.Channel ||
				result.TargetUser != tt.expected.TargetUser ||
				result.ViewerCount != tt.expected.ViewerCount ||
				!reflect.DeepEqual(result.Tags, tt.expected.Tags) ||
				result.RawMessage != tt.expected.RawMessage {
				t.Errorf("parseHOSTTARGET() = %v, want %v", result, tt.expected)
			}
		})
	}
}
