package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"mahoroba.local/mahoroba/internal/generation"
)

func TestLoadStrictAndPrecedence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	content := strings.ReplaceAll(`
data_dir = "DATA"
timezone = "Asia/Tokyo"
[server]
listen = "127.0.0.1:9999"
[generation]
retry_backoff = ["2s", "7s"]
`, "DATA", filepath.ToSlash(filepath.Join(dir, "data")))
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MAHOROBA_LISTEN", "[::1]:7777")
	cfg, err := Load(Overrides{ConfigPath: path, Listen: "127.0.0.1:8888"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Listen != "127.0.0.1:8888" || cfg.Timezone != "Asia/Tokyo" {
		t.Fatalf("unexpected precedence result: %#v", cfg)
	}
	if got := cfg.Generation.RetryBackoff; len(got) != 2 || got[1] != 7*time.Second {
		t.Fatalf("unexpected retry backoff: %v", got)
	}
}

func TestLoadRejectsUnknownAndSecretFields(t *testing.T) {
	for _, content := range []string{
		"unknown = true\n",
		"[generation]\napi_key = \"must-not-live-here\"\n",
	} {
		path := filepath.Join(t.TempDir(), "config.toml")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(Overrides{ConfigPath: path}); err == nil {
			t.Fatalf("expected strict decode failure for %q", content)
		}
	}
}

func TestRejectsUnsafeNetworkSurfaces(t *testing.T) {
	cfg := defaults()
	cfg.Server.Listen = "0.0.0.0:8787"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("non-listener commands must be able to read dialogue settings: %v", err)
	}
	if err := cfg.ValidateForServe(); err == nil {
		t.Fatal("expected non-loopback serve rejection without an explicit flag")
	}
	cfg = defaults()
	cfg.Generation.BaseURL = "http://example.com/v1"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected remote plaintext provider rejection")
	}
	cfg.Generation.BaseURL = "http://127.0.0.1:11434/v1"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("loopback provider should be allowed: %v", err)
	}
}

func TestValidateForServe(t *testing.T) {
	cfg := defaults()
	if err := cfg.ValidateForServe(); err == nil {
		t.Fatal("expected missing provider configuration")
	}
	cfg.Generation.BaseURL = "https://api.example.test/v1"
	cfg.Generation.Model = "model"
	if err := cfg.ValidateForServe(); err != nil {
		t.Fatal(err)
	}
}

func TestStructuredOutputModeResolvesAndValidatesCapability(t *testing.T) {
	for _, test := range []struct {
		name     string
		mode     StructuredOutputMode
		supports bool
		want     generation.StructuredOutputMode
		wantErr  bool
	}{
		{name: "auto prompt fallback", mode: StructuredOutputAuto, want: generation.StructuredOutputPrompt},
		{name: "zero-value source compatibility", want: generation.StructuredOutputPrompt},
		{name: "auto native schema", mode: StructuredOutputAuto, supports: true, want: generation.StructuredOutputJSONSchema},
		{name: "required native schema", mode: StructuredOutputRequired, supports: true, want: generation.StructuredOutputJSONSchema},
		{name: "required unsupported", mode: StructuredOutputRequired, wantErr: true},
		{name: "prompt ignores capability", mode: StructuredOutputPrompt, supports: true, want: generation.StructuredOutputPrompt},
		{name: "unknown", mode: "json_schema", supports: true, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := defaults()
			cfg.Generation.StructuredOutputMode = test.mode
			cfg.Generation.SupportsJSONSchema = test.supports
			got, err := cfg.Generation.ResolvedStructuredOutputMode()
			if test.wantErr {
				if err == nil || cfg.Validate() == nil {
					t.Fatalf("mode=%q supports=%v unexpectedly validated", test.mode, test.supports)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("resolved mode=%q err=%v, want %q", got, err, test.want)
			}
			if err := cfg.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestLoadStructuredOutputConfigurationAndEnvironment(t *testing.T) {
	path := writeConfig(t, "[generation]\nstructured_output_mode = \"required\"\nsupports_json_schema = true\n")
	cfg, err := Load(Overrides{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Generation.StructuredOutputMode != StructuredOutputRequired || !cfg.Generation.SupportsJSONSchema {
		t.Fatalf("structured output config = %#v", cfg.Generation)
	}
	if got, err := cfg.Generation.ResolvedStructuredOutputMode(); err != nil || got != generation.StructuredOutputJSONSchema {
		t.Fatalf("resolved file mode=%q err=%v", got, err)
	}

	t.Setenv("MAHOROBA_STRUCTURED_OUTPUT_MODE", "prompt")
	t.Setenv("MAHOROBA_SUPPORTS_JSON_SCHEMA", "false")
	cfg, err = Load(Overrides{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Generation.StructuredOutputMode != StructuredOutputPrompt || cfg.Generation.SupportsJSONSchema {
		t.Fatalf("environment did not override structured output config: %#v", cfg.Generation)
	}

	t.Setenv("MAHOROBA_SUPPORTS_JSON_SCHEMA", "not-a-bool")
	if _, err := Load(Overrides{ConfigPath: path}); err == nil {
		t.Fatal("invalid supports-json-schema environment value was accepted")
	}
}

func TestProjectionDefaults(t *testing.T) {
	cfg := defaults()
	if cfg.Projection.ScanInterval != 30*time.Second {
		t.Fatalf("scan interval = %v", cfg.Projection.ScanInterval)
	}
	if cfg.Projection.AsOfRefreshInterval != time.Minute {
		t.Fatalf("as-of refresh interval = %v", cfg.Projection.AsOfRefreshInterval)
	}
	if cfg.Projection.RebuildRetryInterval != 30*time.Second {
		t.Fatalf("rebuild retry interval = %v", cfg.Projection.RebuildRetryInterval)
	}
	if cfg.Projection.MaxStaleness != 5*time.Minute {
		t.Fatalf("max staleness = %v", cfg.Projection.MaxStaleness)
	}
	if cfg.Projection.RebuildTransactionTimeout != 2*time.Second {
		t.Fatalf("rebuild transaction timeout = %v", cfg.Projection.RebuildTransactionTimeout)
	}
}

func TestProjectionOverrides(t *testing.T) {
	path := writeConfig(t, `
[projection]
scan_interval = "45s"
as_of_refresh_interval = "2m"
rebuild_retry_interval = "1m"
max_staleness = "10m"
rebuild_transaction_timeout = "2s"

[projection.overrides.resident_current_status]
scan_interval = "5s"
max_staleness = "30s"

[projection.overrides.runtime_states]
as_of_refresh_interval = "15s"
rebuild_retry_interval = "3m"
`)
	cfg, err := Load(Overrides{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Projection.ScanInterval != 45*time.Second || cfg.Projection.AsOfRefreshInterval != 2*time.Minute || cfg.Projection.RebuildRetryInterval != time.Minute || cfg.Projection.MaxStaleness != 10*time.Minute {
		t.Fatalf("unexpected projection settings: %#v", cfg.Projection)
	}
	status := cfg.Projection.Overrides["resident_current_status"]
	if status.ScanInterval != 5*time.Second || status.MaxStaleness != 30*time.Second || status.AsOfRefreshInterval != 0 || status.RebuildRetryInterval != 0 {
		t.Fatalf("unexpected status override: %#v", status)
	}
	runtime := cfg.Projection.Overrides["runtime_states"]
	if runtime.AsOfRefreshInterval != 15*time.Second || runtime.RebuildRetryInterval != 3*time.Minute {
		t.Fatalf("unexpected runtime override: %#v", runtime)
	}
}

func TestProjectionRejectsNonPositiveDurations(t *testing.T) {
	for _, test := range []struct {
		name    string
		content string
	}{
		{name: "global zero", content: "[projection]\nscan_interval = \"0s\"\n"},
		{name: "global negative", content: "[projection]\nmax_staleness = \"-1s\"\n"},
		{name: "override zero", content: "[projection.overrides.runtime_states]\nas_of_refresh_interval = \"0s\"\n"},
		{name: "override negative", content: "[projection.overrides.resident_current_revision]\nrebuild_retry_interval = \"-1s\"\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Load(Overrides{ConfigPath: writeConfig(t, test.content)}); err == nil {
				t.Fatal("expected non-positive duration rejection")
			}
		})
	}
}

func TestProjectionRejectsUnknownOverrideName(t *testing.T) {
	path := writeConfig(t, "[projection.overrides.not_registered]\nscan_interval = \"5s\"\n")
	if _, err := Load(Overrides{ConfigPath: path}); err == nil {
		t.Fatal("expected inactive projection override rejection")
	}
}

func TestM5ProjectionOverridesAcceptProductionClaimProjections(t *testing.T) {
	path := writeConfig(t, `[projection.overrides.claim_states]
scan_interval = "5s"
max_staleness = "30s"

[projection.overrides.claim_view_scope_current]
rebuild_retry_interval = "45s"
`)
	cfg, err := Load(Overrides{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Projection.Overrides["claim_states"]; got.ScanInterval != 5*time.Second || got.MaxStaleness != 30*time.Second {
		t.Fatalf("claim_states override = %#v", got)
	}
	if got := cfg.Projection.Overrides["claim_view_scope_current"]; got.RebuildRetryInterval != 45*time.Second {
		t.Fatalf("claim_view_scope_current override = %#v", got)
	}
}

func TestM7ProjectionOverridesAcceptContentReferences(t *testing.T) {
	path := writeConfig(t, `[projection.overrides.content_references]
scan_interval = "7s"
rebuild_retry_interval = "45s"
`)
	cfg, err := Load(Overrides{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	got := cfg.Projection.Overrides["content_references"]
	if got.ScanInterval != 7*time.Second || got.RebuildRetryInterval != 45*time.Second {
		t.Fatalf("content_references override = %#v", got)
	}
}

func TestProjectionTransactionTimeoutCannotBeOverridden(t *testing.T) {
	path := writeConfig(t, "[projection.overrides.runtime_states]\nrebuild_transaction_timeout = \"1s\"\n")
	if _, err := Load(Overrides{ConfigPath: path}); err == nil {
		t.Fatal("expected strict decode rejection for per-projection transaction timeout")
	}
}

func TestM6AutonomyDefaultsAreDisabledAndFixed(t *testing.T) {
	cfg := defaults()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	a := cfg.Autonomy
	if a.SelfTalk.Enabled || a.Initiative.Enabled || a.TTS.Enabled || a.Retention.Mode != "disabled" {
		t.Fatalf("optional features are not disabled: %#v", a)
	}
	if a.Version != "autonomy-policy-v0" || a.ScanInterval != 30*time.Second ||
		a.SelfTalk.Interval != 30*time.Minute || a.SelfTalk.ConsecutiveLimit != 10 ||
		a.SelfTalk.HourlyLimit != 2 || a.SelfTalk.DailyLimit != 12 {
		t.Fatalf("unexpected autonomy defaults: %#v", a)
	}
	if a.TTS.Provider != "openai_speech" || a.TTS.RequestTimeout != 15*time.Second ||
		a.TTS.MaxConcurrency != 1 || a.TTS.QueueCapacity != 8 || a.TTS.MaxAudioBytes != 8<<20 {
		t.Fatalf("unexpected TTS defaults: %#v", a.TTS)
	}
}

func TestM6AutonomyConfigLoadsAllFields(t *testing.T) {
	path := writeConfig(t, `
[autonomy]
version = "autonomy-policy-v0"
scan_interval = "45s"
[autonomy.self_talk]
enabled = true
interval = "40m"
consecutive_limit = 7
hourly_limit = 3
daily_limit = 9
quiet_hours_start = "22:30"
quiet_hours_end = "06:15"
[autonomy.initiative]
enabled = true
minimum_interval = "8h"
hourly_limit = 2
daily_limit = 3
recent_user_suppression = "45m"
quiet_hours_start = "22:00"
quiet_hours_end = "06:00"
triggers = ["future_to_current"]
[autonomy.retention]
mode = "candidate_after"
duration = "960h"
scan_interval = "12h"
eligibility = "present_self_talk"
[autonomy.tts]
enabled = false
provider = "aivis_speech"
request_timeout = "20s"
max_concurrency = 2
queue_capacity = 4
max_audio_bytes = 4194304
[autonomy.tts.openai_speech]
base_url = "https://api.example.test/v1"
model = "speech-model"
voice = "voice"
[autonomy.tts.aivis_speech]
base_url = "http://127.0.0.1:10101"
style_id = -2147483648
`)
	cfg, err := Load(Overrides{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Autonomy.SelfTalk.Enabled || !cfg.Autonomy.Initiative.Enabled ||
		cfg.Autonomy.ScanInterval != 45*time.Second || cfg.Autonomy.Retention.Mode != "candidate_after" {
		t.Fatalf("autonomy config=%#v", cfg.Autonomy)
	}
	if cfg.Autonomy.TTS.AivisSpeech.StyleID == nil || *cfg.Autonomy.TTS.AivisSpeech.StyleID != -2147483648 {
		t.Fatalf("Aivis config=%#v", cfg.Autonomy.TTS.AivisSpeech)
	}
	if _, err := cfg.Autonomy.SchedulerPolicy(cfg.Timezone); err != nil {
		t.Fatal(err)
	}
}

func TestM6AutonomyConfigRejectsUnknownAndNonPositiveValues(t *testing.T) {
	for _, test := range []struct {
		name    string
		content string
	}{
		{name: "unknown", content: "[autonomy]\nunknown = true\n"},
		{name: "empty duration", content: "[autonomy]\nscan_interval = \"\"\n"},
		{name: "zero duration", content: "[autonomy.self_talk]\ninterval = \"0s\"\n"},
		{name: "negative duration", content: "[autonomy.retention]\nduration = \"-1s\"\n"},
		{name: "zero limit", content: "[autonomy.self_talk]\nhourly_limit = 0\n"},
		{name: "negative limit", content: "[autonomy.initiative]\ndaily_limit = -1\n"},
		{name: "secret in TOML", content: "[autonomy.tts.openai_speech]\napi_key = \"forbidden\"\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Load(Overrides{ConfigPath: writeConfig(t, test.content)}); err == nil {
				t.Fatal("invalid autonomy config was accepted")
			}
		})
	}
}

func TestM6AutonomyConfigRejectsInvalidQuietHoursAndTriggers(t *testing.T) {
	for _, content := range []string{
		"[autonomy.self_talk]\nquiet_hours_start = \"7:00\"\n",
		"[autonomy.self_talk]\nquiet_hours_start = \"07:00\"\nquiet_hours_end = \"07:00\"\n",
		"[autonomy.initiative]\ntriggers = [\"unknown\"]\n",
		"[autonomy.initiative]\ntriggers = [\"future_to_current\", \"future_to_current\"]\n",
	} {
		if _, err := Load(Overrides{ConfigPath: writeConfig(t, content)}); err == nil {
			t.Fatalf("invalid autonomy config was accepted: %s", content)
		}
	}
}

func TestM6TTSConfigRequiresOnlyEnabledSelectedProvider(t *testing.T) {
	cfg := defaults()
	cfg.Autonomy.TTS.Enabled = true
	if err := cfg.Validate(); err == nil {
		t.Fatal("enabled incomplete OpenAI speech config was accepted")
	}
	cfg.Autonomy.TTS.OpenAISpeech = OpenAISpeech{
		BaseURL: "https://api.example.test/v1", Model: "speech", Voice: "voice", APIKey: "secret",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	cfg = defaults()
	cfg.Autonomy.TTS.Enabled = true
	cfg.Autonomy.TTS.Provider = "aivis_speech"
	style := int64(-1)
	cfg.Autonomy.TTS.AivisSpeech.StyleID = &style
	if err := cfg.Validate(); err != nil {
		t.Fatalf("negative signed global style ID was rejected: %v", err)
	}
	tooLarge := int64(1 << 31)
	cfg.Autonomy.TTS.AivisSpeech.StyleID = &tooLarge
	if err := cfg.Validate(); err == nil {
		t.Fatal("out-of-range Aivis style ID was accepted")
	}
}

func TestM6AutonomySchedulerGateAndTTSHardLimits(t *testing.T) {
	cfg := defaults()
	if cfg.Autonomy.AnySchedulerFeatureEnabled() {
		t.Fatal("default autonomy unexpectedly enables a scheduler")
	}
	cfg.Autonomy.Retention.Mode = "candidate_after"
	if !cfg.Autonomy.AnySchedulerFeatureEnabled() {
		t.Fatal("retention candidate scan did not enable the scheduler gate")
	}
	cfg = defaults()
	cfg.Autonomy.TTS.MaxAudioBytes = (8 << 20) + 1
	if err := cfg.Validate(); err == nil {
		t.Fatal("TTS audio cap above 8 MiB was accepted")
	}
}

func TestM6AivisRequiresNumericLoopbackHTTPAndSignedInt32Style(t *testing.T) {
	for _, baseURL := range []string{"https://127.0.0.1:10101", "http://localhost:10101", "https://example.test"} {
		cfg := defaults()
		cfg.Autonomy.TTS.Enabled = true
		cfg.Autonomy.TTS.Provider = "aivis_speech"
		style := int64(-1)
		cfg.Autonomy.TTS.AivisSpeech.StyleID = &style
		cfg.Autonomy.TTS.AivisSpeech.BaseURL = baseURL
		if err := cfg.Validate(); err == nil {
			t.Fatalf("unsafe Aivis URL %q was accepted", baseURL)
		}
	}
	cfg := defaults()
	cfg.Autonomy.TTS.Enabled = true
	cfg.Autonomy.TTS.Provider = "aivis_speech"
	style := int64(-2147483648)
	cfg.Autonomy.TTS.AivisSpeech.StyleID = &style
	if err := cfg.Validate(); err != nil {
		t.Fatalf("numeric loopback and signed int32 style were rejected: %v", err)
	}
}

func TestM6TTSAPIKeyComesOnlyFromEnvironment(t *testing.T) {
	t.Setenv("MAHOROBA_TTS_API_KEY", "tts-secret")
	path := writeConfig(t, `
[autonomy.tts]
enabled = true
provider = "openai_speech"
[autonomy.tts.openai_speech]
base_url = "https://api.example.test/v1"
model = "speech"
voice = "voice"
`)
	cfg, err := Load(Overrides{ConfigPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Autonomy.TTS.OpenAISpeech.APIKey != "tts-secret" {
		t.Fatal("TTS API key was not loaded from its dedicated environment variable")
	}
}

func TestM7SecretEnvironmentRejectsConflictEmptyAndDisabledTTS(t *testing.T) {
	for _, test := range []struct {
		name string
		env  map[string]string
	}{
		{name: "provider conflict", env: map[string]string{
			"MAHOROBA_PROVIDER_API_KEY": "supersecretvalue", "MAHOROBA_PROVIDER_API_KEY_FILE": "not-disclosed",
		}},
		{name: "provider empty direct", env: map[string]string{"MAHOROBA_PROVIDER_API_KEY": ""}},
		{name: "provider empty file", env: map[string]string{"MAHOROBA_PROVIDER_API_KEY_FILE": ""}},
		{name: "disabled tts direct", env: map[string]string{"MAHOROBA_TTS_API_KEY": "must-not-load"}},
		{name: "disabled tts file", env: map[string]string{"MAHOROBA_TTS_API_KEY_FILE": "must-not-open"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			for name, value := range test.env {
				t.Setenv(name, value)
			}
			_, err := Load(Overrides{ConfigPath: writeConfig(t, "")})
			if err == nil {
				t.Fatal("expected secret environment rejection")
			}
			if strings.Contains(err.Error(), "supersecretvalue") || strings.Contains(err.Error(), "not-disclosed") ||
				strings.Contains(err.Error(), "must-not") {
				t.Fatalf("secret error leaked source material: %v", err)
			}
		})
	}
}

func TestM7AivisSpeechRejectsUnusedTTSSecret(t *testing.T) {
	t.Setenv("MAHOROBA_TTS_API_KEY", "must-not-load")
	path := writeConfig(t, `[autonomy.tts]
enabled = true
provider = "aivis_speech"
[autonomy.tts.aivis_speech]
base_url = "http://127.0.0.1:10101"
style_id = 1
`)
	if _, err := Load(Overrides{ConfigPath: path}); err == nil || strings.Contains(err.Error(), "must-not-load") {
		t.Fatalf("Aivis secret shape error = %v", err)
	}
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDialogueRemoteAccessRequiresExplicitOverride(t *testing.T) {
	t.Setenv("MAHOROBA_LISTEN", "100.64.0.42:8787")
	t.Setenv("MAHOROBA_ALLOW_REMOTE", "true") // There is intentionally no environment opt-in.
	path := writeConfig(t, "[generation]\nbase_url = \"http://127.0.0.1:1/v1\"\nmodel = \"test\"\n")
	for _, allow := range []bool{false, true} {
		cfg, err := Load(Overrides{ConfigPath: path, AllowRemote: allow})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Server.AllowRemote != allow {
			t.Fatalf("remote override = %v", cfg.Server.AllowRemote)
		}
		if err := cfg.ValidateForServe(); (err == nil) != allow {
			t.Fatalf("allow=%v: %v", allow, err)
		}
	}
	for _, key := range []string{"allow_remote", "dialogue_allow_remote"} {
		if _, err := Load(Overrides{ConfigPath: writeConfig(t, "[server]\n"+key+" = true\n")}); err == nil {
			t.Fatalf("configuration unexpectedly enabled %s", key)
		}
	}
}

func TestDialogueListenConfigurationRequiresNumericAddressAndPort(t *testing.T) {
	for _, address := range []string{"0.0.0.0:8787", "[::]:8787", "100.64.0.42:8787", "127.0.0.1:0"} {
		cfg := defaults()
		cfg.Server.Listen = address
		cfg.Server.AllowRemote = true
		cfg.Generation.BaseURL = "http://127.0.0.1:1/v1"
		cfg.Generation.Model = "test"
		if err := cfg.ValidateForServe(); err != nil {
			t.Fatalf("%s: %v", address, err)
		}
	}
	for _, address := range []string{"example.test:8787", ":8787", "100.64.0.42:", "100.64.0.42:65536", "100.64.0.42:http"} {
		cfg := defaults()
		cfg.Server.Listen = address
		cfg.Server.AllowRemote = true
		if err := cfg.Validate(); err == nil {
			t.Fatalf("invalid address accepted: %s", address)
		}
	}
}
