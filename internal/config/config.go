package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
	"mahoroba.local/mahoroba/internal/autonomy"
	"mahoroba.local/mahoroba/internal/configsecure"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/secret"
)

const envPrefix = "MAHOROBA_"

type StructuredOutputMode string

const (
	StructuredOutputAuto     StructuredOutputMode = "auto"
	StructuredOutputRequired StructuredOutputMode = "required"
	StructuredOutputPrompt   StructuredOutputMode = "prompt"
)

type Server struct {
	// AllowRemote is set only by an explicit dialogue CLI flag, never TOML or environment.
	AllowRemote bool `toml:"-"`
	// ContainerListen permits container binding only; HTTP Host policy remains local.
	ContainerListen bool          `toml:"-"`
	Listen          string        `toml:"listen"`
	ShutdownTimeout time.Duration `toml:"-"`
}

type Database struct {
	Filename string `toml:"filename"`
}

type Generation struct {
	// AllowDockerHostHTTP is an explicit environment opt-in for a local provider
	// reached through Docker Desktop's canonical host name. It is not a TTS setting.
	AllowDockerHostHTTP  bool `toml:"-"`
	Provider             string
	BaseURL              string
	Model                string
	APIKey               string        `toml:"-"`
	RequestTimeout       time.Duration `toml:"-"`
	MaxAttempts          int
	RetryBackoff         []time.Duration `toml:"-"`
	MaxConcurrency       int
	DisconnectPolicy     string
	MaxInputBytes        int
	MaxOutputBytes       int
	SafetyScanInterval   time.Duration `toml:"-"`
	StructuredOutputMode StructuredOutputMode
	SupportsJSONSchema   bool
}

type Projection struct {
	ScanInterval              time.Duration `toml:"-"`
	AsOfRefreshInterval       time.Duration `toml:"-"`
	RebuildRetryInterval      time.Duration `toml:"-"`
	MaxStaleness              time.Duration `toml:"-"`
	RebuildTransactionTimeout time.Duration `toml:"-"`
	Overrides                 map[string]ProjectionOverride
}

type ProjectionOverride struct {
	ScanInterval         time.Duration `toml:"-"`
	AsOfRefreshInterval  time.Duration `toml:"-"`
	RebuildRetryInterval time.Duration `toml:"-"`
	MaxStaleness         time.Duration `toml:"-"`
}

type Autonomy struct {
	Version      string
	ScanInterval time.Duration
	SelfTalk     AutonomySelfTalk
	Initiative   AutonomyInitiative
	Retention    AutonomyRetention
	TTS          AutonomyTTS
}

type AutonomySelfTalk struct {
	Enabled          bool
	Interval         time.Duration
	ConsecutiveLimit int64
	HourlyLimit      int64
	DailyLimit       int64
	QuietHoursStart  string
	QuietHoursEnd    string
}

type AutonomyInitiative struct {
	Enabled               bool
	MinimumInterval       time.Duration
	HourlyLimit           int64
	DailyLimit            int64
	RecentUserSuppression time.Duration
	QuietHoursStart       string
	QuietHoursEnd         string
	Triggers              []string
}

type AutonomyRetention struct {
	Mode         string
	Duration     time.Duration
	ScanInterval time.Duration
	Eligibility  string
}

type AutonomyTTS struct {
	Enabled        bool
	Provider       string
	RequestTimeout time.Duration
	MaxConcurrency int
	QueueCapacity  int
	MaxAudioBytes  int
	OpenAISpeech   OpenAISpeech
	AivisSpeech    AivisSpeech
}

type OpenAISpeech struct {
	BaseURL string
	Model   string
	Voice   string
	APIKey  string `toml:"-"`
}

type AivisSpeech struct {
	BaseURL string
	StyleID *int64
}

type Config struct {
	DataDir    string
	Timezone   string
	Server     Server
	Database   Database
	Generation Generation
	Projection Projection
	Autonomy   Autonomy
}

type Overrides struct {
	ConfigPath      string
	DataDir         string
	Listen          string
	Timezone        string
	AllowRemote     bool
	ContainerListen bool
}

type fileConfig struct {
	DataDir  string `toml:"data_dir"`
	Timezone string `toml:"timezone"`
	Server   struct {
		Listen          string `toml:"listen"`
		ShutdownTimeout string `toml:"shutdown_timeout"`
	} `toml:"server"`
	Database struct {
		Filename string `toml:"filename"`
	} `toml:"database"`
	Generation struct {
		Provider             string   `toml:"provider"`
		BaseURL              string   `toml:"base_url"`
		Model                string   `toml:"model"`
		RequestTimeout       string   `toml:"request_timeout"`
		MaxAttempts          int      `toml:"max_attempts"`
		RetryBackoff         []string `toml:"retry_backoff"`
		MaxConcurrency       int      `toml:"max_concurrency"`
		DisconnectPolicy     string   `toml:"disconnect_policy"`
		MaxInputBytes        int      `toml:"max_input_bytes"`
		MaxOutputBytes       int      `toml:"max_output_bytes"`
		SafetyScanInterval   string   `toml:"safety_scan_interval"`
		StructuredOutputMode string   `toml:"structured_output_mode"`
		SupportsJSONSchema   bool     `toml:"supports_json_schema"`
	} `toml:"generation"`
	Projection struct {
		ScanInterval              string                            `toml:"scan_interval"`
		AsOfRefreshInterval       string                            `toml:"as_of_refresh_interval"`
		RebuildRetryInterval      string                            `toml:"rebuild_retry_interval"`
		MaxStaleness              string                            `toml:"max_staleness"`
		RebuildTransactionTimeout string                            `toml:"rebuild_transaction_timeout"`
		Overrides                 map[string]fileProjectionOverride `toml:"overrides"`
	} `toml:"projection"`
	Autonomy fileAutonomy `toml:"autonomy"`
}

type fileProjectionOverride struct {
	ScanInterval         string `toml:"scan_interval"`
	AsOfRefreshInterval  string `toml:"as_of_refresh_interval"`
	RebuildRetryInterval string `toml:"rebuild_retry_interval"`
	MaxStaleness         string `toml:"max_staleness"`
}

type fileAutonomy struct {
	Version      *string                `toml:"version"`
	ScanInterval *string                `toml:"scan_interval"`
	SelfTalk     fileAutonomySelfTalk   `toml:"self_talk"`
	Initiative   fileAutonomyInitiative `toml:"initiative"`
	Retention    fileAutonomyRetention  `toml:"retention"`
	TTS          fileAutonomyTTS        `toml:"tts"`
}

type fileAutonomySelfTalk struct {
	Enabled          *bool   `toml:"enabled"`
	Interval         *string `toml:"interval"`
	ConsecutiveLimit *int64  `toml:"consecutive_limit"`
	HourlyLimit      *int64  `toml:"hourly_limit"`
	DailyLimit       *int64  `toml:"daily_limit"`
	QuietHoursStart  *string `toml:"quiet_hours_start"`
	QuietHoursEnd    *string `toml:"quiet_hours_end"`
}

type fileAutonomyInitiative struct {
	Enabled               *bool    `toml:"enabled"`
	MinimumInterval       *string  `toml:"minimum_interval"`
	HourlyLimit           *int64   `toml:"hourly_limit"`
	DailyLimit            *int64   `toml:"daily_limit"`
	RecentUserSuppression *string  `toml:"recent_user_suppression"`
	QuietHoursStart       *string  `toml:"quiet_hours_start"`
	QuietHoursEnd         *string  `toml:"quiet_hours_end"`
	Triggers              []string `toml:"triggers"`
}

type fileAutonomyRetention struct {
	Mode         *string `toml:"mode"`
	Duration     *string `toml:"duration"`
	ScanInterval *string `toml:"scan_interval"`
	Eligibility  *string `toml:"eligibility"`
}

type fileAutonomyTTS struct {
	Enabled        *bool   `toml:"enabled"`
	Provider       *string `toml:"provider"`
	RequestTimeout *string `toml:"request_timeout"`
	MaxConcurrency *int    `toml:"max_concurrency"`
	QueueCapacity  *int    `toml:"queue_capacity"`
	MaxAudioBytes  *int    `toml:"max_audio_bytes"`
	OpenAISpeech   struct {
		BaseURL *string `toml:"base_url"`
		Model   *string `toml:"model"`
		Voice   *string `toml:"voice"`
	} `toml:"openai_speech"`
	AivisSpeech struct {
		BaseURL *string `toml:"base_url"`
		StyleID *int64  `toml:"style_id"`
	} `toml:"aivis_speech"`
}

var activeProjectionNames = map[string]struct{}{
	"resident_current_status":   {},
	"resident_current_revision": {},
	"runtime_states":            {},
	"claim_states":              {},
	"claim_view_scope_current":  {},
	"content_references":        {},
}

func defaults() Config {
	policy := autonomy.DefaultPolicy("UTC")
	return Config{
		DataDir:  defaultDataDir(),
		Timezone: "UTC",
		Server: Server{
			Listen:          "127.0.0.1:8787",
			ShutdownTimeout: 15 * time.Second,
		},
		Database: Database{Filename: "mahoroba.db"},
		Generation: Generation{
			Provider:             "chat-completions",
			RequestTimeout:       120 * time.Second,
			MaxAttempts:          3,
			RetryBackoff:         []time.Duration{time.Second, 5 * time.Second},
			MaxConcurrency:       1,
			DisconnectPolicy:     "continue",
			MaxInputBytes:        64 << 10,
			MaxOutputBytes:       64 << 10,
			SafetyScanInterval:   30 * time.Second,
			StructuredOutputMode: StructuredOutputAuto,
		},
		Projection: Projection{
			ScanInterval:              30 * time.Second,
			AsOfRefreshInterval:       time.Minute,
			RebuildRetryInterval:      30 * time.Second,
			MaxStaleness:              5 * time.Minute,
			RebuildTransactionTimeout: 2 * time.Second,
			Overrides:                 make(map[string]ProjectionOverride),
		},
		Autonomy: Autonomy{
			Version: string(policy.Version), ScanInterval: policy.ScanInterval,
			SelfTalk: AutonomySelfTalk{
				Enabled: policy.SelfTalk.Enabled, Interval: policy.SelfTalk.Interval,
				ConsecutiveLimit: policy.SelfTalk.ConsecutiveLimit, HourlyLimit: policy.SelfTalk.HourlyLimit,
				DailyLimit: policy.SelfTalk.DailyLimit, QuietHoursStart: policy.SelfTalk.QuietHours.Start.String(),
				QuietHoursEnd: policy.SelfTalk.QuietHours.End.String(),
			},
			Initiative: AutonomyInitiative{
				Enabled: policy.Initiative.Enabled, MinimumInterval: policy.Initiative.MinimumInterval,
				HourlyLimit: policy.Initiative.HourlyLimit, DailyLimit: policy.Initiative.DailyLimit,
				RecentUserSuppression: policy.Initiative.RecentUserSuppression,
				QuietHoursStart:       policy.Initiative.QuietHours.Start.String(), QuietHoursEnd: policy.Initiative.QuietHours.End.String(),
				Triggers: []string{string(autonomy.TriggerFutureToCurrent), string(autonomy.TriggerVolatileAging)},
			},
			Retention: AutonomyRetention{
				Mode: string(policy.Retention.Mode), Duration: policy.Retention.Duration,
				ScanInterval: policy.Retention.ScanInterval, Eligibility: string(policy.Retention.Eligibility),
			},
			TTS: AutonomyTTS{
				Provider: "openai_speech", RequestTimeout: 15 * time.Second, MaxConcurrency: 1,
				QueueCapacity: 8, MaxAudioBytes: 8 << 20,
				AivisSpeech: AivisSpeech{BaseURL: "http://127.0.0.1:10101"},
			},
		},
	}
}

func Load(overrides Overrides) (Config, error) {
	cfg := defaults()
	path := overrides.ConfigPath
	if path == "" {
		path = DefaultConfigPath()
	}
	if path != "" {
		data, err := readConfigFile(path)
		if err != nil && !(errors.Is(err, os.ErrNotExist) && overrides.ConfigPath == "") {
			return Config{}, fmt.Errorf("read config: %w", err)
		}
		if err == nil {
			var fc fileConfig
			dec := toml.NewDecoder(strings.NewReader(string(data)))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&fc); err != nil {
				return Config{}, fmt.Errorf("decode config: %w", err)
			}
			if err := applyFile(&cfg, fc); err != nil {
				return Config{}, err
			}
		}
	}
	if err := applyEnvironment(&cfg); err != nil {
		return Config{}, err
	}
	if overrides.DataDir != "" {
		cfg.DataDir = overrides.DataDir
	}
	if overrides.Listen != "" {
		cfg.Server.Listen = overrides.Listen
	}
	if overrides.Timezone != "" {
		cfg.Timezone = overrides.Timezone
	}
	cfg.Server.AllowRemote = overrides.AllowRemote
	cfg.Server.ContainerListen = overrides.ContainerListen
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func readConfigFile(path string) ([]byte, error) {
	if runtime.GOOS == "linux" && filepath.Clean(path) == "/etc/mahoroba/config.toml" {
		content, err := configsecure.Read(path)
		if err != nil {
			return nil, configsecure.ErrInvalid
		}
		return content, nil
	}
	return os.ReadFile(path)
}

func applyFile(cfg *Config, fc fileConfig) error {
	if fc.DataDir != "" {
		cfg.DataDir = fc.DataDir
	}
	if fc.Timezone != "" {
		cfg.Timezone = fc.Timezone
	}
	if fc.Server.Listen != "" {
		cfg.Server.Listen = fc.Server.Listen
	}
	if err := parseDurationInto(fc.Server.ShutdownTimeout, &cfg.Server.ShutdownTimeout, "server.shutdown_timeout"); err != nil {
		return err
	}
	if fc.Database.Filename != "" {
		cfg.Database.Filename = fc.Database.Filename
	}
	g := fc.Generation
	if g.Provider != "" {
		cfg.Generation.Provider = g.Provider
	}
	if g.BaseURL != "" {
		cfg.Generation.BaseURL = g.BaseURL
	}
	if g.Model != "" {
		cfg.Generation.Model = g.Model
	}
	if err := parseDurationInto(g.RequestTimeout, &cfg.Generation.RequestTimeout, "generation.request_timeout"); err != nil {
		return err
	}
	if g.MaxAttempts != 0 {
		cfg.Generation.MaxAttempts = g.MaxAttempts
	}
	if len(g.RetryBackoff) > 0 {
		cfg.Generation.RetryBackoff = make([]time.Duration, len(g.RetryBackoff))
		for i, raw := range g.RetryBackoff {
			if err := parseDurationInto(raw, &cfg.Generation.RetryBackoff[i], fmt.Sprintf("generation.retry_backoff[%d]", i)); err != nil {
				return err
			}
		}
	}
	if g.MaxConcurrency != 0 {
		cfg.Generation.MaxConcurrency = g.MaxConcurrency
	}
	if g.DisconnectPolicy != "" {
		cfg.Generation.DisconnectPolicy = g.DisconnectPolicy
	}
	if g.MaxInputBytes != 0 {
		cfg.Generation.MaxInputBytes = g.MaxInputBytes
	}
	if g.MaxOutputBytes != 0 {
		cfg.Generation.MaxOutputBytes = g.MaxOutputBytes
	}
	if err := parseDurationInto(g.SafetyScanInterval, &cfg.Generation.SafetyScanInterval, "generation.safety_scan_interval"); err != nil {
		return err
	}
	if g.StructuredOutputMode != "" {
		cfg.Generation.StructuredOutputMode = StructuredOutputMode(g.StructuredOutputMode)
	}
	cfg.Generation.SupportsJSONSchema = g.SupportsJSONSchema
	p := fc.Projection
	if err := parseDurationInto(p.ScanInterval, &cfg.Projection.ScanInterval, "projection.scan_interval"); err != nil {
		return err
	}
	if err := parseDurationInto(p.AsOfRefreshInterval, &cfg.Projection.AsOfRefreshInterval, "projection.as_of_refresh_interval"); err != nil {
		return err
	}
	if err := parseDurationInto(p.RebuildRetryInterval, &cfg.Projection.RebuildRetryInterval, "projection.rebuild_retry_interval"); err != nil {
		return err
	}
	if err := parseDurationInto(p.MaxStaleness, &cfg.Projection.MaxStaleness, "projection.max_staleness"); err != nil {
		return err
	}
	if err := parseDurationInto(p.RebuildTransactionTimeout, &cfg.Projection.RebuildTransactionTimeout, "projection.rebuild_transaction_timeout"); err != nil {
		return err
	}
	for name, raw := range p.Overrides {
		if _, ok := activeProjectionNames[name]; !ok {
			return fmt.Errorf("projection override names an inactive projection %q", name)
		}
		var override ProjectionOverride
		if err := parsePositiveOptionalDuration(raw.ScanInterval, &override.ScanInterval, "projection.overrides."+name+".scan_interval"); err != nil {
			return err
		}
		if err := parsePositiveOptionalDuration(raw.AsOfRefreshInterval, &override.AsOfRefreshInterval, "projection.overrides."+name+".as_of_refresh_interval"); err != nil {
			return err
		}
		if err := parsePositiveOptionalDuration(raw.RebuildRetryInterval, &override.RebuildRetryInterval, "projection.overrides."+name+".rebuild_retry_interval"); err != nil {
			return err
		}
		if err := parsePositiveOptionalDuration(raw.MaxStaleness, &override.MaxStaleness, "projection.overrides."+name+".max_staleness"); err != nil {
			return err
		}
		cfg.Projection.Overrides[name] = override
	}
	if err := applyAutonomyFile(&cfg.Autonomy, fc.Autonomy); err != nil {
		return err
	}
	return nil
}

func applyAutonomyFile(dst *Autonomy, raw fileAutonomy) error {
	if raw.Version != nil {
		dst.Version = *raw.Version
	}
	if err := parsePresentDuration(raw.ScanInterval, &dst.ScanInterval, "autonomy.scan_interval"); err != nil {
		return err
	}
	selfTalk := raw.SelfTalk
	if selfTalk.Enabled != nil {
		dst.SelfTalk.Enabled = *selfTalk.Enabled
	}
	if err := parsePresentDuration(selfTalk.Interval, &dst.SelfTalk.Interval, "autonomy.self_talk.interval"); err != nil {
		return err
	}
	if selfTalk.ConsecutiveLimit != nil {
		dst.SelfTalk.ConsecutiveLimit = *selfTalk.ConsecutiveLimit
	}
	if selfTalk.HourlyLimit != nil {
		dst.SelfTalk.HourlyLimit = *selfTalk.HourlyLimit
	}
	if selfTalk.DailyLimit != nil {
		dst.SelfTalk.DailyLimit = *selfTalk.DailyLimit
	}
	if selfTalk.QuietHoursStart != nil {
		dst.SelfTalk.QuietHoursStart = *selfTalk.QuietHoursStart
	}
	if selfTalk.QuietHoursEnd != nil {
		dst.SelfTalk.QuietHoursEnd = *selfTalk.QuietHoursEnd
	}

	initiative := raw.Initiative
	if initiative.Enabled != nil {
		dst.Initiative.Enabled = *initiative.Enabled
	}
	if err := parsePresentDuration(initiative.MinimumInterval, &dst.Initiative.MinimumInterval, "autonomy.initiative.minimum_interval"); err != nil {
		return err
	}
	if initiative.HourlyLimit != nil {
		dst.Initiative.HourlyLimit = *initiative.HourlyLimit
	}
	if initiative.DailyLimit != nil {
		dst.Initiative.DailyLimit = *initiative.DailyLimit
	}
	if err := parsePresentDuration(initiative.RecentUserSuppression, &dst.Initiative.RecentUserSuppression, "autonomy.initiative.recent_user_suppression"); err != nil {
		return err
	}
	if initiative.QuietHoursStart != nil {
		dst.Initiative.QuietHoursStart = *initiative.QuietHoursStart
	}
	if initiative.QuietHoursEnd != nil {
		dst.Initiative.QuietHoursEnd = *initiative.QuietHoursEnd
	}
	if initiative.Triggers != nil {
		dst.Initiative.Triggers = append([]string(nil), initiative.Triggers...)
	}

	retention := raw.Retention
	if retention.Mode != nil {
		dst.Retention.Mode = *retention.Mode
	}
	if err := parsePresentDuration(retention.Duration, &dst.Retention.Duration, "autonomy.retention.duration"); err != nil {
		return err
	}
	if err := parsePresentDuration(retention.ScanInterval, &dst.Retention.ScanInterval, "autonomy.retention.scan_interval"); err != nil {
		return err
	}
	if retention.Eligibility != nil {
		dst.Retention.Eligibility = *retention.Eligibility
	}

	tts := raw.TTS
	if tts.Enabled != nil {
		dst.TTS.Enabled = *tts.Enabled
	}
	if tts.Provider != nil {
		dst.TTS.Provider = *tts.Provider
	}
	if err := parsePresentDuration(tts.RequestTimeout, &dst.TTS.RequestTimeout, "autonomy.tts.request_timeout"); err != nil {
		return err
	}
	if tts.MaxConcurrency != nil {
		dst.TTS.MaxConcurrency = *tts.MaxConcurrency
	}
	if tts.QueueCapacity != nil {
		dst.TTS.QueueCapacity = *tts.QueueCapacity
	}
	if tts.MaxAudioBytes != nil {
		dst.TTS.MaxAudioBytes = *tts.MaxAudioBytes
	}
	if tts.OpenAISpeech.BaseURL != nil {
		dst.TTS.OpenAISpeech.BaseURL = *tts.OpenAISpeech.BaseURL
	}
	if tts.OpenAISpeech.Model != nil {
		dst.TTS.OpenAISpeech.Model = *tts.OpenAISpeech.Model
	}
	if tts.OpenAISpeech.Voice != nil {
		dst.TTS.OpenAISpeech.Voice = *tts.OpenAISpeech.Voice
	}
	if tts.AivisSpeech.BaseURL != nil {
		dst.TTS.AivisSpeech.BaseURL = *tts.AivisSpeech.BaseURL
	}
	if tts.AivisSpeech.StyleID != nil {
		value := *tts.AivisSpeech.StyleID
		dst.TTS.AivisSpeech.StyleID = &value
	}
	return nil
}

func applyEnvironment(cfg *Config) error {
	if value, ok := os.LookupEnv(envPrefix + "PROVIDER_ALLOW_DOCKER_HOST_HTTP"); ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse %sPROVIDER_ALLOW_DOCKER_HOST_HTTP: %w", envPrefix, err)
		}
		cfg.Generation.AllowDockerHostHTTP = parsed
	}
	stringValues := []struct {
		name string
		dst  *string
	}{
		{"DATA_DIR", &cfg.DataDir}, {"TIMEZONE", &cfg.Timezone}, {"LISTEN", &cfg.Server.Listen},
		{"DB_FILENAME", &cfg.Database.Filename}, {"PROVIDER", &cfg.Generation.Provider},
		{"PROVIDER_BASE_URL", &cfg.Generation.BaseURL}, {"PROVIDER_MODEL", &cfg.Generation.Model},
		{"DISCONNECT_POLICY", &cfg.Generation.DisconnectPolicy},
		{"STRUCTURED_OUTPUT_MODE", (*string)(&cfg.Generation.StructuredOutputMode)},
	}
	for _, item := range stringValues {
		if value, ok := os.LookupEnv(envPrefix + item.name); ok {
			*item.dst = value
		}
	}
	durations := []struct {
		name string
		dst  *time.Duration
	}{
		{"SHUTDOWN_TIMEOUT", &cfg.Server.ShutdownTimeout},
		{"REQUEST_TIMEOUT", &cfg.Generation.RequestTimeout},
		{"SAFETY_SCAN_INTERVAL", &cfg.Generation.SafetyScanInterval},
	}
	for _, item := range durations {
		if value, ok := os.LookupEnv(envPrefix + item.name); ok {
			if err := parseDurationInto(value, item.dst, envPrefix+item.name); err != nil {
				return err
			}
		}
	}
	ints := []struct {
		name string
		dst  *int
	}{
		{"MAX_ATTEMPTS", &cfg.Generation.MaxAttempts},
		{"MAX_CONCURRENCY", &cfg.Generation.MaxConcurrency},
		{"MAX_INPUT_BYTES", &cfg.Generation.MaxInputBytes},
		{"MAX_OUTPUT_BYTES", &cfg.Generation.MaxOutputBytes},
	}
	for _, item := range ints {
		if value, ok := os.LookupEnv(envPrefix + item.name); ok {
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("parse %s: %w", envPrefix+item.name, err)
			}
			*item.dst = parsed
		}
	}
	if value, ok := os.LookupEnv(envPrefix + "SUPPORTS_JSON_SCHEMA"); ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("parse %sSUPPORTS_JSON_SCHEMA: %w", envPrefix, err)
		}
		cfg.Generation.SupportsJSONSchema = parsed
	}
	providerSecret, err := loadEnvironmentSecret(secret.ProviderAPIKey, "PROVIDER_API_KEY")
	if err != nil {
		return err
	}
	cfg.Generation.APIKey = providerSecret
	ttsSecretConfigured := environmentPresent("TTS_API_KEY") || environmentPresent("TTS_API_KEY_FILE")
	if !cfg.Autonomy.TTS.Enabled {
		if ttsSecretConfigured {
			return errors.New("secret source forbidden: tts_api_key")
		}
		return nil
	}
	if cfg.Autonomy.TTS.Provider != "openai_speech" {
		if ttsSecretConfigured {
			return errors.New("secret source forbidden: tts_api_key")
		}
		return nil
	}
	ttsSecret, err := loadEnvironmentSecret(secret.TTSAPIKey, "TTS_API_KEY")
	if err != nil {
		return err
	}
	cfg.Autonomy.TTS.OpenAISpeech.APIKey = ttsSecret
	return nil
}

func environmentPresent(suffix string) bool {
	_, present := os.LookupEnv(envPrefix + suffix)
	return present
}

func loadEnvironmentSecret(name secret.Name, suffix string) (string, error) {
	direct, directPresent := os.LookupEnv(envPrefix + suffix)
	filePath, filePresent := os.LookupEnv(envPrefix + suffix + "_FILE")
	if directPresent && direct == "" || filePresent && filePath == "" {
		return "", fmt.Errorf("%w: %s", secret.ErrDirectValueInvalid, name)
	}
	if directPresent && filePresent {
		return "", fmt.Errorf("%w: %s", secret.ErrSourceConflict, name)
	}
	value, err := secret.Load(name, direct, filePath)
	if err != nil {
		return "", err
	}
	defer secret.Zero(value)
	return string(value), nil
}

func parseDurationInto(raw string, dst *time.Duration, field string) error {
	if raw == "" {
		return nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("parse %s: %w", field, err)
	}
	*dst = value
	return nil
}

func parsePresentDuration(raw *string, dst *time.Duration, field string) error {
	if raw == nil {
		return nil
	}
	if *raw == "" {
		return fmt.Errorf("parse %s: duration is empty", field)
	}
	return parseDurationInto(*raw, dst, field)
}

func parsePositiveOptionalDuration(raw string, dst *time.Duration, field string) error {
	if raw == "" {
		return nil
	}
	if err := parseDurationInto(raw, dst, field); err != nil {
		return err
	}
	if *dst <= 0 {
		return fmt.Errorf("%s must be positive", field)
	}
	return nil
}

func (c Config) Validate() error {
	if c.DataDir == "" || !filepath.IsAbs(c.DataDir) {
		return errors.New("data_dir must be an absolute path")
	}
	if _, err := time.LoadLocation(c.Timezone); err != nil {
		return fmt.Errorf("invalid timezone %q: %w", c.Timezone, err)
	}
	// Administrative commands also load this configuration but do not bind its
	// dialogue listener. Enforce permission to bind remotely at serve startup.
	if !isNumericListen(c.Server.Listen) {
		return fmt.Errorf("server.listen %q must use a numeric IP and port", c.Server.Listen)
	}
	if c.Server.ShutdownTimeout <= 0 {
		return errors.New("server.shutdown_timeout must be positive")
	}
	if filepath.Base(c.Database.Filename) != c.Database.Filename || c.Database.Filename == "." {
		return errors.New("database.filename must be a filename, not a path")
	}
	if c.Generation.Provider != "chat-completions" {
		return fmt.Errorf("unsupported generation.provider %q", c.Generation.Provider)
	}
	if c.Generation.RequestTimeout <= 0 || c.Generation.SafetyScanInterval <= 0 {
		return errors.New("generation durations must be positive")
	}
	if c.Generation.MaxAttempts < 1 {
		return errors.New("generation.max_attempts must be at least one")
	}
	if len(c.Generation.RetryBackoff) != c.Generation.MaxAttempts-1 {
		return errors.New("generation.retry_backoff must contain max_attempts-1 entries")
	}
	for _, duration := range c.Generation.RetryBackoff {
		if duration <= 0 {
			return errors.New("generation.retry_backoff entries must be positive")
		}
	}
	if c.Generation.MaxConcurrency < 1 || c.Generation.MaxInputBytes < 1 || c.Generation.MaxOutputBytes < 1 {
		return errors.New("generation concurrency and byte limits must be positive")
	}
	if c.Generation.DisconnectPolicy != "continue" {
		return errors.New("only generation.disconnect_policy=continue is supported in M3")
	}
	if _, err := c.Generation.ResolvedStructuredOutputMode(); err != nil {
		return err
	}
	if c.Generation.BaseURL != "" {
		if err := c.Generation.validateProviderURL(); err != nil {
			return err
		}
	}
	if c.Projection.ScanInterval <= 0 || c.Projection.AsOfRefreshInterval <= 0 || c.Projection.RebuildRetryInterval <= 0 || c.Projection.MaxStaleness <= 0 || c.Projection.RebuildTransactionTimeout <= 0 {
		return errors.New("projection durations must be positive")
	}
	for name, override := range c.Projection.Overrides {
		if _, ok := activeProjectionNames[name]; !ok {
			return fmt.Errorf("projection override names an inactive projection %q", name)
		}
		if override.ScanInterval < 0 || override.AsOfRefreshInterval < 0 || override.RebuildRetryInterval < 0 || override.MaxStaleness < 0 {
			return fmt.Errorf("projection override durations for %q must be positive", name)
		}
	}
	if _, err := c.Autonomy.SchedulerPolicy(c.Timezone); err != nil {
		return err
	}
	if err := c.Autonomy.validateTTS(); err != nil {
		return err
	}
	return nil
}

func (a Autonomy) SchedulerPolicy(timezone string) (autonomy.Policy, error) {
	if a.isZero() {
		policy := autonomy.DefaultPolicy(timezone)
		return policy, policy.Validate()
	}
	selfStart, err := autonomy.ParseLocalTime(a.SelfTalk.QuietHoursStart)
	if err != nil {
		return autonomy.Policy{}, err
	}
	selfEnd, err := autonomy.ParseLocalTime(a.SelfTalk.QuietHoursEnd)
	if err != nil {
		return autonomy.Policy{}, err
	}
	initiativeStart, err := autonomy.ParseLocalTime(a.Initiative.QuietHoursStart)
	if err != nil {
		return autonomy.Policy{}, err
	}
	initiativeEnd, err := autonomy.ParseLocalTime(a.Initiative.QuietHoursEnd)
	if err != nil {
		return autonomy.Policy{}, err
	}
	triggers := make([]autonomy.TriggerKind, len(a.Initiative.Triggers))
	for index, raw := range a.Initiative.Triggers {
		triggers[index] = autonomy.TriggerKind(raw)
	}
	policy := autonomy.Policy{
		Version: autonomy.PolicyVersion(a.Version), Timezone: timezone, ScanInterval: a.ScanInterval,
		SelfTalk: autonomy.SelfTalkPolicy{
			Enabled: a.SelfTalk.Enabled, Interval: a.SelfTalk.Interval,
			ConsecutiveLimit: a.SelfTalk.ConsecutiveLimit, HourlyLimit: a.SelfTalk.HourlyLimit,
			DailyLimit: a.SelfTalk.DailyLimit, QuietHours: autonomy.QuietHours{Start: selfStart, End: selfEnd},
		},
		Initiative: autonomy.InitiativePolicy{
			Enabled: a.Initiative.Enabled, MinimumInterval: a.Initiative.MinimumInterval,
			HourlyLimit: a.Initiative.HourlyLimit, DailyLimit: a.Initiative.DailyLimit,
			RecentUserSuppression: a.Initiative.RecentUserSuppression,
			QuietHours:            autonomy.QuietHours{Start: initiativeStart, End: initiativeEnd}, Triggers: triggers,
		},
		Retention: autonomy.RetentionPolicy{
			Mode: autonomy.RetentionMode(a.Retention.Mode), Duration: a.Retention.Duration,
			ScanInterval: a.Retention.ScanInterval, Eligibility: autonomy.RetentionEligibility(a.Retention.Eligibility),
		},
	}
	if err := policy.Validate(); err != nil {
		return autonomy.Policy{}, err
	}
	return policy, nil
}

func (a Autonomy) AnySchedulerFeatureEnabled() bool {
	return a.SelfTalk.Enabled || a.Initiative.Enabled || a.Retention.Mode == string(autonomy.RetentionCandidateAfter)
}

func (a Autonomy) isZero() bool {
	return a.Version == "" && a.ScanInterval == 0 && a.SelfTalk == (AutonomySelfTalk{}) &&
		a.Retention == (AutonomyRetention{}) && !a.Initiative.Enabled && a.Initiative.MinimumInterval == 0 &&
		a.Initiative.HourlyLimit == 0 && a.Initiative.DailyLimit == 0 && a.Initiative.RecentUserSuppression == 0 &&
		a.Initiative.QuietHoursStart == "" && a.Initiative.QuietHoursEnd == "" && len(a.Initiative.Triggers) == 0 &&
		!a.TTS.Enabled && a.TTS.Provider == "" && a.TTS.RequestTimeout == 0 && a.TTS.MaxConcurrency == 0 &&
		a.TTS.QueueCapacity == 0 && a.TTS.MaxAudioBytes == 0 && a.TTS.OpenAISpeech == (OpenAISpeech{}) &&
		a.TTS.AivisSpeech.BaseURL == "" && a.TTS.AivisSpeech.StyleID == nil
}

func (a Autonomy) validateTTS() error {
	if a.isZero() {
		return nil
	}
	tts := a.TTS
	if tts.Provider != "openai_speech" && tts.Provider != "aivis_speech" {
		return fmt.Errorf("unsupported autonomy.tts.provider %q", tts.Provider)
	}
	if tts.RequestTimeout <= 0 || tts.MaxConcurrency < 1 || tts.QueueCapacity < 1 || tts.MaxAudioBytes < 1 {
		return errors.New("autonomy.tts timeout, concurrency, queue capacity, and audio limit must be positive")
	}
	if tts.MaxAudioBytes > 8<<20 {
		return errors.New("autonomy.tts.max_audio_bytes cannot exceed 8388608")
	}
	if tts.OpenAISpeech.BaseURL != "" {
		if err := validateProviderURL(tts.OpenAISpeech.BaseURL); err != nil {
			return fmt.Errorf("autonomy.tts.openai_speech: %w", err)
		}
	}
	if tts.AivisSpeech.BaseURL != "" {
		if err := validateProviderURL(tts.AivisSpeech.BaseURL); err != nil {
			return fmt.Errorf("autonomy.tts.aivis_speech: %w", err)
		}
	}
	if tts.AivisSpeech.StyleID != nil && (*tts.AivisSpeech.StyleID < -1<<31 || *tts.AivisSpeech.StyleID > 1<<31-1) {
		return errors.New("autonomy.tts.aivis_speech.style_id must fit a signed 32-bit integer")
	}
	if !tts.Enabled {
		return nil
	}
	switch tts.Provider {
	case "openai_speech":
		if tts.OpenAISpeech.BaseURL == "" || tts.OpenAISpeech.Model == "" || tts.OpenAISpeech.Voice == "" || tts.OpenAISpeech.APIKey == "" {
			return errors.New("enabled OpenAI speech requires base_url, model, voice, and MAHOROBA_TTS_API_KEY")
		}
	case "aivis_speech":
		if tts.AivisSpeech.BaseURL == "" || tts.AivisSpeech.StyleID == nil {
			return errors.New("enabled AivisSpeech requires base_url and explicit style_id")
		}
		if err := validateAivisLoopbackURL(tts.AivisSpeech.BaseURL); err != nil {
			return err
		}
	}
	return nil
}

func validateAivisLoopbackURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("autonomy.tts.aivis_speech.base_url %q must be a loopback HTTP URL", raw)
	}
	ip := net.ParseIP(u.Hostname())
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("autonomy.tts.aivis_speech.base_url %q must use a numeric loopback host", raw)
	}
	return nil
}

// ResolvedStructuredOutputMode freezes an executable provider mode. The
// public `auto|required|prompt` setting is never persisted in a generation
// envelope: recovered work records only `json_schema` or `prompt`.
func (g Generation) ResolvedStructuredOutputMode() (generation.StructuredOutputMode, error) {
	switch g.StructuredOutputMode {
	case "", StructuredOutputAuto:
		if g.SupportsJSONSchema {
			return generation.StructuredOutputJSONSchema, nil
		}
		return generation.StructuredOutputPrompt, nil
	case StructuredOutputRequired:
		if !g.SupportsJSONSchema {
			return "", errors.New("generation.structured_output_mode=required requires generation.supports_json_schema=true")
		}
		return generation.StructuredOutputJSONSchema, nil
	case StructuredOutputPrompt:
		return generation.StructuredOutputPrompt, nil
	default:
		return "", fmt.Errorf("unsupported generation.structured_output_mode %q", g.StructuredOutputMode)
	}
}

func (c Config) ValidateForServe() error {
	if err := c.Validate(); err != nil {
		return err
	}
	if !c.Server.AllowRemote && !c.Server.ContainerListen && !isLoopbackListen(c.Server.Listen) {
		return fmt.Errorf("server.listen %q is not a loopback address; use --allow-remote for dialogue access or --container-listen with host-loopback port publishing", c.Server.Listen)
	}
	if c.Generation.BaseURL == "" {
		return errors.New("generation.base_url is required for serve")
	}
	if c.Generation.Model == "" {
		return errors.New("generation.model is required for serve")
	}
	return nil
}

func validateProviderURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("invalid generation.base_url %q", raw)
	}
	if u.Scheme == "https" {
		return nil
	}
	if u.Scheme == "http" {
		host := u.Hostname()
		ip := net.ParseIP(host)
		if ip != nil && ip.IsLoopback() {
			return nil
		}
	}
	return errors.New("generation.base_url must use HTTPS, except for a loopback HTTP provider")
}

func isNumericListen(listen string) bool {
	host, port, err := net.SplitHostPort(listen)
	if err != nil || net.ParseIP(host) == nil || port == "" {
		return false
	}
	_, err = strconv.ParseUint(port, 10, 16)
	return err == nil
}

func isLoopbackListen(listen string) bool {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func DefaultConfigPath() string {
	base, err := os.UserConfigDir()
	if err != nil {
		return ""
	}
	return filepath.Join(base, "Mahoroba", "config.toml")
}

func defaultDataDir() string {
	if runtime.GOOS == "windows" {
		if base := os.Getenv("LOCALAPPDATA"); base != "" {
			return filepath.Join(base, "Mahoroba")
		}
	}
	if base := os.Getenv("XDG_DATA_HOME"); base != "" {
		return filepath.Join(base, "mahoroba")
	}
	home, err := os.UserHomeDir()
	if err == nil {
		return filepath.Join(home, ".local", "share", "mahoroba")
	}
	return filepath.Join(os.TempDir(), "mahoroba")
}

func (c Config) DatabasePath() string {
	return filepath.Join(c.DataDir, c.Database.Filename)
}

func (c Config) BlobPath() string {
	return filepath.Join(c.DataDir, "blobs")
}
