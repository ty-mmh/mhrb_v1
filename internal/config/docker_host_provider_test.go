package config

import "testing"

func TestDockerHostProviderRequiresExplicitEnvironmentOptIn(t *testing.T) {
	path := writeConfig(t, "")
	t.Setenv("MAHOROBA_PROVIDER_BASE_URL", "http://host.docker.internal:8080/v1")
	for _, value := range []string{"false", "true"} {
		t.Setenv("MAHOROBA_PROVIDER_ALLOW_DOCKER_HOST_HTTP", value)
		cfg, err := Load(Overrides{ConfigPath: path})
		if value == "false" {
			if err == nil {
				t.Fatal("default URL boundary accepted Docker HTTP")
			}
		} else if err != nil || !cfg.Generation.UsesDockerHostHTTP() || cfg.Server.AllowRemote || cfg.Server.ContainerListen {
			t.Fatalf("provider opt-in changed unrelated permissions: %v / %+v", err, cfg.Server)
		}
	}
	for _, value := range []string{"", "yes", "invalid"} {
		t.Setenv("MAHOROBA_PROVIDER_ALLOW_DOCKER_HOST_HTTP", value)
		if _, err := Load(Overrides{ConfigPath: path}); err == nil {
			t.Fatalf("accepted invalid provider opt-in %q", value)
		}
	}
	t.Setenv("MAHOROBA_PROVIDER_ALLOW_DOCKER_HOST_HTTP", "false")
	t.Setenv("MAHOROBA_PROVIDER_BASE_URL", "https://provider.example/v1")
	if _, err := Load(Overrides{ConfigPath: writeConfig(t, "[generation]\nallow_docker_host_http = true\n")}); err == nil {
		t.Fatal("TOML unexpectedly enabled the environment-only Docker opt-in")
	}
}

func TestDockerHostProviderExactEndpointBoundary(t *testing.T) {
	for _, raw := range []string{
		"http://host.docker.internal:8080/v1", "http://host.docker.internal:1", "http://host.docker.internal:65535/",
	} {
		settings := Generation{BaseURL: raw, AllowDockerHostHTTP: true}
		if !settings.UsesDockerHostHTTP() || settings.validateProviderURL() != nil {
			t.Fatalf("rejected explicit Docker endpoint %q", raw)
		}
		settings.AllowDockerHostHTTP = false
		if settings.UsesDockerHostHTTP() || settings.validateProviderURL() == nil {
			t.Fatalf("accepted Docker endpoint without opt-in: %q", raw)
		}
	}
	for _, raw := range []string{
		"http://host.docker.internal", "http://host.docker.internal:", "http://host.docker.internal:0",
		"http://host.docker.internal:65536", "http://host.docker.internal:http", "http://host.docker.internal:-1",
		"http://HOST.DOCKER.INTERNAL:8080", "http://host.docker.internal.:8080", "http://host.docker.internal.example:8080",
		"http://sub.host.docker.internal:8080", "http://gateway.docker.internal:8080", "http://provider.example:8080",
		"http://192.0.2.1:8080", "http://user:password@host.docker.internal:8080/v1",
		"http://host.docker.internal:8080/v1?key=value", "http://host.docker.internal:8080/v1?",
		"http://host.docker.internal:8080/v1#fragment", "http://host.docker.internal:8080/v1#",
		"ftp://host.docker.internal:8080/v1", "//host.docker.internal:8080/v1",
	} {
		settings := Generation{BaseURL: raw, AllowDockerHostHTTP: true}
		if settings.UsesDockerHostHTTP() || settings.validateProviderURL() == nil {
			t.Fatalf("accepted noncanonical Docker endpoint %q", raw)
		}
	}
	for _, raw := range []string{"https://provider.example/v1", "http://127.0.0.1:8080/v1", "http://[::1]:8080/v1"} {
		settings := Generation{BaseURL: raw, AllowDockerHostHTTP: true}
		if settings.UsesDockerHostHTTP() || settings.validateProviderURL() != nil {
			t.Fatalf("changed existing provider path %q", raw)
		}
	}
}

func TestDockerHostGenerationOptInDoesNotChangeTTSBoundary(t *testing.T) {
	for _, provider := range []string{"openai_speech", "aivis_speech"} {
		cfg := defaults()
		cfg.Generation.AllowDockerHostHTTP = true
		cfg.Generation.BaseURL = "http://host.docker.internal:8080/v1"
		if provider == "openai_speech" {
			cfg.Autonomy.TTS.OpenAISpeech.BaseURL = "http://host.docker.internal:8080/v1"
		} else {
			cfg.Autonomy.TTS.AivisSpeech.BaseURL = "http://host.docker.internal:10101"
		}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("generation opt-in relaxed %s URL validation", provider)
		}
	}
}
