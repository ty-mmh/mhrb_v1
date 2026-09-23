package config

import "testing"

func TestContainerListenRequiresExplicitOverrideAndDoesNotEnableRemote(t *testing.T) {
	t.Setenv("MAHOROBA_LISTEN", "0.0.0.0:9876")
	t.Setenv("MAHOROBA_CONTAINER_LISTEN", "true") // Not an accepted environment override.
	path := writeConfig(t, "[generation]\nbase_url = \"http://127.0.0.1:1/v1\"\nmodel = \"test\"\n")
	for _, container := range []bool{false, true} {
		cfg, err := Load(Overrides{ConfigPath: path, ContainerListen: container})
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Server.ContainerListen != container || cfg.Server.AllowRemote {
			t.Fatalf("container override altered Host permission: %+v", cfg.Server)
		}
		if err := cfg.ValidateForServe(); (err == nil) != container {
			t.Fatalf("container=%v: %v", container, err)
		}
	}
	if _, err := Load(Overrides{ConfigPath: writeConfig(t, "[server]\ncontainer_listen = true\n")}); err == nil {
		t.Fatal("TOML unexpectedly enabled container binding")
	}
}
