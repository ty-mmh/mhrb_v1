package config

import (
	"net/url"
	"strconv"
	"strings"
)

// UsesDockerHostHTTP is true only for the explicitly enabled, exact Docker
// Desktop host endpoint. The same predicate selects its direct, no-redirect
// HTTP client so URL validation and credential routing cannot disagree.
func (generation Generation) UsesDockerHostHTTP() bool {
	if !generation.AllowDockerHostHTTP {
		return false
	}
	parsed, err := url.Parse(generation.BaseURL)
	if err != nil || parsed.Scheme != "http" || parsed.Hostname() != "host.docker.internal" ||
		parsed.User != nil || parsed.Opaque != "" || parsed.RawQuery != "" || parsed.ForceQuery ||
		strings.Contains(generation.BaseURL, "#") {
		return false
	}
	port := parsed.Port()
	if parsed.Host != "host.docker.internal:"+port || port == "" {
		return false
	}
	numeric, err := strconv.ParseUint(port, 10, 16)
	return err == nil && numeric > 0
}

func (generation Generation) validateProviderURL() error {
	if generation.UsesDockerHostHTTP() {
		return nil
	}
	return validateProviderURL(generation.BaseURL)
}
