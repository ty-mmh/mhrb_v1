package main

import (
	"net/http"

	"mahoroba.local/mahoroba/internal/config"
)

func generationHTTPClient(settings config.Generation) *http.Client {
	if !settings.UsesDockerHostHTTP() {
		return &http.Client{}
	}
	// Docker Desktop resolves this explicit host endpoint. Never send this
	// local HTTP traffic through an environment proxy or follow a redirect
	// carrying its prompt or credentials to another endpoint.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}
