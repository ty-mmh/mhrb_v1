package main

import "net"

// dialogueConnectURL converts wildcard bind addresses into usable local links.
// A specific interface address (including a Tailscale address) stays unchanged.
func dialogueConnectURL(address string) string {
	host, port, err := net.SplitHostPort(address)
	if err == nil {
		if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
			if ip.To4() != nil {
				host = "127.0.0.1"
			} else {
				host = "::1"
			}
			address = net.JoinHostPort(host, port)
		}
	}
	return "http://" + address
}

// Container ports are published on host IPv4 loopback with the same port.
// A Go wildcard listener may report [::] even when configured as 0.0.0.0;
// its browser link must still target the published host address.
func localUIConnectURL(address string, containerListen bool) string {
	if containerListen {
		if _, port, err := net.SplitHostPort(address); err == nil {
			return "http://" + net.JoinHostPort("127.0.0.1", port)
		}
	}
	return dialogueConnectURL(address)
}
