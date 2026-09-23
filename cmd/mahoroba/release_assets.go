package main

import "mahoroba.local/mahoroba/internal/releaseasset"

func init() {
	if len(releaseasset.ThirdPartyLicenses()) == 0 {
		panic("embedded third-party license inventory is empty")
	}
}
