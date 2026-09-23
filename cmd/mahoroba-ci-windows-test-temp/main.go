package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"mahoroba.local/mahoroba/internal/namespacelock"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(arguments []string) int {
	flags := flag.NewFlagSet("mahoroba-ci-windows-test-temp", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	basename := flags.String("basename", "", "unique child basename to create")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 || strings.TrimSpace(*basename) == "" {
		return 2
	}
	path, err := namespacelock.PrepareHostedCITestTemp(*basename)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Println(path)
	return 0
}
