package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"strings"

	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/cliresult"
	"mahoroba.local/mahoroba/internal/config"
	diagnosticsservice "mahoroba.local/mahoroba/internal/diagnostics"
	"mahoroba.local/mahoroba/internal/hostlock"
	"mahoroba.local/mahoroba/internal/namespacelock"
	"mahoroba.local/mahoroba/internal/projection"
)

type diagnosticsExecutor func(context.Context, diagnosticsservice.Request) (*cliresult.DiagnosticsResult, error)

func runAdminDiagnostics(ctx context.Context, arguments []string, stdout, stderr io.Writer) int {
	return runAdminDiagnosticsWithExecutor(ctx, arguments, stdout, stderr, diagnosticsservice.Inspect)
}

func runAdminDiagnosticsWithExecutor(
	ctx context.Context,
	arguments []string,
	stdout, stderr io.Writer,
	execute diagnosticsExecutor,
) int {
	flags := flag.NewFlagSet("admin diagnostics", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	all := flags.Bool("all", false, "inspect all residents")
	residentRaw := flags.String("resident", "", "resident ULID")
	format := flags.String("format", "json", "text or json")
	configPath := flags.String("config", "", "TOML configuration path")
	dataDir := flags.String("data-dir", "", "absolute Mahoroba data directory")
	if duplicateDiagnosticsFlag(arguments) || flags.Parse(arguments) != nil || flags.NArg() != 0 ||
		(*all == (*residentRaw != "")) || (*format != "text" && *format != "json") {
		return renderAdminIntegrityEnvelope(stdout, stderr, cliresult.NewFailure(
			cliresult.CommandAdminDiagnostics, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil,
		))
	}
	var residentID *canonical.ID
	if *residentRaw != "" {
		parsed, err := canonical.ParseID(*residentRaw)
		if err != nil {
			return renderAdminIntegrityEnvelope(stdout, stderr, cliresult.NewFailure(
				cliresult.CommandAdminDiagnostics, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil,
			))
		}
		residentID = &parsed
	}
	cfg, err := config.Load(config.Overrides{ConfigPath: *configPath, DataDir: *dataDir})
	if err != nil {
		return renderAdminIntegrityEnvelope(stdout, stderr, cliresult.NewFailure(
			cliresult.CommandAdminDiagnostics, classifyHealthcheckConfigError(err), cliresult.StagePreflight, nil,
		))
	}
	result, err := execute(ctx, diagnosticsservice.Request{
		DataDir: cfg.DataDir, DatabaseFilename: cfg.Database.Filename, BlobRoot: cfg.BlobPath(),
		ResidentID: residentID, MaxAttempts: cfg.Generation.MaxAttempts,
		Timezone:   canonical.MustTimezone(cfg.Timezone),
		Projection: diagnosticsProjectionSchedule(cfg),
	})
	if err != nil {
		code := cliresult.ErrorOperationFailed
		if errors.Is(err, namespacelock.ErrBusy) || errors.Is(err, hostlock.ErrLocked) {
			code = cliresult.ErrorAdminLockBusy
		}
		return renderAdminIntegrityEnvelope(stdout, stderr, cliresult.NewFailure(
			cliresult.CommandAdminDiagnostics, code, cliresult.StagePreflight, nil,
		))
	}
	envelope := cliresult.NewSuccess(cliresult.CommandAdminDiagnostics, result)
	envelope.Warnings = []cliresult.Warning{{
		WarningCode: cliresult.WarningLocalAdminNotAttributed,
		TargetIDs:   []string{},
	}}
	if *format == "json" {
		return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
	}
	// Validate through the exact common renderer and retain its warning stream,
	// but replace the JSON success body with the sole approved text renderer.
	var validatedJSON bytes.Buffer
	exitCode, renderErr := cliresult.Render(&validatedJSON, stderr, envelope)
	if renderErr != nil || exitCode != cliresult.ExitSuccess {
		return cliresult.ExitOperational
	}
	if err := diagnosticsservice.RenderText(stdout, result); err != nil {
		return cliresult.ExitOperational
	}
	return cliresult.ExitSuccess
}

func diagnosticsProjectionSchedule(cfg config.Config) diagnosticsservice.ProjectionSchedule {
	overrides := make(map[projection.Name]projection.ScheduleOverride, len(cfg.Projection.Overrides))
	for name, override := range cfg.Projection.Overrides {
		overrides[projection.Name(name)] = projection.ScheduleOverride{
			ScanInterval: override.ScanInterval, AsOfRefreshInterval: override.AsOfRefreshInterval,
			RebuildRetryInterval: override.RebuildRetryInterval, MaxStaleness: override.MaxStaleness,
		}
	}
	return diagnosticsservice.ProjectionSchedule{
		Clock: canonical.SystemClock{}, ScanInterval: cfg.Projection.ScanInterval,
		AsOfRefreshInterval:  cfg.Projection.AsOfRefreshInterval,
		RebuildRetryInterval: cfg.Projection.RebuildRetryInterval, MaxStaleness: cfg.Projection.MaxStaleness,
		Overrides: overrides,
	}
}

func duplicateDiagnosticsFlag(arguments []string) bool {
	seen := make(map[string]bool, 5)
	for _, argument := range arguments {
		if argument == "--" {
			break
		}
		for _, name := range []string{"--all", "--resident", "--format", "--config", "--data-dir"} {
			if argument == name || strings.HasPrefix(argument, name+"=") {
				if seen[name] {
					return true
				}
				seen[name] = true
			}
		}
	}
	return false
}
