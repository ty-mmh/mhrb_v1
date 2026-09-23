package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"mahoroba.local/mahoroba/internal/app"
	"mahoroba.local/mahoroba/internal/autonomy"
	"mahoroba.local/mahoroba/internal/blob"
	"mahoroba.local/mahoroba/internal/blobgc"
	blobgcservice "mahoroba.local/mahoroba/internal/blobgc/service"
	"mahoroba.local/mahoroba/internal/canonical"
	"mahoroba.local/mahoroba/internal/cliresult"
	"mahoroba.local/mahoroba/internal/config"
	"mahoroba.local/mahoroba/internal/domain"
	exportjsonlservice "mahoroba.local/mahoroba/internal/exportjsonl/service"
	"mahoroba.local/mahoroba/internal/fssecure"
	"mahoroba.local/mahoroba/internal/generation"
	"mahoroba.local/mahoroba/internal/generation/chatcompletions"
	"mahoroba.local/mahoroba/internal/hostlock"
	"mahoroba.local/mahoroba/internal/httpui"
	"mahoroba.local/mahoroba/internal/integrity"
	"mahoroba.local/mahoroba/internal/integrityrun"
	"mahoroba.local/mahoroba/internal/memory"
	"mahoroba.local/mahoroba/internal/operationalmetrics"
	"mahoroba.local/mahoroba/internal/projection"
	"mahoroba.local/mahoroba/internal/readiness"
	"mahoroba.local/mahoroba/internal/runtimegate"
	store "mahoroba.local/mahoroba/internal/store/sqlite"
)

const (
	defaultPrinciples = "Be truthful, explicit about uncertainty, respectful of the owner, and preserve canonical history."
	defaultPersona    = "A calm local resident who answers clearly and concisely."
	defaultMemory     = `{"mandatory_event_types":[],"memory_recall_enabled":false,"version":"memory-policy-v1"}`
)

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Stdout, os.Stderr))
}

func run(parent context.Context, arguments []string, stdout, stderr io.Writer) int {
	if len(arguments) == 0 {
		printUsage(stderr)
		return 2
	}
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(arguments) >= 3 && arguments[0] == "admin" &&
		arguments[1] == "integrity" && arguments[2] == "scan" {
		return runAdminIntegrityScan(ctx, arguments[3:], stdout, stderr)
	}
	if len(arguments) >= 3 && arguments[0] == "admin" &&
		arguments[1] == "recovery" && arguments[2] == "terminalize" {
		return runAdminRecoveryTerminalize(ctx, arguments[3:], stdout, stderr)
	}
	if len(arguments) >= 4 && arguments[0] == "admin" && arguments[1] == "runtime" &&
		arguments[2] == "session-policy" && arguments[3] == "select" {
		return runAdminSessionPolicySelect(ctx, arguments[4:], stdout, stderr)
	}
	if len(arguments) >= 3 && arguments[0] == "admin" && arguments[1] == "erasure" {
		switch arguments[2] {
		case "decide":
			return runAdminErasureDecide(ctx, arguments[3:], stdout, stderr)
		case "apply":
			return runAdminErasureApply(ctx, arguments[3:], stdout, stderr)
		case "plan":
			if len(arguments) >= 4 {
				switch arguments[3] {
				case "content":
					return runAdminErasurePlan(ctx, "content", arguments[4:], stdout, stderr)
				case "resident":
					return runAdminErasurePlan(ctx, "resident", arguments[4:], stdout, stderr)
				}
			}
		}
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(cliresult.CommandUnknown, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}
	if len(arguments) >= 2 && arguments[0] == "admin" && arguments[1] == "diagnostics" {
		return runAdminDiagnostics(ctx, arguments[2:], stdout, stderr)
	}
	if arguments[0] == "healthcheck" {
		return runHealthcheck(ctx, arguments[1:], stdout, stderr)
	}
	if arguments[0] == "export" {
		if len(arguments) >= 2 && arguments[1] == "jsonl" {
			return runExportJSONL(ctx, arguments[2:], stdout, stderr)
		}
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(cliresult.CommandExportJSONL, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}
	if arguments[0] == "blob" && len(arguments) >= 2 && arguments[1] == "gc" {
		return runBlobGC(ctx, arguments[2:], stdout, stderr)
	}
	if arguments[0] == "backup" && len(arguments) >= 2 {
		switch arguments[1] {
		case "create":
			return runBackupCreate(ctx, arguments[2:], stdout, stderr)
		case "verify":
			return runBackupVerify(ctx, arguments[2:], stdout, stderr)
		case "restore":
			return runBackupRestore(ctx, arguments[2:], stdout, stderr)
		}
	}
	var err error
	switch arguments[0] {
	case "serve":
		err = runServe(ctx, arguments[1:], stdout, stderr)
	case "db":
		if len(arguments) > 1 && arguments[1] == "verify" {
			err = runDBVerify(ctx, arguments[2:], stdout, stderr)
		} else {
			err = errors.New("usage: mahoroba db verify [options]")
		}
	case "ledger":
		if len(arguments) > 1 && arguments[1] == "verify" {
			err = runLedgerVerify(ctx, arguments[2:], stdout, stderr)
		} else {
			err = errors.New("usage: mahoroba ledger verify [options]")
		}
	case "blob":
		if len(arguments) > 1 && arguments[1] == "recover" {
			err = runBlobRecover(ctx, arguments[2:], stdout, stderr)
		} else {
			err = errors.New("usage: mahoroba blob recover [options]")
		}
	case "projection":
		if len(arguments) < 2 {
			err = errors.New("usage: mahoroba projection status|rebuild [options]")
		} else {
			switch arguments[1] {
			case "status":
				err = runProjectionStatus(ctx, arguments[2:], stdout, stderr)
			case "rebuild":
				err = runProjectionRebuild(ctx, arguments[2:], stdout, stderr)
			default:
				err = fmt.Errorf("unknown projection operation %q", arguments[1])
			}
		}
	case "admin":
		err = runAdmin(ctx, arguments[1:], stdout, stderr)
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	default:
		err = fmt.Errorf("unknown command %q", arguments[0])
	}
	if err != nil {
		fmt.Fprintln(stderr, "error:", err)
		return 1
	}
	return 0
}

type commonFlags struct {
	configPath      *string
	dataDir         *string
	listen          *string
	timezone        *string
	allowRemote     *bool
	containerListen *bool
}

func addCommon(flags *flag.FlagSet, includeListen bool) commonFlags {
	result := commonFlags{
		configPath: flags.String("config", "", "TOML configuration path"),
		dataDir:    flags.String("data-dir", "", "absolute Mahoroba data directory"),
		timezone:   flags.String("timezone", "", "IANA timezone override"),
	}
	if includeListen {
		result.listen = flags.String("listen", "", "dialogue listen address (loopback unless --allow-remote)")
		result.allowRemote = flags.Bool("allow-remote", false, "allow remote dialogue access; does not expose administration or add authentication")
		result.containerListen = flags.Bool("container-listen", false, "allow container binding only; publish ports on host loopback and retain local Host checks")
	}
	return result
}

func (common commonFlags) load() (config.Config, error) {
	var listen string
	if common.listen != nil {
		listen = *common.listen
	}
	return config.Load(config.Overrides{
		ConfigPath: *common.configPath, DataDir: *common.dataDir, Listen: listen, Timezone: *common.timezone,
		AllowRemote:     common.allowRemote != nil && *common.allowRemote,
		ContainerListen: common.containerListen != nil && *common.containerListen,
	})
}

type runtimeComponents struct {
	app               *app.Application
	blobs             *blob.FileStore
	store             *store.Store
	writer            *canonical.Writer
	projections       *projection.Coordinator
	projectionStarted bool
	integrity         *integrityrun.Service
	metrics           *operationalmetrics.Metrics
	lock              *hostlock.Lock
	databaseBoundary  *store.BoundDatabase
}

func openRuntime(ctx context.Context, cfg config.Config, generator generation.Generator) (*runtimeComponents, error) {
	return openRuntimeWithObserver(ctx, cfg, generator, true, operationalmetrics.New())
}

func openRuntimeForServe(ctx context.Context, cfg config.Config, generator generation.Generator) (*runtimeComponents, error) {
	return openRuntimeWithObserver(ctx, cfg, generator, false, operationalmetrics.New())
}

func openRuntimeWithOptions(ctx context.Context, cfg config.Config, generator generation.Generator, startProjection bool) (*runtimeComponents, error) {
	return openRuntimeWithObserver(ctx, cfg, generator, startProjection, operationalmetrics.New())
}

func openRuntimeWithObserver(
	ctx context.Context,
	cfg config.Config,
	generator generation.Generator,
	startProjection bool,
	metrics *operationalmetrics.Metrics,
) (*runtimeComponents, error) {
	return openRuntimeWithOperationalObserver(ctx, cfg, generator, startProjection, metrics, metrics)
}

func openRuntimeWithOperationalObserver(
	ctx context.Context,
	cfg config.Config,
	generator generation.Generator,
	startProjection bool,
	metrics *operationalmetrics.Metrics,
	operationalObserver app.OperationalObserver,
) (*runtimeComponents, error) {
	if metrics == nil {
		return nil, errors.New("runtime: operational metrics observer is required")
	}
	if operationalObserver == nil {
		return nil, errors.New("runtime: application operational observer is required")
	}
	dataLock, err := hostlock.Acquire(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	if err := ensureRuntimeDatabaseBoundary(cfg.DataDir, cfg.Database.Filename); err != nil {
		_ = dataLock.Close()
		return nil, err
	}
	databaseBoundary, err := store.OpenWritableBoundDatabase(cfg.DataDir, cfg.Database.Filename)
	if err != nil {
		_ = dataLock.Close()
		return nil, fmt.Errorf("runtime database identity boundary: %w", err)
	}
	cfg.DataDir = databaseBoundary.DataDir()
	database, err := store.Open(ctx, cfg.DatabasePath())
	if err != nil {
		_ = databaseBoundary.Close()
		_ = dataLock.Close()
		return nil, err
	}
	cleanup := func() {
		_ = database.Close()
		_ = databaseBoundary.Close()
		_ = dataLock.Close()
	}
	if err := databaseBoundary.Verify(); err != nil {
		cleanup()
		return nil, fmt.Errorf("runtime opened database identity changed: %w", err)
	}
	if err := database.BindCanonicalBoundary(databaseBoundary); err != nil {
		cleanup()
		return nil, fmt.Errorf("runtime bind Canonical database boundary: %w", err)
	}
	timezone, err := canonical.ParseTimezone(cfg.Timezone)
	if err != nil {
		cleanup()
		return nil, err
	}
	blobs, err := blob.NewFileStore(cfg.BlobPath())
	if err != nil {
		cleanup()
		return nil, err
	}
	blobs.SetRecoveryObserver(metrics)
	initialized, err := database.Canonical().BootstrapInitialized(ctx)
	if err != nil {
		cleanup()
		return nil, err
	}
	if err := verifyCanonicalResidents(ctx, database.Canonical()); err != nil {
		cleanup()
		return nil, err
	}
	if err := database.MinimumCheckerWithBlobObjects(blobs).Check(ctx); err != nil {
		cleanup()
		return nil, fmt.Errorf("runtime MinimumCheck: %w", err)
	}
	if err := databaseBoundary.Verify(); err != nil {
		cleanup()
		return nil, fmt.Errorf("runtime preflight database identity changed: %w", err)
	}
	ids := canonical.NewSecureIDGenerator()
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: database.Canonical(), IDs: ids, Clock: canonical.SystemClock{}, Timezone: timezone, QueueCapacity: 128,
		Observer: metrics,
	})
	if err != nil {
		cleanup()
		return nil, err
	}
	if initialized {
		if err := registerDialogueV4(ctx, writer, ids); err != nil {
			_ = writer.Close(context.Background())
			cleanup()
			return nil, fmt.Errorf("runtime register current dialogue pipeline: %w", err)
		}
	}
	integrityService, err := integrityrun.New(integrityrun.Options{
		Writer: writer, Repository: database.Canonical(),
		Scanner: database.IntegrityScanner(), IDs: ids,
	})
	if err != nil {
		_ = writer.Close(context.Background())
		cleanup()
		return nil, err
	}
	projectionStore := database.ProjectionWithOptions(store.ProjectionOptions{
		TransactionTimeout: cfg.Projection.RebuildTransactionTimeout,
	})
	coordinator, err := newProjectionCoordinatorWithObserver(cfg, projectionStore, projectionStore, metrics, func(err error) {
		slog.Error("Projection reconciliation failed", "error", err)
	})
	if err != nil {
		_ = writer.Close(context.Background())
		cleanup()
		return nil, err
	}
	structuredMode, err := cfg.Generation.ResolvedStructuredOutputMode()
	if err != nil {
		_ = writer.Close(context.Background())
		cleanup()
		return nil, err
	}
	autonomyPolicy, err := cfg.Autonomy.SchedulerPolicy(cfg.Timezone)
	if err != nil {
		_ = writer.Close(context.Background())
		cleanup()
		return nil, err
	}
	application, err := app.New(app.Options{
		Writer: writer, CommitNotifier: coordinator, Repository: database.Canonical(), IDs: ids, Clock: canonical.SystemClock{},
		Timezone: timezone, Blobs: blobs, Generator: generator, Provider: cfg.Generation.Provider,
		Model: cfg.Generation.Model, StructuredOutputMode: structuredMode, MaxAttempts: cfg.Generation.MaxAttempts,
		RetryBackoff: cfg.Generation.RetryBackoff, MaxInputBytes: cfg.Generation.MaxInputBytes,
		MaxOutputBytes: cfg.Generation.MaxOutputBytes, SafetyScanInterval: cfg.Generation.SafetyScanInterval,
		AutonomyPolicy: &autonomyPolicy, AutonomySource: database.Canonical(),
		AutonomyClock: autonomy.NewSystemSchedulerClock(), ProjectionMaxStaleness: cfg.Projection.MaxStaleness,
		ErasureCandidateSink: loggingErasureCandidateSink{logger: slog.Default()},
		OperationalObserver:  operationalObserver,
	})
	if err != nil {
		_ = writer.Close(context.Background())
		cleanup()
		return nil, err
	}
	// Runtime shutdown owns Projection cancellation explicitly. In particular,
	// a serve signal first stops and joins Application recovery; only then does
	// runtime.Close stop the Coordinator before closing the Writer and database.
	if startProjection {
		if err := coordinator.Start(context.WithoutCancel(ctx)); err != nil {
			_ = writer.Close(context.Background())
			cleanup()
			return nil, err
		}
	}
	return &runtimeComponents{
		app: application, blobs: blobs, store: database, writer: writer, projections: coordinator,
		projectionStarted: startProjection, integrity: integrityService, metrics: metrics, lock: dataLock,
		databaseBoundary: databaseBoundary,
	}, nil
}

// ensureRuntimeDatabaseBoundary creates a missing Canonical database through
// the exact protected root and rejects an existing symlink, reparse point,
// owner/mode/ACL mismatch, or non-regular entry before SQLite sees a pathname.
// Every runtime additionally retains a compatible identity guard for the
// complete writable lifetime.
func ensureRuntimeDatabaseBoundary(dataDir, databaseFilename string) (resultErr error) {
	policy, err := fssecure.CurrentSecurityPolicy()
	if err != nil {
		return fmt.Errorf("runtime database security policy: %w", err)
	}
	root, err := fssecure.OpenRoot(filepath.Clean(dataDir), policy)
	if err != nil {
		return fmt.Errorf("runtime database root boundary: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, root.Close()) }()
	guard, err := root.OpenRegularIdentityGuard(databaseFilename)
	if err == nil {
		return guard.Close()
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("runtime database identity boundary: %w", err)
	}
	created, err := root.CreateRegular(databaseFilename)
	if err != nil {
		return fmt.Errorf("runtime database protected creation: %w", err)
	}
	return created.Close()
}

// openOfflineWritableStore must only be called while the caller retains the
// data-directory host lock. It rejects path aliases/reparse points before
// SQLite opens the database and retains the exact root-relative identity for
// the complete offline mutation lifetime.
func openOfflineWritableStore(
	ctx context.Context,
	cfg config.Config,
) (*store.Store, *store.BoundDatabase, error) {
	// Offline mutators share the runtime creation contract: while the caller
	// owns the host lock, create a missing Canonical database through the exact
	// protected root before retaining its writable identity guard. Opening the
	// guard first would make first-run integrity/recovery commands fail closed
	// merely because the database does not exist yet.
	if err := ensureRuntimeDatabaseBoundary(cfg.DataDir, cfg.Database.Filename); err != nil {
		return nil, nil, err
	}
	boundary, err := store.OpenWritableBoundDatabase(cfg.DataDir, cfg.Database.Filename)
	if err != nil {
		return nil, nil, fmt.Errorf("offline database identity boundary: %w", err)
	}
	database, err := store.Open(ctx, filepath.Join(boundary.DataDir(), cfg.Database.Filename))
	if err != nil {
		return nil, nil, errors.Join(err, boundary.Close())
	}
	if err := boundary.Verify(); err != nil {
		return nil, nil, errors.Join(
			fmt.Errorf("offline opened database identity changed: %w", err),
			database.Close(), boundary.Close(),
		)
	}
	if err := database.BindCanonicalBoundary(boundary); err != nil {
		return nil, nil, errors.Join(
			fmt.Errorf("offline bind Canonical database boundary: %w", err),
			database.Close(), boundary.Close(),
		)
	}
	return database, boundary, nil
}

func verifyCanonicalResidents(ctx context.Context, repository *store.CanonicalRepository) error {
	residentIDs, err := repository.ListResidentIDs(ctx)
	if err != nil {
		return fmt.Errorf("verify Canonical resident inventory: %w", err)
	}
	verifier := canonical.LedgerVerifier{EnvelopeValidator: repository}
	for _, residentID := range residentIDs {
		if _, err := verifier.Verify(ctx, repository, residentID); err != nil {
			return fmt.Errorf("verify Canonical resident %s: %w", residentID, err)
		}
	}
	return nil
}

type residentExistenceReader interface {
	ResidentExists(context.Context, canonical.ID) (bool, error)
}

func requireResidentExists(ctx context.Context, repository residentExistenceReader, residentID canonical.ID) error {
	exists, err := repository.ResidentExists(ctx, residentID)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("resident %s does not exist", residentID)
	}
	return nil
}

func registerDialogueV4(ctx context.Context, writer *canonical.Writer, ids *canonical.IDGenerator) error {
	pipelineID, err := ids.New()
	if err != nil {
		return fmt.Errorf("allocate dialogue-v4 pipeline ID: %w", err)
	}
	definition, err := domain.DialoguePipelineDefinition(pipelineID, domain.DialoguePipelineVersionV4)
	if err != nil {
		return err
	}
	_, err = writer.Submit(ctx, domain.RegisterPipelineVersionsCommand(domain.RegisterPipelineVersions{
		Versions: []domain.PipelineVersionDefinition{definition},
	}))
	return err
}

func (runtime *runtimeComponents) Close(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	runtime.app.Stop()
	waitErr := runtime.app.Wait(ctx)
	if errors.Is(waitErr, app.ErrApplicationWorkersLive) {
		// A live application worker may still hold or submit Canonical work. Do
		// not race it by closing the Writer/DB underneath it.
		return fmt.Errorf("wait for application shutdown: %w", waitErr)
	}
	var projectionWaitErr error
	if runtime.projectionStarted {
		runtime.projections.Stop()
		projectionWaitErr = runtime.projections.Wait(ctx)
	}
	if projectionWaitErr != nil {
		return errors.Join(waitErr, fmt.Errorf("wait for Projection shutdown: %w", projectionWaitErr))
	}
	if err := runtime.writer.Close(ctx); err != nil {
		return errors.Join(waitErr, projectionWaitErr, fmt.Errorf("close Canonical Writer: %w", err))
	}
	return errors.Join(waitErr, projectionWaitErr, runtime.store.Close(), runtime.databaseBoundary.Close(), runtime.lock.Close())
}

type serveLifecycleHooks struct {
	Ready         func(string)
	ManagementURL string
}

func runServe(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	return runServeWithLifecycle(ctx, arguments, stdout, stderr, serveLifecycleHooks{})
}

func runServeWithLifecycle(ctx context.Context, arguments []string, stdout, stderr io.Writer, hooks serveLifecycleHooks) (returnErr error) {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, true)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	cfg, err := common.load()
	if err != nil {
		return err
	}
	if err := cfg.ValidateForServe(); err != nil {
		return err
	}
	provider, err := chatcompletions.NewWithCapabilities(
		cfg.Generation.BaseURL, cfg.Generation.APIKey,
		cfg.Generation.RequestTimeout, cfg.Generation.MaxConcurrency, generationHTTPClient(cfg.Generation),
		generation.Capabilities{SupportsJSONSchema: cfg.Generation.SupportsJSONSchema},
	)
	if err != nil {
		return err
	}
	metrics := operationalmetrics.New()
	logger := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	operationalObserver := serveOperationalObserver{metrics: metrics, logger: logger}
	reporter, err := operationalmetrics.NewReporter(metrics, operationalmetrics.ReporterOptions{Output: stderr})
	if err != nil {
		return err
	}
	reporterCtx, stopReporter := context.WithCancel(context.WithoutCancel(ctx))
	reporterDone := make(chan error, 1)
	go func() { reporterDone <- reporter.Run(reporterCtx) }()
	defer func() {
		stopReporter()
		returnErr = errors.Join(returnErr, <-reporterDone)
	}()
	runtime, err := openRuntimeWithOperationalObserver(ctx, cfg, provider, false, metrics, operationalObserver)
	if err != nil {
		return err
	}
	healthState := newServeHealthState(runtime)
	hub := httpui.NewHub(128)
	ttsRuntime, err := newServeTTSRuntime(cfg.Autonomy.TTS, hub, logger, &http.Client{})
	if err != nil {
		hub.Close()
		closeErr := runtime.Close(cfg.Server.ShutdownTimeout)
		if closeErr != nil {
			return errors.Join(err, errServeCleanupIncomplete, closeErr)
		}
		return err
	}
	requestCtx, cancelRequests := context.WithCancel(ctx)
	defer cancelRequests()
	gate := runtimegate.New()
	cleanupNeeded := true
	defer func() {
		if cleanupNeeded {
			ttsRuntime.Stop()
			shutdown, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
			ttsErr := ttsRuntime.Wait(shutdown)
			cancel()
			hub.Close()
			closeErr := runtime.Close(cfg.Server.ShutdownTimeout)
			if cleanupErr := errors.Join(ttsErr, closeErr); cleanupErr != nil {
				returnErr = errors.Join(returnErr, errServeCleanupIncomplete, cleanupErr)
			}
		}
	}()
	runtime.app.SetObserver(hubObserver{hub: hub, tts: ttsRuntime})
	server, err := httpui.New(runtime.app, hub, httpui.Options{
		Address: cfg.Server.Listen, BaseContext: requestCtx,
		ManagementURL:       hooks.ManagementURL,
		DialogueAllowRemote: cfg.Server.AllowRemote,
		ContainerListen:     cfg.Server.ContainerListen,
		AudioEnabled:        ttsRuntime != nil,
		MaxMessageBytes:     int64(cfg.Generation.MaxInputBytes),
		ShutdownTimeout:     cfg.Server.ShutdownTimeout,
		Logger:              logger,
		Health:              healthState.Evaluate,
	})
	if err != nil {
		return err
	}
	var preflightEligibility readiness.Result
	var projectionPreflight projection.PreflightResult
	var listener net.Listener
	listenerOwned := false
	defer func() {
		if listenerOwned {
			_ = listener.Close()
		}
	}()
	serveErrors := make(chan error, 1)
	err = runServeStartupSequence(serveStartupOperations{
		StartTTSGated: func() error { return ttsRuntime.StartGated(requestCtx, gate) },
		PreflightMandatory: func() error {
			if err := runtime.app.PreflightMandatory(ctx); err != nil {
				return fmt.Errorf("runtime readiness: mandatory recovery preflight: %w", err)
			}
			return nil
		},
		TerminalizeRunning: func() error { return runtime.app.Prepare(ctx) },
		ScanIntegrity: func(postRecovery bool) error {
			if _, err := runtime.integrity.Run(ctx, nil); err != nil {
				if postRecovery {
					return fmt.Errorf("runtime readiness: post-recovery integrity scan/apply: %w", err)
				}
				return fmt.Errorf("runtime readiness: integrity scan/apply: %w", err)
			}
			return nil
		},
		TerminalizeMandatory: func() (bool, error) {
			mandatoryRecovery, err := runtime.app.RecoverMandatory(ctx)
			if err != nil {
				return false, fmt.Errorf("runtime readiness: mandatory recovery: %w", err)
			}
			// A synthetic cancellation envelope which cannot be reconstructed is
			// a typed integrity predicate. Re-scan it before readiness rather than
			// inventing a generation run.
			return len(mandatoryRecovery.UnresolvedCandidates) != 0, nil
		},
		PreflightEligibility: func() error {
			var err error
			preflightEligibility, err = readiness.EvaluateServiceReadiness(
				ctx,
				runtime.store.ServiceReadinessSource(),
				readiness.Request{
					StartupComplete: true,
					Projection: readiness.ProjectionCheckFunc(func(
						context.Context, readiness.ProjectionRequirement,
					) (bool, error) {
						return false, nil
					}),
				},
			)
			if err != nil {
				return fmt.Errorf("runtime readiness: evaluate preflight eligibility: %w", err)
			}
			if preflightEligibility.ActiveResidentID == nil ||
				preflightEligibility.ActiveResidentStatus != "active" ||
				!preflightEligibility.MemoryPolicyServiceCurrent ||
				preflightEligibility.SessionPolicyID == nil {
				return fmt.Errorf("runtime readiness: service not ready: %s", preflightEligibility.ReasonCodes[0])
			}
			return nil
		},
		// Close and drain the Canonical Writer entrance before taking the
		// Projection/readiness proof. HTTP, TTS, and workers remain gated.
		PauseAdmissions: func() error {
			if err := runtime.writer.PauseAdmissions(ctx); err != nil {
				return fmt.Errorf("runtime readiness: pause Canonical admission: %w", err)
			}
			return nil
		},
		PreflightContentRefs: func() error {
			if _, err := runtime.projections.PreflightContentReferences(ctx); err != nil {
				return fmt.Errorf("runtime GC availability: synchronous content references preflight: %w", err)
			}
			return nil
		},
		PreflightProjection: func() error {
			var err error
			projectionPreflight, err = runtime.projections.PreflightServiceResident(
				ctx, *preflightEligibility.ActiveResidentID,
			)
			if err != nil {
				return fmt.Errorf("runtime readiness: synchronous Projection preflight: %w", err)
			}
			return nil
		},
		EvaluateFinalReadiness: func() error {
			serviceReadiness, err := readiness.EvaluateServiceReadiness(
				ctx,
				runtime.store.ServiceReadinessSource(),
				readiness.Request{
					StartupComplete: true,
					Projection: readiness.CanonicalProjectionProof{
						ResidentID:      projectionPreflight.ResidentID,
						SessionPolicyID: *preflightEligibility.SessionPolicyID,
						Head:            projectionPreflight.Target.Head,
						IsCurrent:       true,
					},
				},
			)
			if err != nil {
				return fmt.Errorf("runtime readiness: final evaluation: %w", err)
			}
			if !serviceReadiness.Ready {
				return fmt.Errorf("runtime readiness: service not ready: %s", serviceReadiness.ReasonCodes[0])
			}
			return nil
		},
		BindHTTP: func() error {
			var err error
			listener, err = net.Listen("tcp", server.Address())
			if err != nil {
				return fmt.Errorf("httpui: bind activation listener: %w", err)
			}
			listenerOwned = true
			return nil
		},
		StartProjection: func() error {
			if err := runtime.projections.Start(context.WithoutCancel(ctx)); err != nil {
				return err
			}
			runtime.projectionStarted = true
			return nil
		},
		WaitProjectionReady: func() error {
			if err := runtime.projections.WaitReady(ctx); err != nil {
				return fmt.Errorf("runtime readiness: Projection Coordinator initialization: %w", err)
			}
			return nil
		},
		StartWorkersGated: func() error { return runtime.app.StartWorkers(gate) },
		// Binding reserves the address, but this goroutine cannot Accept before
		// the same one-shot barrier used by every provider/background worker.
		StartHTTPGated: func() error {
			go func() {
				serveErrors <- serveAfterRuntimeStartGate(ctx, gate, func() error {
					return server.Serve(listener)
				})
			}()
			return nil
		},
		RequireCanonicalHead: func() error {
			if err := runtime.projections.RequireCanonicalHead(ctx, projectionPreflight.Target.Head); err != nil {
				return fmt.Errorf("runtime readiness: activation head verification: %w", err)
			}
			return nil
		},
		// Since every external actor is still gated, reopening admission cannot
		// introduce a head race between the final check and activation.
		ResumeAdmissions: func() error {
			if err := runtime.writer.ResumeAdmissions(); err != nil {
				return fmt.Errorf("runtime readiness: resume Canonical admission: %w", err)
			}
			return nil
		},
		OpenGate: func() error {
			healthState.MarkStarted()
			return gate.Open()
		},
		FailGate: func(cause error) { _ = gate.Fail(cause) },
	})
	if err != nil {
		return err
	}
	serveReturned := make(chan struct{})
	shutdownDone := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
		case <-serveReturned:
		}
		// Voice output is ephemeral. Stop accepting and cancel in-flight
		// synthesis before draining HTTP; this can never affect Canonical text.
		ttsRuntime.Stop()
		// Shutdown does not cancel active request contexts itself. Cancel the
		// server lifetime first so an in-flight POST abandons body, DB, or
		// Canonical work before Shutdown waits for handlers to drain.
		healthState.MarkShutdown()
		cancelRequests()
		runtime.app.Stop()
		shutdown, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
		defer cancel()
		shutdownDone <- server.Shutdown(shutdown)
	}()
	if hooks.Ready != nil {
		hooks.Ready(localUIConnectURL(listener.Addr().String(), cfg.Server.ContainerListen) + "/")
	}
	fmt.Fprintf(stdout, "Mahoroba listening on http://%s\n", listener.Addr().String())
	serveErr := <-serveErrors
	listenerOwned = false
	close(serveReturned)
	// http.Server.Serve returns as soon as Shutdown closes listeners. Always
	// wait for the one coordinated Shutdown pass, including an unexpected
	// listener failure while non-SSE handlers are still active.
	shutdownErr := <-shutdownDone
	ttsRuntime.Stop()
	ttsWaitCtx, cancelTTSWait := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	ttsWaitErr := ttsRuntime.Wait(ttsWaitCtx)
	cancelTTSWait()
	hub.Close()
	closeErr := closeRuntimeAfterHTTPDrain(runtime, shutdownErr, cfg.Server.ShutdownTimeout)
	cleanupNeeded = false
	if ttsWaitErr != nil || closeErr != nil {
		return errors.Join(serveErr, ttsWaitErr, closeErr, errServeCleanupIncomplete)
	}
	return serveErr
}

func serveAfterRuntimeStartGate(ctx context.Context, gate *runtimegate.Gate, serve func() error) error {
	if serve == nil {
		return errors.New("httpui: nil serve function")
	}
	if err := gate.Wait(ctx); err != nil {
		return fmt.Errorf("httpui: runtime activation gate: %w", err)
	}
	return serve()
}

func closeRuntimeAfterHTTPDrain(runtime *runtimeComponents, drainErr error, timeout time.Duration) error {
	if drainErr != nil {
		// net/http deliberately leaves active handlers running when Shutdown's
		// context expires. Closing the Writer or database here would race those
		// handlers. The CLI is terminating, so leave persistence and the host
		// lock owned until process exit releases them safely.
		runtime.app.Stop()
		return fmt.Errorf("HTTP handlers did not drain; persistence remains open until process exit: %w", drainErr)
	}
	return runtime.Close(timeout)
}

type hubObserver struct {
	hub *httpui.Hub
	tts *serveTTSRuntime
}

type loggingErasureCandidateSink struct{ logger *slog.Logger }

func (sink loggingErasureCandidateSink) NotifyErasureCandidate(candidate autonomy.RetentionCandidate) bool {
	logger := sink.logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.Info("Autonomy retention candidate",
		"resident_id", candidate.ResidentID.String(), "event_id", candidate.EventID.String(),
		"content_id", candidate.ContentID.String(), "age_microseconds", candidate.AgeMicroseconds,
		"evidence_references", candidate.References.EvidenceReferences,
		"claim_references", candidate.References.ClaimReferences,
		"generation_input_references", candidate.References.GenerationInputReferences,
		"generation_output_references", candidate.References.GenerationOutputReferences,
		"blocking_reasons", candidate.BlockingReasons,
	)
	return true
}

func (observer hubObserver) UserCommitted(domain.Event) {}
func (observer hubObserver) GenerationStarted(residentID, runID canonical.ID, attempt int64) {
	_ = observer.hub.PublishStatus(residentID, &runID, "generating", fmt.Sprintf("Generating response (attempt %d)", attempt))
}
func (observer hubObserver) GenerationDelta(residentID, runID canonical.ID, text string) {
	_ = observer.hub.PublishProvisional(domain.GenerationPurposeDialogue, residentID, runID, text)
}
func (observer hubObserver) ResidentCommitted(event domain.Event) {
	if err := observer.hub.PublishCommitted(event); err == nil {
		observer.tts.EnqueueCommitted(event)
	}
}
func (observer hubObserver) GenerationFailed(residentID, runID canonical.ID, attempt int64, class string, terminal bool) {
	if terminal {
		_ = observer.hub.PublishError(residentID, &runID, class, "The resident could not complete this response.")
		return
	}
	_ = observer.hub.PublishStatus(residentID, &runID, "retry_pending", fmt.Sprintf("Provider attempt %d failed; retry scheduled.", attempt))
}

func runDBVerify(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("db verify", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	cfg, err := common.load()
	if err != nil {
		return err
	}
	database, err := store.OpenInspection(ctx, cfg.DatabasePath())
	if err != nil {
		return err
	}
	defer database.Close()
	return writeIndentedJSON(stdout, database.SchemaReport())
}

type readOnlyProjectionStore struct {
	*store.ProjectionInspection
}

func (readOnlyProjectionStore) Apply(context.Context, projection.ApplyRequest) error {
	return errors.New("projection status store is query-only")
}

func (readOnlyProjectionStore) Drop(context.Context, projection.DropRequest) error {
	return errors.New("projection status store is query-only")
}

func newProjectionCoordinator(cfg config.Config, source projection.Source, projectionStore projection.Store, onErrors ...func(error)) (*projection.Coordinator, error) {
	return newProjectionCoordinatorWithObserver(cfg, source, projectionStore, nil, onErrors...)
}

func newProjectionCoordinatorWithObserver(
	cfg config.Config,
	source projection.Source,
	projectionStore projection.Store,
	observer interface {
		ProjectionObserved(commitLag uint64, rebuildDuration time.Duration, rebuilt, failed bool)
	},
	onErrors ...func(error),
) (*projection.Coordinator, error) {
	registry, err := store.ActiveProjectionRegistry()
	if err != nil {
		return nil, err
	}
	timezone, err := canonical.ParseTimezone(cfg.Timezone)
	if err != nil {
		return nil, err
	}
	overrides := make(map[projection.Name]projection.ScheduleOverride, len(cfg.Projection.Overrides))
	for name, override := range cfg.Projection.Overrides {
		overrides[projection.Name(name)] = projection.ScheduleOverride{
			ScanInterval: override.ScanInterval, AsOfRefreshInterval: override.AsOfRefreshInterval,
			RebuildRetryInterval: override.RebuildRetryInterval, MaxStaleness: override.MaxStaleness,
		}
	}
	var onError func(error)
	if len(onErrors) > 0 {
		onError = onErrors[0]
	}
	return projection.NewCoordinator(projection.CoordinatorOptions{
		Registry: registry, Source: source, Store: projectionStore,
		Clock: canonical.SystemClock{}, Timezone: timezone,
		ScanInterval: cfg.Projection.ScanInterval, AsOfRefreshInterval: cfg.Projection.AsOfRefreshInterval,
		RebuildRetryInterval: cfg.Projection.RebuildRetryInterval, MaxStaleness: cfg.Projection.MaxStaleness,
		Overrides: overrides, OnError: onError, Observer: observer,
	})
}

func runProjectionStatus(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("projection status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	residentRaw := flags.String("resident", "", "resident ULID; all residents when omitted")
	nameRaw := flags.String("name", "", "Projection name; all active Projections when omitted")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("projection status accepts flags only")
	}
	cfg, err := common.load()
	if err != nil {
		return err
	}
	database, err := store.OpenInspection(ctx, cfg.DatabasePath())
	if err != nil {
		return err
	}
	defer database.Close()
	surface := database.Projection()
	readStore := readOnlyProjectionStore{ProjectionInspection: surface}
	coordinator, err := newProjectionCoordinator(cfg, surface, readStore)
	if err != nil {
		return err
	}
	filter := projection.StatusFilter{}
	if *residentRaw != "" {
		residentID, err := canonical.ParseID(*residentRaw)
		if err != nil {
			return err
		}
		filter.ResidentID = &residentID
	}
	if *nameRaw != "" {
		name := projection.Name(*nameRaw)
		registry, err := store.ActiveProjectionRegistry()
		if err != nil {
			return err
		}
		if _, err := registry.Definition(name); err != nil {
			return err
		}
		filter.Name = &name
	}
	statuses, err := coordinator.Status(ctx, filter)
	if err != nil {
		return err
	}
	return writeIndentedJSON(stdout, statuses)
}

func runProjectionRebuild(ctx context.Context, arguments []string, stdout, stderr io.Writer) (resultErr error) {
	flags := flag.NewFlagSet("projection rebuild", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	residentRaw := flags.String("resident", "", "resident ULID")
	nameRaw := flags.String("name", "", "one active Projection name")
	all := flags.Bool("all", false, "rebuild every active Projection")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("projection rebuild accepts flags only")
	}
	if (*nameRaw == "") == !*all {
		return errors.New("projection rebuild requires exactly one of --name or --all")
	}
	residentID, err := requiredID(*residentRaw, "resident")
	if err != nil {
		return err
	}
	cfg, err := common.load()
	if err != nil {
		return err
	}
	dataLock, err := hostlock.Acquire(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("projection rebuild requires exclusive offline access: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, dataLock.Close()) }()
	database, databaseBoundary, err := openOfflineWritableStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, databaseBoundary.Close()) }()
	defer func() { resultErr = errors.Join(resultErr, database.Close()) }()
	if err := verifyCanonicalResidents(ctx, database.Canonical()); err != nil {
		return err
	}
	if err := databaseBoundary.Verify(); err != nil {
		return fmt.Errorf("projection rebuild database identity changed before rebuild: %w", err)
	}
	surface := database.ProjectionWithOptions(store.ProjectionOptions{TransactionTimeout: cfg.Projection.RebuildTransactionTimeout})
	coordinator, err := newProjectionCoordinator(cfg, surface, surface)
	if err != nil {
		return err
	}
	if *all {
		err = coordinator.RebuildAll(ctx, residentID)
	} else {
		name := projection.Name(*nameRaw)
		err = coordinator.Rebuild(ctx, residentID, name)
	}
	if err != nil {
		return err
	}
	if err := databaseBoundary.Verify(); err != nil {
		return fmt.Errorf("projection rebuild database identity changed after rebuild: %w", err)
	}
	statuses, err := coordinator.Status(ctx, projection.StatusFilter{ResidentID: &residentID})
	if err != nil {
		return err
	}
	return writeIndentedJSON(stdout, map[string]any{"rebuilt": true, "status": statuses})
}

func runLedgerVerify(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("ledger verify", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	residentRaw := flags.String("resident", "", "resident ULID; all residents when omitted")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	cfg, err := common.load()
	if err != nil {
		return err
	}
	database, err := store.OpenInspection(ctx, cfg.DatabasePath())
	if err != nil {
		return err
	}
	defer database.Close()
	repository := database.Canonical()
	residentIDs, err := repository.ListResidentIDs(ctx)
	if err != nil {
		return err
	}
	if *residentRaw != "" {
		id, err := canonical.ParseID(*residentRaw)
		if err != nil {
			return err
		}
		if err := requireResidentExists(ctx, repository, id); err != nil {
			return err
		}
		residentIDs = []canonical.ID{id}
	}
	verifier := canonical.LedgerVerifier{EnvelopeValidator: repository}
	reports := make([]canonical.LedgerVerification, 0, len(residentIDs))
	for _, residentID := range residentIDs {
		report, err := verifier.Verify(ctx, repository, residentID)
		if err != nil {
			return err
		}
		reports = append(reports, report)
	}
	output := make([]map[string]any, 0, len(reports))
	for _, report := range reports {
		item := map[string]any{
			"resident_id":   report.ResidentID.String(),
			"content_count": report.ContentCount.Int64(),
			"event_count":   report.EventCount.Int64(),
		}
		if report.EventCount > 0 {
			item["first_seq"] = report.FirstSeq.Int64()
			item["last_seq"] = report.LastSeq.Int64()
			item["head_hash"] = report.HeadHash.Hex()
		}
		output = append(output, item)
	}
	return writeIndentedJSON(stdout, output)
}

func runBlobRecover(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("blob recover", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	cfg, err := common.load()
	if err != nil {
		return err
	}
	dataLock, err := hostlock.Acquire(cfg.DataDir)
	if err != nil {
		return err
	}
	defer dataLock.Close()
	// Recovery is deliberately a standalone, writer-quiescent command: no
	// Canonical Writer or Application is opened while reference checks and
	// physical cleanup run.
	databaseBoundary, err := store.OpenReadOnlyBoundDatabase(ctx, cfg.DataDir, cfg.Database.Filename)
	if err != nil {
		return err
	}
	defer databaseBoundary.Close()
	database := databaseBoundary.Inspection()
	objects, err := blob.NewFileStore(filepath.Join(databaseBoundary.DataDir(), "blobs"))
	if err != nil {
		return err
	}
	if err := databaseBoundary.Verify(); err != nil {
		return fmt.Errorf("blob recovery database identity changed before enumeration: %w", err)
	}
	orphans, err := objects.ListOrphans(ctx)
	if err != nil {
		return err
	}
	results := make([]map[string]any, 0, len(orphans))
	for _, orphan := range orphans {
		if err := databaseBoundary.Verify(); err != nil {
			return fmt.Errorf("blob recovery database identity changed before candidate %q: %w", orphan.Name, err)
		}
		result, err := objects.CleanupOrphan(ctx, orphan, database.Canonical())
		if err != nil {
			return fmt.Errorf("recover blob candidate %q: %w", orphan.Name, err)
		}
		if err := databaseBoundary.Verify(); err != nil {
			return fmt.Errorf("blob recovery database identity changed after candidate %q: %w", orphan.Name, err)
		}
		item := map[string]any{
			"name": orphan.Name, "kind": orphan.Kind,
			"disposition": result.Disposition,
		}
		if !result.ResidentID.IsZero() {
			item["resident_id"] = result.ResidentID.String()
			item["digest"] = result.Digest.String()
		}
		results = append(results, item)
	}
	if err := databaseBoundary.Verify(); err != nil {
		return fmt.Errorf("blob recovery database identity changed before result: %w", err)
	}
	return writeIndentedJSON(stdout, map[string]any{"recovered": len(results), "results": results})
}

func runAdmin(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	if len(arguments) > 0 && arguments[0] == "healthcheck" {
		return runAdminHealthcheck(ctx, arguments[1:], stdout, stderr)
	}
	if len(arguments) > 0 && arguments[0] == "serve" {
		return runAdminServe(ctx, arguments[1:], stdout, stderr)
	}
	if len(arguments) < 2 {
		return errors.New("usage: mahoroba admin bootstrap|resident|memory|autonomy <operation> [options]")
	}
	if arguments[0] == "autonomy" {
		switch arguments[1] {
		case "status":
			return runAutonomyStatus(ctx, arguments[2:], stdout, stderr)
		case "retention":
			if len(arguments) >= 3 && arguments[2] == "candidates" {
				return runAutonomyRetentionCandidates(ctx, arguments[3:], stdout, stderr)
			}
		}
	}
	if len(arguments) >= 3 && arguments[0] == "memory" && arguments[1] == "policy" && arguments[2] == "activate-v0" {
		return runMemoryPolicyActivate(ctx, arguments[3:], stdout, stderr)
	}
	if len(arguments) >= 3 && arguments[0] == "memory" && arguments[1] == "policy" && arguments[2] == "activate-autonomy-v0" {
		return runAutonomyMemoryPolicyActivate(ctx, arguments[3:], stdout, stderr)
	}
	if len(arguments) >= 3 && arguments[0] == "memory" && arguments[1] == "policy" && arguments[2] == "activate-v4" {
		return runMemoryPolicyActivateV4(ctx, arguments[3:], stdout, stderr)
	}
	if len(arguments) >= 3 && arguments[0] == "memory" && arguments[1] == "claim" {
		switch arguments[2] {
		case "list", "show":
			return runMemoryClaimQuery(ctx, arguments[2], arguments[3:], stdout, stderr)
		case "scope":
			return runMemoryClaimScope(ctx, arguments[3:], stdout, stderr)
		case "status":
			return runMemoryClaimStatus(ctx, arguments[3:], stdout, stderr)
		}
	}
	if len(arguments) >= 3 && arguments[0] == "memory" && arguments[1] == "persona" {
		switch arguments[2] {
		case "list":
			return runMemoryPersonaList(ctx, arguments[3:], stdout, stderr)
		case "propose":
			return runMemoryPersonaPropose(ctx, arguments[3:], stdout, stderr)
		}
	}
	if len(arguments) >= 2 && arguments[0] == "memory" && arguments[1] == "reextract" {
		return runMemoryReextract(ctx, arguments[2:], stdout, stderr)
	}
	if len(arguments) >= 2 && arguments[0] == "memory" && (arguments[1] == "abstract" || arguments[1] == "split") {
		return runMemoryDerive(ctx, arguments[1], arguments[2:], stdout, stderr)
	}
	switch arguments[0] + " " + arguments[1] {
	case "bootstrap init":
		return runBootstrapInit(ctx, arguments[2:], stdout, stderr)
	case "bootstrap approve":
		return runBootstrapApprove(ctx, arguments[2:], stdout, stderr)
	case "bootstrap finalize":
		return runBootstrapFinalize(ctx, arguments[2:], stdout, stderr)
	case "resident list", "resident show", "resident select", "resident archive":
		return runResidentAdmin(ctx, arguments[1], arguments[2:], stdout, stderr)
	default:
		return fmt.Errorf("unknown admin operation %q", strings.Join(arguments[:2], " "))
	}
}

func runAdminIntegrityScan(ctx context.Context, arguments []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("admin integrity scan", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	all := flags.Bool("all", false, "scan every resident")
	residentRaw := flags.String("resident", "", "resident ULID")
	configPath := flags.String("config", "", "TOML configuration path")
	dataDir := flags.String("data-dir", "", "absolute Mahoroba data directory")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 || (*all == (*residentRaw != "")) {
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(cliresult.CommandAdminIntegrityScan, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}
	var residentFilter *canonical.ID
	if *residentRaw != "" {
		residentID, err := canonical.ParseID(*residentRaw)
		if err != nil {
			return renderAdminIntegrityEnvelope(stdout, stderr,
				cliresult.NewFailure(cliresult.CommandAdminIntegrityScan, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
		}
		residentFilter = &residentID
	}
	cfg, err := config.Load(config.Overrides{ConfigPath: *configPath, DataDir: *dataDir})
	if err != nil {
		envelope := cliresult.NewFailure(
			cliresult.CommandAdminIntegrityScan, cliresult.ErrorConfigurationInvalid, cliresult.StagePreflight, nil,
		)
		if residentFilter != nil {
			envelope.TargetIDs = []string{residentFilter.String()}
		}
		return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
	}

	result, code, stage, operationErr := executeAdminIntegrityScan(ctx, cfg, residentFilter)
	resultEnvelope := integrityScanCLIResult(result)
	commits := integrityScanCLICommits(result.CanonicalCommits)
	targetIDs := []string{}
	if residentFilter != nil {
		targetIDs = append(targetIDs, residentFilter.String())
	}
	warnings := []cliresult.Warning{{
		WarningCode: cliresult.WarningLocalAdminNotAttributed,
		TargetIDs:   []string{},
	}}
	if operationErr == nil {
		envelope := cliresult.NewSuccess(cliresult.CommandAdminIntegrityScan, resultEnvelope)
		envelope.CanonicalCommits = commits
		envelope.CanonicalApplied = len(commits) > 0
		envelope.Warnings = warnings
		return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
	}
	if len(commits) > 0 {
		envelope := cliresult.NewPartial(cliresult.CommandAdminIntegrityScan, code, stage, resultEnvelope)
		envelope.CanonicalCommits = commits
		envelope.CanonicalApplied = true
		envelope.TargetIDs = targetIDs
		envelope.Warnings = warnings
		return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
	}
	envelope := cliresult.NewFailure(cliresult.CommandAdminIntegrityScan, code, stage, nil)
	envelope.TargetIDs = targetIDs
	envelope.Warnings = warnings
	return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
}

func executeAdminIntegrityScan(
	ctx context.Context,
	cfg config.Config,
	residentFilter *canonical.ID,
) (integrityrun.Result, cliresult.ErrorCode, cliresult.Stage, error) {
	var result integrityrun.Result
	dataLock, err := hostlock.Acquire(cfg.DataDir)
	if err != nil {
		code := cliresult.ErrorOperationFailed
		if errors.Is(err, hostlock.ErrLocked) {
			code = cliresult.ErrorAdminLockBusy
		}
		return result, code, cliresult.StagePreflight, err
	}
	database, databaseBoundary, err := openOfflineWritableStore(ctx, cfg)
	if err != nil {
		return result, cliresult.ErrorIntegrityFatal, cliresult.StagePreflight,
			errors.Join(err, dataLock.Close())
	}
	closeStoreAndLock := func() error {
		return errors.Join(database.Close(), databaseBoundary.Close(), dataLock.Close())
	}
	timezone, err := canonical.ParseTimezone(cfg.Timezone)
	if err != nil {
		return result, cliresult.ErrorConfigurationInvalid, cliresult.StagePreflight,
			errors.Join(err, closeStoreAndLock())
	}
	blobs, err := blob.NewFileStore(cfg.BlobPath())
	if err != nil {
		return result, cliresult.ErrorSourceUnavailable, cliresult.StagePreflight,
			errors.Join(err, closeStoreAndLock())
	}
	if err := verifyCanonicalResidents(ctx, database.Canonical()); err != nil {
		return result, cliresult.ErrorIntegrityFatal, cliresult.StagePreflight,
			errors.Join(err, closeStoreAndLock())
	}
	if err := database.MinimumCheckerWithBlobObjects(blobs).Check(ctx); err != nil {
		return result, cliresult.ErrorIntegrityFatal, cliresult.StagePreflight,
			errors.Join(err, closeStoreAndLock())
	}
	if err := databaseBoundary.Verify(); err != nil {
		return result, cliresult.ErrorIntegrityFatal, cliresult.StagePreflight,
			errors.Join(fmt.Errorf("integrity scan database identity changed during preflight: %w", err),
				closeStoreAndLock())
	}
	if residentFilter != nil {
		if err := requireResidentExists(ctx, database.Canonical(), *residentFilter); err != nil {
			return result, cliresult.ErrorSourceUnavailable, cliresult.StagePreflight,
				errors.Join(err, closeStoreAndLock())
		}
	}
	ids := canonical.NewSecureIDGenerator()
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: database.Canonical(), IDs: ids, Clock: canonical.SystemClock{},
		Timezone: timezone, QueueCapacity: 128,
	})
	if err != nil {
		return result, cliresult.ErrorOperationFailed, cliresult.StagePreflight,
			errors.Join(err, closeStoreAndLock())
	}
	service, err := integrityrun.New(integrityrun.Options{
		Writer: writer, Repository: database.Canonical(),
		Scanner: database.IntegrityScanner(), IDs: ids,
	})
	if err == nil {
		if boundaryErr := databaseBoundary.Verify(); boundaryErr != nil {
			err = fmt.Errorf("integrity scan database identity changed before scan: %w", boundaryErr)
		}
	}
	if err == nil {
		result, err = service.Run(ctx, residentFilter)
	}
	if boundaryErr := databaseBoundary.Verify(); boundaryErr != nil {
		err = errors.Join(err,
			fmt.Errorf("integrity scan database identity changed after scan: %w", boundaryErr))
	}
	closeErr := errors.Join(writer.Close(context.WithoutCancel(ctx)), closeStoreAndLock())
	if err != nil {
		return result, cliresult.ErrorIntegrityApplyConflict, cliresult.StageCanonical,
			errors.Join(err, closeErr)
	}
	if closeErr != nil {
		return result, cliresult.ErrorOperationFailed, cliresult.StageCanonical, closeErr
	}
	return result, "", "", nil
}

func integrityScanCLIResult(result integrityrun.Result) *cliresult.IntegrityScanResult {
	return &cliresult.IntegrityScanResult{
		CapturedHead:       integrityScanCLIHead(result.CapturedHead),
		ResultHead:         integrityScanCLIHead(result.ResultHead),
		ExistingFindings:   fmt.Sprint(result.ExistingFindings),
		CreatedFindings:    fmt.Sprint(result.CreatedFindings),
		CreatedQuarantines: fmt.Sprint(result.CreatedQuarantines),
		ReadinessBlocked:   result.ReadinessBlocked,
	}
}

func integrityScanCLIHead(metadata integrity.HeadMetadata) cliresult.Head {
	if !metadata.Head.Exists {
		return cliresult.Head{}
	}
	commitID := metadata.CommitID.String()
	commitSeq := metadata.Head.CommitSeq.String()
	committedAt := metadata.Head.CommittedAt.String()
	committedTZ := metadata.CommittedTZ.String()
	return cliresult.Head{
		Exists: true, CommitID: &commitID, CommitSeq: &commitSeq,
		CommittedAt: &committedAt, CommittedTZ: &committedTZ,
	}
}

func integrityScanCLICommits(commits []integrityrun.Commit) []cliresult.CanonicalCommit {
	result := make([]cliresult.CanonicalCommit, 0, len(commits))
	for _, commit := range commits {
		var residentID *string
		if scopedID, scoped := commit.Metadata.Scope.ResidentID(); scoped {
			value := scopedID.String()
			residentID = &value
		}
		effects := make([]cliresult.EffectCode, 0, 3)
		if commit.Effects.ClaimStatusQuarantined {
			effects = append(effects, cliresult.EffectClaimStatusQuarantined)
		}
		if commit.Effects.IntegrityFindingRecorded {
			effects = append(effects, cliresult.EffectIntegrityFindingRecorded)
		}
		if commit.Effects.PipelineVersionRegistered {
			effects = append(effects, cliresult.EffectPipelineVersionRegistered)
		}
		result = append(result, cliresult.CanonicalCommit{
			ResidentID: residentID, CommitID: commit.Metadata.CommitID.String(),
			CommitSeq: commit.Metadata.CommitSeq.String(), Disposition: cliresult.Disposition(commit.Disposition),
			Effects: effects,
		})
	}
	return result
}

func renderAdminIntegrityEnvelope(stdout, stderr io.Writer, envelope cliresult.Envelope) int {
	exitCode, err := cliresult.Render(stdout, stderr, envelope)
	if err != nil {
		return cliresult.ExitOperational
	}
	return exitCode
}

type exportJSONLExecutor func(context.Context, exportjsonlservice.Request) (exportjsonlservice.Result, error)

func runExportJSONL(ctx context.Context, arguments []string, stdout, stderr io.Writer) int {
	return runExportJSONLWithExecutor(ctx, arguments, stdout, stderr, exportjsonlservice.Create)
}

// runExportJSONLWithExecutor keeps strict parsing and the exact cliresult
// renderer on the production path while tests can exercise every outcome
// without depending on a platform ACL sandbox.
func runExportJSONLWithExecutor(
	ctx context.Context,
	arguments []string,
	stdout, stderr io.Writer,
	execute exportJSONLExecutor,
) int {
	flags := flag.NewFlagSet("export jsonl", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	output := flags.String("output", "", "absolute JSONL artifact path")
	configPath := flags.String("config", "", "TOML configuration path")
	dataDir := flags.String("data-dir", "", "absolute Mahoroba data directory")
	if duplicateExportJSONLFlag(arguments) || flags.Parse(arguments) != nil || flags.NArg() != 0 ||
		*output == "" || !filepath.IsAbs(*output) {
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(cliresult.CommandExportJSONL, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}
	cfg, err := config.Load(config.Overrides{ConfigPath: *configPath, DataDir: *dataDir})
	if err != nil {
		return renderAdminIntegrityEnvelope(stdout, stderr, cliresult.NewFailure(
			cliresult.CommandExportJSONL, cliresult.ErrorConfigurationInvalid, cliresult.StagePreflight, nil,
		))
	}
	artifactPath := filepath.Clean(*output)
	result, operationErr := execute(ctx, exportjsonlservice.Request{
		SourceDataDir: cfg.DataDir, DatabaseFilename: cfg.Database.Filename, Output: artifactPath,
	})
	if operationErr != nil {
		code, stage := classifyExportJSONLError(operationErr)
		failureResult := &cliresult.ErrorResult{ErrorStage: stage}
		if code == cliresult.ErrorArtifactTargetExists || code == cliresult.ErrorArtifactIOFailed ||
			code == cliresult.ErrorPublishDurabilityUnknown {
			failureResult.ArtifactPath = &artifactPath
		}
		if code == cliresult.ErrorPublishDurabilityUnknown {
			published := false
			failureResult.Published = &published
		}
		envelope := cliresult.NewFailure(cliresult.CommandExportJSONL, code, stage, failureResult)
		if code == cliresult.ErrorPublishDurabilityUnknown {
			action, actionErr := cliresult.NewRequiredAction(cliresult.ActionRepairStaticDesign, nil, nil, nil)
			if actionErr != nil {
				return cliresult.ExitOperational
			}
			envelope.RequiredActions = []cliresult.RequiredAction{action}
		}
		return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
	}
	warning, err := cliresult.NewWarning(cliresult.WarningRuntimeUnmanagedCopy)
	if err != nil {
		return cliresult.ExitOperational
	}
	envelope := cliresult.NewSuccess(cliresult.CommandExportJSONL, &cliresult.ExportJSONLResult{
		ArtifactPath:       result.ArtifactPath,
		FormatVersion:      result.FormatVersion,
		CapturedHead:       exportJSONLCLIHead(result.CapturedHead),
		RecordCount:        strconv.FormatInt(result.RecordCount, 10),
		ByteCount:          strconv.FormatInt(result.ByteCount, 10),
		ExternalCopyNotice: cliresult.ExternalCopyNotice,
	})
	envelope.Warnings = []cliresult.Warning{warning}
	return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
}

func duplicateExportJSONLFlag(arguments []string) bool {
	seen := map[string]bool{}
	for _, argument := range arguments {
		if argument == "--" {
			break
		}
		for _, name := range []string{"--output", "--config", "--data-dir"} {
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

func classifyExportJSONLError(err error) (cliresult.ErrorCode, cliresult.Stage) {
	switch {
	case errors.Is(err, hostlock.ErrLocked):
		return cliresult.ErrorAdminLockBusy, cliresult.StagePreflight
	case errors.Is(err, exportjsonlservice.ErrArtifactTargetExists):
		return cliresult.ErrorArtifactTargetExists, cliresult.StagePreflight
	case errors.Is(err, exportjsonlservice.ErrDurabilityUnknown):
		return cliresult.ErrorPublishDurabilityUnknown, cliresult.StagePublish
	case errors.Is(err, integrity.ErrFatal), errors.Is(err, exportjsonlservice.ErrContentIntegrity):
		return cliresult.ErrorIntegrityFatal, cliresult.StagePreflight
	case errors.Is(err, exportjsonlservice.ErrArtifactIO):
		return cliresult.ErrorArtifactIOFailed, cliresult.StagePublish
	case errors.Is(err, exportjsonlservice.ErrSourceUnavailable), errors.Is(err, exportjsonlservice.ErrSchemaCoverage):
		return cliresult.ErrorSourceUnavailable, cliresult.StagePreflight
	default:
		return cliresult.ErrorOperationFailed, cliresult.StagePreflight
	}
}

func exportJSONLCLIHead(head exportjsonlservice.CapturedHead) cliresult.Head {
	if !head.Exists {
		return cliresult.Head{}
	}
	commitID, commitSeq := head.CommitID.String(), head.CommitSeq.String()
	committedAt, committedTZ := head.CommittedAt.String(), head.CommittedTZ.String()
	return cliresult.Head{
		Exists: true, CommitID: &commitID, CommitSeq: &commitSeq,
		CommittedAt: &committedAt, CommittedTZ: &committedTZ,
	}
}

type blobGCExecutor func(context.Context, blobgcservice.Request) (blobgc.Result, error)

func runBlobGC(ctx context.Context, arguments []string, stdout, stderr io.Writer) int {
	return runBlobGCWithExecutor(ctx, arguments, stdout, stderr, blobgcservice.Run)
}

func runBlobGCWithExecutor(
	ctx context.Context,
	arguments []string,
	stdout, stderr io.Writer,
	execute blobGCExecutor,
) int {
	flags := flag.NewFlagSet("blob gc", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	residentRaw := flags.String("resident", "", "resident ULID")
	apply := flags.Bool("apply", false, "delete the freshly recomputed candidate set")
	confirm := flags.String("confirm", "", "exact sha256 plan digest")
	configPath := flags.String("config", "", "TOML configuration path")
	dataDir := flags.String("data-dir", "", "absolute Mahoroba data directory")
	if duplicateBlobGCFlag(arguments) || flags.Parse(arguments) != nil || flags.NArg() != 0 ||
		*residentRaw == "" || (*apply != (*confirm != "")) {
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(cliresult.CommandBlobGC, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}
	residentID, err := canonical.ParseID(*residentRaw)
	if err != nil || (*confirm != "" && !validBlobGCConfirm(*confirm)) {
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(cliresult.CommandBlobGC, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}
	cfg, err := config.Load(config.Overrides{ConfigPath: *configPath, DataDir: *dataDir})
	if err != nil {
		envelope := cliresult.NewFailure(cliresult.CommandBlobGC, classifyHealthcheckConfigError(err), cliresult.StagePreflight, nil)
		envelope.TargetIDs = []string{residentID.String()}
		return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
	}
	result, operationErr := execute(ctx, blobgcservice.Request{
		SourceDataDir: cfg.DataDir, DatabaseFilename: cfg.Database.Filename,
		ResidentID: residentID, Apply: *apply, Confirm: *confirm, Observer: operationalmetrics.New(),
	})
	if operationErr == nil {
		envelope := cliresult.NewSuccess(cliresult.CommandBlobGC, blobGCCLIResult(result))
		envelope.Warnings = blobGCWarnings(result, residentID)
		return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
	}
	if errors.Is(operationErr, blobgc.ErrPartial) {
		code, stage := classifyBlobGCError(operationErr, result)
		envelope := cliresult.NewPartial(cliresult.CommandBlobGC, code, stage, blobGCCLIResult(result))
		envelope.TargetIDs = []string{residentID.String()}
		envelope.Warnings = blobGCWarnings(result, residentID)
		action, actionErr := blobGCRerunAction(*configPath, cfg.DataDir, residentID)
		if actionErr != nil {
			return cliresult.ExitOperational
		}
		envelope.RequiredActions = []cliresult.RequiredAction{action}
		return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
	}
	code, stage := classifyBlobGCError(operationErr, result)
	envelope := cliresult.NewFailure(cliresult.CommandBlobGC, code, stage, nil)
	envelope.TargetIDs = []string{residentID.String()}
	return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
}

func validBlobGCConfirm(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	_, err := canonical.ParseDigestHex(strings.TrimPrefix(value, "sha256:"))
	return err == nil
}

func duplicateBlobGCFlag(arguments []string) bool {
	seen := make(map[string]bool)
	for _, argument := range arguments {
		if argument == "--" {
			break
		}
		for _, name := range []string{"--resident", "--apply", "--confirm", "--config", "--data-dir"} {
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

func blobGCCLIResult(result blobgc.Result) *cliresult.BlobGCResult {
	return &cliresult.BlobGCResult{
		ResidentID: result.Plan.ResidentID.String(), CapturedHead: blobGCCLIHead(result.Plan.CapturedHead),
		PlanDigest: result.Plan.Digest(), CandidateCount: strconv.FormatInt(result.CandidateCount, 10),
		CandidateBytes: strconv.FormatInt(result.CandidateBytes, 10), DeletedCount: strconv.FormatInt(result.DeletedCount, 10),
		RemainingCount: strconv.FormatInt(result.RemainingCount, 10),
	}
}

func blobGCCLIHead(head blobgc.CapturedHead) cliresult.Head {
	if !head.Exists {
		return cliresult.Head{}
	}
	commitID, commitSeq := head.CommitID.String(), head.CommitSeq.String()
	committedAt, committedTZ := head.CommittedAt.String(), head.CommittedTZ.String()
	return cliresult.Head{
		Exists: true, CommitID: &commitID, CommitSeq: &commitSeq,
		CommittedAt: &committedAt, CommittedTZ: &committedTZ,
	}
}

func blobGCWarnings(result blobgc.Result, residentID canonical.ID) []cliresult.Warning {
	warnings := make([]cliresult.Warning, 0, 2)
	if result.MaintenanceIncomplete {
		warning, _ := cliresult.NewWarning(cliresult.WarningGCMaintenanceIncomplete, residentID.String())
		warnings = append(warnings, warning)
	}
	physical, _ := cliresult.NewWarning(cliresult.WarningPhysicalErasureNotGuaranteed, residentID.String())
	warnings = append(warnings, physical)
	return warnings
}

func classifyBlobGCError(err error, result blobgc.Result) (cliresult.ErrorCode, cliresult.Stage) {
	switch {
	case errors.Is(err, hostlock.ErrLocked):
		return cliresult.ErrorAdminLockBusy, cliresult.StagePreflight
	case errors.Is(err, blobgc.ErrProjectionNotCurrent), errors.Is(err, projection.ErrProjectionNotCurrent):
		return cliresult.ErrorProjectionNotCurrent, cliresult.StageDerived
	case errors.Is(err, blobgc.ErrPlanStale):
		stage := cliresult.StagePreflight
		if result.PhysicalMutation || result.DeletedCount > 0 {
			stage = cliresult.StageCanonical
		}
		return cliresult.ErrorGCPlanStale, stage
	case errors.Is(err, integrity.ErrFatal), errors.Is(err, blobgc.ErrContentIntegrity):
		return cliresult.ErrorIntegrityFatal, cliresult.StagePreflight
	case errors.Is(err, blobgc.ErrSourceUnavailable):
		return cliresult.ErrorSourceUnavailable, cliresult.StagePreflight
	default:
		stage := cliresult.StagePreflight
		if result.PhysicalMutation || result.DeletedCount > 0 || errors.Is(err, blobgc.ErrPartial) {
			stage = cliresult.StageCanonical
		}
		return cliresult.ErrorOperationFailed, stage
	}
}

func blobGCRerunAction(configPath, dataDir string, residentID canonical.ID) (cliresult.RequiredAction, error) {
	argv := []string{"mahoroba", "blob", "gc", "--resident", residentID.String()}
	if configPath != "" {
		absolute, err := filepath.Abs(configPath)
		if err != nil {
			return cliresult.RequiredAction{}, err
		}
		argv = append(argv, "--config", filepath.Clean(absolute))
	}
	argv = append(argv, "--data-dir", filepath.Clean(dataDir))
	return cliresult.NewRequiredAction(
		cliresult.ActionRerunBlobGCDryRun, argv, []string{residentID.String()}, nil,
	)
}

type adminRecoveryExecution struct {
	terminalized int
	cancelled    int
	head         integrity.HeadMetadata
	commits      []cliresult.CanonicalCommit
}

func runAdminRecoveryTerminalize(ctx context.Context, arguments []string, stdout, stderr io.Writer) int {
	return runAdminRecoveryTerminalizeWithExecutor(
		ctx, arguments, stdout, stderr, executeAdminRecoveryTerminalize,
	)
}

type adminRecoveryExecutor func(
	context.Context, config.Config,
) (adminRecoveryExecution, cliresult.ErrorCode, cliresult.Stage, error)

// runAdminRecoveryTerminalizeWithExecutor keeps argument/configuration parsing
// and the exact CLI renderer on the production path while allowing command
// evidence to drive the real recovery service without requiring a platform
// filesystem ACL fixture.
func runAdminRecoveryTerminalizeWithExecutor(
	ctx context.Context,
	arguments []string,
	stdout, stderr io.Writer,
	execute adminRecoveryExecutor,
) int {
	flags := flag.NewFlagSet("admin recovery terminalize", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "TOML configuration path")
	dataDir := flags.String("data-dir", "", "absolute Mahoroba data directory")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 {
		return renderAdminIntegrityEnvelope(stdout, stderr,
			cliresult.NewFailure(cliresult.CommandAdminRecoveryTerminalize, cliresult.ErrorCLIUsage, cliresult.StageUsage, nil))
	}
	cfg, err := config.Load(config.Overrides{ConfigPath: *configPath, DataDir: *dataDir})
	if err != nil {
		return renderAdminIntegrityEnvelope(stdout, stderr, cliresult.NewFailure(
			cliresult.CommandAdminRecoveryTerminalize, cliresult.ErrorConfigurationInvalid, cliresult.StagePreflight, nil,
		))
	}
	execution, code, stage, operationErr := execute(ctx, cfg)
	// The CLI contract orders evidence by the logical Canonical cursor even if
	// phases return batches in a different order. Validation later rejects any
	// malformed cursor; this pass is solely deterministic ordering/merging.
	mergeCLICommitEvidence(&execution.commits, nil)
	result := &cliresult.RecoveryTerminalizeResult{
		AllExisting:            recoveryAllExisting(execution.commits),
		TerminalizedAttempts:   strconv.Itoa(execution.terminalized),
		CancelledMandatoryWork: strconv.Itoa(execution.cancelled),
		ResultHead:             integrityScanCLIHead(execution.head),
	}
	warnings := []cliresult.Warning{{
		WarningCode: cliresult.WarningLocalAdminNotAttributed,
		TargetIDs:   []string{},
	}}
	if operationErr == nil {
		envelope := cliresult.NewSuccess(cliresult.CommandAdminRecoveryTerminalize, result)
		envelope.CanonicalCommits = execution.commits
		envelope.CanonicalApplied = len(execution.commits) > 0
		envelope.Warnings = warnings
		return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
	}
	if len(execution.commits) > 0 {
		envelope := cliresult.NewPartial(cliresult.CommandAdminRecoveryTerminalize, code, stage, result)
		envelope.CanonicalCommits = execution.commits
		envelope.CanonicalApplied = true
		envelope.Warnings = warnings
		if overflow := newRecoveryOverflowAction(operationErr); overflow != nil {
			envelope.RequiredActions = []cliresult.RequiredAction{*overflow}
			envelope.TargetIDs = append([]string(nil), overflow.TargetIDs...)
		}
		return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
	}
	envelope := cliresult.NewFailure(cliresult.CommandAdminRecoveryTerminalize, code, stage, nil)
	envelope.Warnings = warnings
	if overflow := newRecoveryOverflowAction(operationErr); overflow != nil {
		envelope.RequiredActions = []cliresult.RequiredAction{*overflow}
		envelope.TargetIDs = append([]string(nil), overflow.TargetIDs...)
	}
	return renderAdminIntegrityEnvelope(stdout, stderr, envelope)
}

func executeAdminRecoveryTerminalize(
	ctx context.Context,
	cfg config.Config,
) (adminRecoveryExecution, cliresult.ErrorCode, cliresult.Stage, error) {
	var execution adminRecoveryExecution
	dataLock, err := hostlock.Acquire(cfg.DataDir)
	if err != nil {
		code := cliresult.ErrorOperationFailed
		if errors.Is(err, hostlock.ErrLocked) {
			code = cliresult.ErrorAdminLockBusy
		}
		return execution, code, cliresult.StagePreflight, err
	}
	database, databaseBoundary, err := openOfflineWritableStore(ctx, cfg)
	if err != nil {
		return execution, cliresult.ErrorIntegrityFatal, cliresult.StagePreflight,
			errors.Join(err, dataLock.Close())
	}
	closeStoreAndLock := func() error {
		return errors.Join(database.Close(), databaseBoundary.Close(), dataLock.Close())
	}
	timezone, err := canonical.ParseTimezone(cfg.Timezone)
	if err != nil {
		return execution, cliresult.ErrorConfigurationInvalid, cliresult.StagePreflight,
			errors.Join(err, closeStoreAndLock())
	}
	blobs, err := blob.NewFileStore(cfg.BlobPath())
	if err != nil {
		return execution, cliresult.ErrorSourceUnavailable, cliresult.StagePreflight,
			errors.Join(err, closeStoreAndLock())
	}
	if err := verifyCanonicalResidents(ctx, database.Canonical()); err != nil {
		return execution, cliresult.ErrorIntegrityFatal, cliresult.StagePreflight,
			errors.Join(err, closeStoreAndLock())
	}
	if err := database.MinimumCheckerWithBlobObjects(blobs).Check(ctx); err != nil {
		return execution, cliresult.ErrorIntegrityFatal, cliresult.StagePreflight,
			errors.Join(err, closeStoreAndLock())
	}
	if err := databaseBoundary.Verify(); err != nil {
		return execution, cliresult.ErrorIntegrityFatal, cliresult.StagePreflight,
			errors.Join(fmt.Errorf("recovery database identity changed during preflight: %w", err),
				closeStoreAndLock())
	}
	ids := canonical.NewSecureIDGenerator()
	writer, err := canonical.OpenWriter(ctx, canonical.WriterOptions{
		Backend: database.Canonical(), IDs: ids, Clock: canonical.SystemClock{},
		Timezone: timezone, QueueCapacity: 128,
	})
	if err != nil {
		return execution, cliresult.ErrorOperationFailed, cliresult.StagePreflight,
			errors.Join(err, closeStoreAndLock())
	}
	closeAll := func() error {
		return errors.Join(writer.Close(context.WithoutCancel(ctx)), closeStoreAndLock())
	}
	terminalizer, err := app.NewRecoveryTerminalizer(app.RecoveryTerminalizerOptions{
		Repository: database.Canonical(), MandatoryRepository: database.Canonical(),
		EnvelopeResolver: database.Canonical(), IDs: ids, Submit: writer.Submit,
	})
	if err != nil {
		return execution, cliresult.ErrorOperationFailed, cliresult.StagePreflight,
			errors.Join(err, closeAll())
	}
	integrityService, err := integrityrun.New(integrityrun.Options{
		Writer: writer, Repository: database.Canonical(), Scanner: database.IntegrityScanner(), IDs: ids,
	})
	if err != nil {
		return execution, cliresult.ErrorOperationFailed, cliresult.StagePreflight,
			errors.Join(err, closeAll())
	}
	if err := terminalizer.PreflightMandatory(ctx); err != nil {
		code := cliresult.ErrorOperationFailed
		if errors.Is(err, domain.ErrRecoveryAttemptOverflow) {
			code = cliresult.ErrorRecoveryAttemptOverflow
		}
		return finishAdminRecovery(ctx, database, execution, code, cliresult.StagePreflight, err, closeAll)
	}
	if err := databaseBoundary.Verify(); err != nil {
		return finishAdminRecovery(ctx, database, execution, cliresult.ErrorOperationFailed,
			cliresult.StagePreflight,
			fmt.Errorf("recovery database identity changed after mandatory preflight: %w", err), closeAll)
	}

	if err := databaseBoundary.Verify(); err != nil {
		return finishAdminRecovery(ctx, database, execution, cliresult.ErrorOperationFailed,
			cliresult.StageCanonical,
			fmt.Errorf("recovery database identity changed before running terminalization: %w", err), closeAll)
	}
	running, err := terminalizer.TerminalizeRunning(ctx)
	execution.terminalized += running.TerminalizedAttempts
	mergeRecoveryMetadata(&execution.commits, running.CanonicalCommits,
		cliresult.EffectGenerationAttemptTerminalized)
	if boundaryErr := databaseBoundary.Verify(); boundaryErr != nil {
		err = errors.Join(err,
			fmt.Errorf("recovery database identity changed after running terminalization: %w", boundaryErr))
	}
	if err != nil {
		return finishAdminRecovery(ctx, database, execution, cliresult.ErrorOperationFailed,
			cliresult.StageCanonical, err, closeAll)
	}
	if err := databaseBoundary.Verify(); err != nil {
		return finishAdminRecovery(ctx, database, execution, cliresult.ErrorOperationFailed,
			cliresult.StageCanonical,
			fmt.Errorf("recovery database identity changed before integrity scan: %w", err), closeAll)
	}
	firstScan, err := integrityService.Run(ctx, nil)
	mergeCLICommitEvidence(&execution.commits, integrityScanCLICommits(firstScan.CanonicalCommits))
	if boundaryErr := databaseBoundary.Verify(); boundaryErr != nil {
		err = errors.Join(err,
			fmt.Errorf("recovery database identity changed after integrity scan: %w", boundaryErr))
	}
	if err != nil {
		return finishAdminRecovery(ctx, database, execution, cliresult.ErrorIntegrityApplyConflict,
			cliresult.StageCanonical, err, closeAll)
	}
	if err := databaseBoundary.Verify(); err != nil {
		return finishAdminRecovery(ctx, database, execution, cliresult.ErrorOperationFailed,
			cliresult.StageCanonical,
			fmt.Errorf("recovery database identity changed before mandatory terminalization: %w", err), closeAll)
	}
	mandatory, err := terminalizer.TerminalizeMandatory(ctx)
	execution.cancelled += mandatory.CancelledMandatoryWork
	mergeRecoveryMetadata(&execution.commits, mandatory.CanonicalCommits,
		cliresult.EffectGenerationAttemptTerminalized, cliresult.EffectMandatoryWorkCancelled)
	if boundaryErr := databaseBoundary.Verify(); boundaryErr != nil {
		err = errors.Join(err,
			fmt.Errorf("recovery database identity changed after mandatory terminalization: %w", boundaryErr))
	}
	if err != nil {
		code := cliresult.ErrorOperationFailed
		if errors.Is(err, domain.ErrRecoveryAttemptOverflow) {
			code = cliresult.ErrorRecoveryAttemptOverflow
		}
		return finishAdminRecovery(ctx, database, execution, code, cliresult.StageCanonical, err, closeAll)
	}
	if len(mandatory.UnresolvedCandidates) != 0 {
		if err := databaseBoundary.Verify(); err != nil {
			return finishAdminRecovery(ctx, database, execution, cliresult.ErrorOperationFailed,
				cliresult.StageCanonical,
				fmt.Errorf("recovery database identity changed before follow-up scan: %w", err), closeAll)
		}
		secondScan, scanErr := integrityService.Run(ctx, nil)
		mergeCLICommitEvidence(&execution.commits, integrityScanCLICommits(secondScan.CanonicalCommits))
		if boundaryErr := databaseBoundary.Verify(); boundaryErr != nil {
			scanErr = errors.Join(scanErr,
				fmt.Errorf("recovery database identity changed after follow-up scan: %w", boundaryErr))
		}
		if scanErr != nil {
			return finishAdminRecovery(ctx, database, execution, cliresult.ErrorIntegrityApplyConflict,
				cliresult.StageCanonical, scanErr, closeAll)
		}
	}
	if err := databaseBoundary.Verify(); err != nil {
		return finishAdminRecovery(ctx, database, execution, cliresult.ErrorOperationFailed,
			cliresult.StageCanonical,
			fmt.Errorf("recovery database identity changed before result: %w", err), closeAll)
	}
	return finishAdminRecovery(ctx, database, execution, "", "", nil, closeAll)
}

func finishAdminRecovery(
	ctx context.Context,
	database *store.Store,
	execution adminRecoveryExecution,
	code cliresult.ErrorCode,
	stage cliresult.Stage,
	cause error,
	closeAll func() error,
) (adminRecoveryExecution, cliresult.ErrorCode, cliresult.Stage, error) {
	if head, err := database.Canonical().LoadHead(ctx); err != nil {
		cause = errors.Join(cause, err)
		if code == "" {
			code, stage = cliresult.ErrorOperationFailed, cliresult.StageCanonical
		}
	} else if rich, err := database.Canonical().IntegrityHeadMetadata(ctx, head); err != nil {
		cause = errors.Join(cause, err)
		if code == "" {
			code, stage = cliresult.ErrorOperationFailed, cliresult.StageCanonical
		}
	} else {
		execution.head = rich
	}
	if err := closeAll(); err != nil {
		cause = errors.Join(cause, err)
		if code == "" {
			code, stage = cliresult.ErrorOperationFailed, cliresult.StageCanonical
		}
	}
	return execution, code, stage, cause
}

func mergeRecoveryMetadata(
	target *[]cliresult.CanonicalCommit,
	commits []canonical.CommitMetadata,
	effects ...cliresult.EffectCode,
) {
	for _, metadata := range commits {
		var residentID *string
		if scoped, ok := metadata.Scope.ResidentID(); ok {
			value := scoped.String()
			residentID = &value
		}
		mergeCLICommitEvidence(target, []cliresult.CanonicalCommit{{
			ResidentID: residentID, CommitID: metadata.CommitID.String(),
			CommitSeq: metadata.CommitSeq.String(), Disposition: cliresult.DispositionCreated,
			Effects: append([]cliresult.EffectCode(nil), effects...),
		}})
	}
}

func mergeCLICommitEvidence(target *[]cliresult.CanonicalCommit, additions []cliresult.CanonicalCommit) {
	for _, addition := range additions {
		merged := false
		for index := range *target {
			if (*target)[index].CommitID != addition.CommitID {
				continue
			}
			entry := &(*target)[index]
			if addition.Disposition == cliresult.DispositionCreated {
				entry.Disposition = cliresult.DispositionCreated
			}
			entry.Effects = append(entry.Effects, addition.Effects...)
			slices.Sort(entry.Effects)
			entry.Effects = slices.Compact(entry.Effects)
			merged = true
			break
		}
		if !merged {
			slices.Sort(addition.Effects)
			addition.Effects = slices.Compact(addition.Effects)
			*target = append(*target, addition)
		}
	}
	slices.SortFunc(*target, func(left, right cliresult.CanonicalCommit) int {
		leftSeq, _ := strconv.ParseInt(left.CommitSeq, 10, 64)
		rightSeq, _ := strconv.ParseInt(right.CommitSeq, 10, 64)
		return cmp.Compare(leftSeq, rightSeq)
	})
}

func recoveryAllExisting(commits []cliresult.CanonicalCommit) bool {
	for _, commit := range commits {
		if commit.Disposition != cliresult.DispositionExisting {
			return false
		}
	}
	return true
}

func newRecoveryOverflowAction(err error) *cliresult.RequiredAction {
	var overflow *domain.RecoveryAttemptOverflowError
	if !errors.As(err, &overflow) || overflow == nil {
		return nil
	}
	action, actionErr := cliresult.NewRequiredAction(
		cliresult.ActionRepairStaticDesign, nil, []string{overflow.RunID.String()}, nil,
	)
	if actionErr != nil {
		return nil
	}
	return &action
}

type repeatedStringFlag []string

func (values *repeatedStringFlag) String() string { return strings.Join(*values, ",") }
func (values *repeatedStringFlag) Set(value string) error {
	if value == "" {
		return errors.New("flag value must not be empty")
	}
	*values = append(*values, value)
	return nil
}

func runAutonomyRetentionCandidates(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("admin autonomy retention candidates", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	residentRaw := flags.String("resident", "", "resident ULID")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("admin autonomy retention candidates accepts flags only")
	}
	residentID, err := requiredID(*residentRaw, "resident")
	if err != nil {
		return err
	}
	cfg, err := common.load()
	if err != nil {
		return err
	}
	policy, err := cfg.Autonomy.SchedulerPolicy(cfg.Timezone)
	if err != nil {
		return err
	}
	database, err := store.OpenInspection(ctx, cfg.DatabasePath())
	if err != nil {
		return err
	}
	defer database.Close()
	now := time.Now().UTC()
	capture, err := database.Canonical().RetentionSources(ctx, autonomy.RetentionRequest{ResidentID: residentID, WallNow: now})
	if err != nil {
		return err
	}
	candidates, err := autonomy.Candidates(policy.Retention, now, capture)
	if err != nil {
		return err
	}
	if candidates == nil {
		candidates = []autonomy.RetentionCandidate{}
	}
	return writeIndentedJSON(stdout, map[string]any{
		"captured_head":   canonicalHeadJSON(capture.CapturedHead),
		"resident_id":     residentID.String(),
		"policy_version":  string(policy.Version),
		"mode":            string(policy.Retention.Mode),
		"candidate_count": len(candidates),
		"candidates":      candidates,
	})
}

func runAutonomyStatus(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("admin autonomy status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	residentRaw := flags.String("resident", "", "resident ULID")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("admin autonomy status accepts flags only")
	}
	residentID, err := requiredID(*residentRaw, "resident")
	if err != nil {
		return err
	}
	cfg, err := common.load()
	if err != nil {
		return err
	}
	policy, err := cfg.Autonomy.SchedulerPolicy(cfg.Timezone)
	if err != nil {
		return err
	}
	timezone, err := canonical.ParseTimezone(cfg.Timezone)
	if err != nil {
		return err
	}
	database, err := store.OpenInspection(ctx, cfg.DatabasePath())
	if err != nil {
		return err
	}
	defer database.Close()
	repository := database.Canonical()
	now := time.Now().UTC()
	snapshot, err := repository.AutonomySnapshot(ctx, autonomy.SnapshotRequest{
		ResidentID: residentID, WallNow: now, Timezone: timezone,
		MaxAttempts: cfg.Generation.MaxAttempts,
	})
	if err != nil {
		return err
	}
	timing := autonomy.ResolveEvaluationTime(snapshot, autonomy.TimePoint{Wall: now}, autonomy.RuntimeAnchors{})

	var selfTalkTrigger autonomy.Trigger
	var initiativeTrigger autonomy.Trigger
	executable, err := repository.DiscoverExecutableAutonomousWork(
		ctx, residentID, cfg.Generation.MaxAttempts, 128,
	)
	if err != nil {
		return err
	}
	selfTalkTrigger, initiativeTrigger = executableAutonomyTriggers(executable)
	var idleFallback autonomy.Trigger
	if snapshot.LastUser != nil {
		idleFallback = autonomy.Trigger{
			Kind: autonomy.TriggerIdle, SourceID: snapshot.LastUser.ID,
			Ordinal: snapshot.ConsecutiveSelfTalk + 1,
		}
	}
	selfTalkTrigger, err = resolveAutonomyStatusTrigger(selfTalkTrigger, func() ([]autonomy.Trigger, error) {
		return repository.DiscoverReevaluationTriggers(ctx, residentID, 128)
	}, idleFallback)
	if err != nil {
		return err
	}
	selfTalkDecision := autonomy.DecideSelfTalk(policy, snapshot, selfTalkTrigger, timing)

	_, projectionAvailable, err := repository.AutonomousProjectionEvidence(
		ctx, residentID, canonical.InstantFromTime(now), cfg.Projection.MaxStaleness,
	)
	if err != nil {
		return err
	}
	initiativeTrigger, err = resolveAutonomyStatusTrigger(initiativeTrigger, func() ([]autonomy.Trigger, error) {
		return repository.DiscoverInitiativeTriggers(
			ctx, residentID, canonical.InstantFromTime(now), cfg.Projection.MaxStaleness, 128,
		)
	}, autonomy.Trigger{})
	if err != nil {
		return err
	}
	initiativeSnapshot := snapshot
	initiativeSnapshot.ProjectionAvailable = projectionAvailable
	initiativeDecision := autonomy.DecideInitiative(policy, initiativeSnapshot, initiativeTrigger, timing)

	capture, err := repository.RetentionSources(ctx, autonomy.RetentionRequest{ResidentID: residentID, WallNow: now})
	if err != nil {
		return err
	}
	candidates, err := autonomy.Candidates(policy.Retention, now, capture)
	if err != nil {
		return err
	}
	if candidates == nil {
		candidates = []autonomy.RetentionCandidate{}
	}
	retentionReason := autonomy.BlockingNone
	if policy.Retention.Mode == autonomy.RetentionDisabled {
		retentionReason = autonomy.BlockingDisabled
	}
	ttsReason := autonomy.BlockingNone
	if !cfg.Autonomy.TTS.Enabled {
		ttsReason = autonomy.BlockingDisabled
	}
	initiativeMemoryEnabled := snapshot.MemoryPolicyVersion == string(memory.PolicyVersionV2) ||
		snapshot.MemoryPolicyVersion == string(memory.PolicyVersionV3) ||
		snapshot.MemoryPolicyVersion == string(memory.PolicyVersionV4)

	return writeIndentedJSON(stdout, map[string]any{
		"captured_head":          canonicalHeadJSON(snapshot.CapturedHead),
		"resident_id":            residentID.String(),
		"resident_status":        snapshot.ResidentStatus,
		"active_memory_policy":   snapshot.MemoryPolicyVersion,
		"active_autonomy_policy": string(policy.Version),
		"counters": map[string]any{
			"consecutive_self_talk": snapshot.ConsecutiveSelfTalk,
			"self_talk_hour":        snapshot.SelfTalkHourCount, "self_talk_day": snapshot.SelfTalkDayCount,
			"initiative_hour": snapshot.InitiativeHourCount, "initiative_day": snapshot.InitiativeDayCount,
		},
		"features": map[string]any{
			"self_talk": autonomyFeatureJSON(policy.SelfTalk.Enabled,
				policy.SelfTalk.Enabled && snapshot.ResidentStatus == "active" &&
					(snapshot.MemoryPolicyVersion == autonomy.MemoryPolicyVersionV3 ||
						snapshot.MemoryPolicyVersion == autonomy.MemoryPolicyVersionV4),
				selfTalkDecision, selfTalkTrigger),
			"initiative": autonomyFeatureJSON(policy.Initiative.Enabled,
				policy.Initiative.Enabled && snapshot.ResidentStatus == "active" &&
					initiativeMemoryEnabled && projectionAvailable,
				initiativeDecision, initiativeTrigger),
			"retention": map[string]any{
				"configured":      policy.Retention.Mode == autonomy.RetentionCandidateAfter,
				"effective":       policy.Retention.Mode == autonomy.RetentionCandidateAfter,
				"candidate_count": len(candidates), "blocking_reason": retentionReason,
			},
			"tts": map[string]any{
				"configured": cfg.Autonomy.TTS.Enabled, "effective": cfg.Autonomy.TTS.Enabled,
				"blocking_reason": ttsReason,
			},
		},
	})
}

func executableAutonomyTriggers(works []domain.AutonomousWork) (autonomy.Trigger, autonomy.Trigger) {
	var selfTalk, initiative autonomy.Trigger
	for _, work := range works {
		switch work.Purpose {
		case domain.GenerationPurposeSelfTalk:
			if selfTalk.Validate() != nil && work.Trigger.Validate() == nil {
				selfTalk = work.Trigger
			}
		case domain.GenerationPurposeOutboundInitiative:
			if initiative.Validate() != nil && work.Trigger.Validate() == nil {
				initiative = work.Trigger
			}
		}
	}
	return selfTalk, initiative
}

func resolveAutonomyStatusTrigger(
	durable autonomy.Trigger,
	discover func() ([]autonomy.Trigger, error),
	fallback autonomy.Trigger,
) (autonomy.Trigger, error) {
	if durable.Validate() == nil {
		return durable, nil
	}
	discovered, err := discover()
	if err != nil {
		return autonomy.Trigger{}, err
	}
	for _, trigger := range discovered {
		if trigger.Validate() == nil {
			return trigger, nil
		}
	}
	return fallback, nil
}

func autonomyFeatureJSON(configured, effective bool, decision autonomy.Decision, trigger autonomy.Trigger) map[string]any {
	result := map[string]any{
		"configured": configured, "effective": effective, "eligible": decision.Eligible,
		"blocking_reason": decision.BlockingReason, "next_eligible_at": nil, "pending_trigger": nil,
	}
	if decision.NextEligibleAt != nil {
		result["next_eligible_at"] = decision.NextEligibleAt.UTC().Format(time.RFC3339Nano)
	}
	if trigger.Validate() == nil {
		result["pending_trigger"] = autonomyTriggerJSON(trigger)
	}
	return result
}

func autonomyTriggerJSON(trigger autonomy.Trigger) map[string]any {
	identity, _ := trigger.StableIdentity()
	related := make([]string, len(trigger.RelatedClaimIDs))
	for index, claimID := range trigger.RelatedClaimIDs {
		related[index] = claimID.String()
	}
	result := map[string]any{
		"identity": identity, "kind": trigger.Kind, "source_id": trigger.SourceID.String(),
		"related_claim_ids": related, "ordinal": trigger.Ordinal,
	}
	if trigger.Boundary != 0 {
		result["boundary"] = trigger.Boundary.String()
	}
	return result
}

func canonicalHeadJSON(head canonical.Head) map[string]any {
	result := map[string]any{"exists": head.Exists}
	if head.Exists {
		result["commit_seq"] = head.CommitSeq.String()
		result["committed_at"] = head.CommittedAt.Time().UTC().Format(time.RFC3339Nano)
	}
	return result
}

func runMemoryDerive(ctx context.Context, operation string, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("admin memory "+operation, flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	residentRaw := flags.String("resident", "", "resident ULID")
	var sourceRaw repeatedStringFlag
	flags.Var(&sourceRaw, "source", "source claim ULID (repeat for abstraction)")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("admin memory %s accepts flags only", operation)
	}
	residentID, err := requiredID(*residentRaw, "resident")
	if err != nil {
		return err
	}
	sources := make([]canonical.ID, len(sourceRaw))
	for index, raw := range sourceRaw {
		sources[index], err = canonical.ParseID(raw)
		if err != nil {
			return fmt.Errorf("--source %d: %w", index+1, err)
		}
	}
	purpose := domain.GenerationPurposeMemoryAbstraction
	if operation == "split" {
		purpose = domain.GenerationPurposeMemoryDifferentiation
		if len(sources) != 1 {
			return errors.New("admin memory split requires exactly one --source")
		}
	} else if len(sources) < domain.MinimumAbstractionSources || len(sources) > domain.MaximumAbstractionSources {
		return fmt.Errorf("admin memory abstract requires %d..%d --source flags", domain.MinimumAbstractionSources, domain.MaximumAbstractionSources)
	}
	return withAdminGenerationRuntime(ctx, common, func(runtime *runtimeComponents, _ config.Config) error {
		result, err := runtime.app.DeriveMemoryClaim(ctx, residentID, purpose, sources)
		if err != nil {
			return err
		}
		return writeIndentedJSON(stdout, result)
	})
}

func runMemoryReextract(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("admin memory reextract", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	residentRaw := flags.String("resident", "", "resident ULID")
	eventRaw := flags.String("event", "", "source event ULID")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("admin memory reextract accepts flags only")
	}
	residentID, err := requiredID(*residentRaw, "resident")
	if err != nil {
		return err
	}
	eventID, err := requiredID(*eventRaw, "event")
	if err != nil {
		return err
	}
	requestID, err := canonical.NewSecureIDGenerator().New()
	if err != nil {
		return err
	}
	return withAdminGenerationRuntime(ctx, common, func(runtime *runtimeComponents, _ config.Config) error {
		result, err := runtime.app.ReextractMemoryEvent(ctx, residentID, eventID, requestID)
		if err != nil {
			return err
		}
		return writeIndentedJSON(stdout, result)
	})
}

func runMemoryPersonaPropose(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("admin memory persona propose", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	residentRaw := flags.String("resident", "", "resident ULID")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("admin memory persona propose accepts flags only")
	}
	residentID, err := requiredID(*residentRaw, "resident")
	if err != nil {
		return err
	}
	return withAdminGenerationRuntime(ctx, common, func(runtime *runtimeComponents, _ config.Config) error {
		result, err := runtime.app.ProposeMemoryPersona(ctx, residentID)
		if err != nil {
			return err
		}
		return writeIndentedJSON(stdout, result)
	})
}

func runMemoryPersonaList(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("admin memory persona list", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	residentRaw := flags.String("resident", "", "resident ULID")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("admin memory persona list accepts flags only")
	}
	residentID, err := requiredID(*residentRaw, "resident")
	if err != nil {
		return err
	}
	cfg, err := common.load()
	if err != nil {
		return err
	}
	database, err := store.OpenInspection(ctx, cfg.DatabasePath())
	if err != nil {
		return err
	}
	defer database.Close()
	queries, err := app.NewMemoryAdminQueries(database.Canonical())
	if err != nil {
		return err
	}
	revisions, err := queries.ListPersonaRevisions(ctx, residentID)
	if err != nil {
		return err
	}
	return writeIndentedJSON(stdout, revisions)
}

func runMemoryClaimStatus(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("admin memory claim status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	residentRaw := flags.String("resident", "", "resident ULID")
	claimRaw := flags.String("claim", "", "claim ULID")
	toRaw := flags.String("to", "", "invalidated|superseded|quarantined|active")
	reasonRaw := flags.String("reason", "", "human status decision reason code")
	replacementRaw := flags.String("replacement", "", "sediment direct claim ULID proposed to replace --claim")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("admin memory claim status accepts flags only")
	}
	residentID, err := requiredID(*residentRaw, "resident")
	if err != nil {
		return err
	}
	claimID, err := requiredID(*claimRaw, "claim")
	if err != nil {
		return err
	}
	to := memory.ClaimStatus(*toRaw)
	if err := to.Validate(); err != nil {
		return err
	}
	reason := memory.HumanDecisionReason(*reasonRaw)
	if err := reason.Validate(); err != nil {
		return err
	}
	var replacementID canonical.ID
	if *replacementRaw != "" {
		replacementID, err = canonical.ParseID(*replacementRaw)
		if err != nil {
			return fmt.Errorf("invalid --replacement: %w", err)
		}
		if to != memory.StatusSuperseded || reason != memory.HumanReasonSupersession {
			return errors.New("--replacement requires --to superseded --reason human_supersession")
		}
	}
	return withAdminRuntime(ctx, common, func(runtime *runtimeComponents, _ config.Config) error {
		if !replacementID.IsZero() {
			result, err := runtime.app.CreateMemoryReplacementIntent(ctx, residentID, replacementID, claimID)
			if err != nil {
				return err
			}
			return writeIndentedJSON(stdout, result)
		}
		result, err := runtime.app.SetMemoryClaimStatus(ctx, residentID, claimID, to, reason)
		if err != nil {
			return err
		}
		return writeIndentedJSON(stdout, result)
	})
}

func runMemoryClaimScope(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("admin memory claim scope", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	residentRaw := flags.String("resident", "", "resident ULID")
	claimRaw := flags.String("claim", "", "claim ULID")
	scopeRaw := flags.String("scope", "", "resident_ui|admin_only")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("admin memory claim scope accepts flags only")
	}
	residentID, err := requiredID(*residentRaw, "resident")
	if err != nil {
		return err
	}
	claimID, err := requiredID(*claimRaw, "claim")
	if err != nil {
		return err
	}
	scope := memory.ViewScope(*scopeRaw)
	if err := scope.Validate(); err != nil {
		return err
	}
	return withAdminRuntime(ctx, common, func(runtime *runtimeComponents, _ config.Config) error {
		result, err := runtime.app.SetMemoryClaimScope(ctx, residentID, claimID, scope)
		if err != nil {
			return err
		}
		return writeIndentedJSON(stdout, result)
	})
}

func runMemoryClaimQuery(ctx context.Context, operation string, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("admin memory claim "+operation, flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	residentRaw := flags.String("resident", "", "resident ULID")
	claimRaw := flags.String("claim", "", "claim ULID")
	stageRaw := flags.String("stage", "", "floating|sediment|settled")
	statusRaw := flags.String("status", "", "active|invalidated|superseded|quarantined")
	scopeRaw := flags.String("scope", "", "resident_ui|admin_only")
	limit := flags.Int("limit", 100, "maximum claims (1..1000)")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("admin memory claim %s accepts flags only", operation)
	}
	residentID, err := requiredID(*residentRaw, "resident")
	if err != nil {
		return err
	}
	cfg, err := common.load()
	if err != nil {
		return err
	}
	database, err := store.OpenInspection(ctx, cfg.DatabasePath())
	if err != nil {
		return err
	}
	defer database.Close()
	queries, err := app.NewMemoryAdminQueries(database.Canonical())
	if err != nil {
		return err
	}
	switch operation {
	case "list":
		if *claimRaw != "" {
			return errors.New("--claim is valid only for claim show")
		}
		filter := domain.MemoryClaimFilter{
			ResidentID: residentID, Stage: memory.ClaimStage(*stageRaw),
			Status: memory.ClaimStatus(*statusRaw), Scope: memory.ViewScope(*scopeRaw), Limit: *limit,
		}
		claims, err := queries.ListClaims(ctx, filter)
		if err != nil {
			return err
		}
		return writeIndentedJSON(stdout, claims)
	case "show":
		if *stageRaw != "" || *statusRaw != "" || *scopeRaw != "" || *limit != 100 {
			return errors.New("claim show does not accept list filters")
		}
		claimID, err := requiredID(*claimRaw, "claim")
		if err != nil {
			return err
		}
		claim, err := queries.ShowClaim(ctx, residentID, claimID)
		if err != nil {
			return err
		}
		return writeIndentedJSON(stdout, claim)
	default:
		return fmt.Errorf("unknown memory claim query %q", operation)
	}
}

func runMemoryPolicyActivate(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("admin memory policy activate-v0", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	residentRaw := flags.String("resident", "", "active resident ULID")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	return withAdminRuntime(ctx, common, func(runtime *runtimeComponents, _ config.Config) error {
		residentID, err := requiredID(*residentRaw, "resident")
		if err != nil {
			return err
		}
		result, err := runtime.app.ActivateMemoryPolicyV0(ctx, residentID)
		if err != nil {
			return err
		}
		return writeIndentedJSON(stdout, map[string]any{
			"resident_id": residentID.String(), "changed": result.Changed,
			"revision_id": result.RevisionID.String(), "activation_id": result.ActivationID.String(),
			"previous_policy_version": result.PreviousPolicyVersion,
			"policy_version":          result.PolicyVersion, "rendering_version": result.RenderingVersion,
			"definition_schema": "mahoroba-memory-policy-v0",
		})
	})
}

func runAutonomyMemoryPolicyActivate(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("admin memory policy activate-autonomy-v0", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	residentRaw := flags.String("resident", "", "active resident ULID")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("admin memory policy activate-autonomy-v0 accepts flags only")
	}
	residentID, err := requiredID(*residentRaw, "resident")
	if err != nil {
		return err
	}
	return withAdminRuntime(ctx, common, func(runtime *runtimeComponents, _ config.Config) error {
		result, err := runtime.app.ActivateAutonomyMemoryPolicyV0(ctx, residentID)
		if err != nil {
			return err
		}
		return writeIndentedJSON(stdout, map[string]any{
			"resident_id": residentID.String(), "changed": result.Changed,
			"revision_id": result.RevisionID.String(), "activation_id": result.ActivationID.String(),
			"previous_policy_version": result.PreviousPolicyVersion,
			"policy_version":          result.PolicyVersion, "rendering_version": result.RenderingVersion,
			"definition_schema": "mahoroba-memory-policy-v0",
		})
	})
}

func runMemoryPolicyActivateV4(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("admin memory policy activate-v4", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	residentRaw := flags.String("resident", "", "active resident ULID")
	fromRaw := flags.String("from", "", "expected active memory policy version")
	ackRecall := flags.Bool("ack-enable-recall", false, "acknowledge enabling resident Recall")
	ackSelfTalk := flags.Bool("ack-self-talk-extraction", false, "acknowledge mandatory self-talk extraction")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("admin memory policy activate-v4 accepts flags only")
	}
	residentID, err := requiredID(*residentRaw, "resident")
	if err != nil {
		return err
	}
	if *fromRaw == "" {
		return errors.New("--from is required")
	}
	from := memory.PolicyVersion(*fromRaw)
	if err := from.Validate(); err != nil {
		return fmt.Errorf("invalid --from: %w", err)
	}
	return withAdminRuntime(ctx, common, func(runtime *runtimeComponents, _ config.Config) error {
		result, err := runtime.app.ActivateMemoryPolicyV4(ctx, app.ActivateMemoryPolicyV4Options{
			ResidentID: residentID, ExpectedFrom: from,
			AcknowledgeRecallEnable: *ackRecall, AcknowledgeSelfTalkExtraction: *ackSelfTalk,
		})
		if err != nil {
			return err
		}
		return writeIndentedJSON(stdout, map[string]any{
			"resident_id": residentID.String(), "changed": result.Changed,
			"revision_id": result.RevisionID.String(), "activation_id": result.ActivationID.String(),
			"previous_policy_version": result.PreviousPolicyVersion,
			"policy_version":          result.PolicyVersion, "rendering_version": result.RenderingVersion,
			"definition_schema": "mahoroba-memory-policy-v0",
		})
	})
}

func runBootstrapInit(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("admin bootstrap init", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	owner := flags.String("owner", "Owner", "owner display name")
	name := flags.String("name", "Mahoroba", "resident display name")
	seed := flags.String("seed", "blank-v1", "immutable seed key")
	principles := flags.String("principles", defaultPrinciples, "initial principles text")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	return withAdminRuntime(ctx, common, func(runtime *runtimeComponents, cfg config.Config) error {
		state, err := runtime.app.BootstrapInit(ctx, app.BootstrapInput{OwnerName: *owner, Name: *name, SeedKey: *seed, Principles: *principles})
		if err != nil {
			return err
		}
		return writeIndentedJSON(stdout, bootstrapOutput(state))
	})
}

func runBootstrapApprove(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("admin bootstrap approve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	residentRaw := flags.String("resident", "", "draft resident ULID")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	return withAdminRuntime(ctx, common, func(runtime *runtimeComponents, cfg config.Config) error {
		id, err := requiredID(*residentRaw, "resident")
		if err != nil {
			return err
		}
		if err := runtime.app.ApprovePrinciples(ctx, id); err != nil {
			return err
		}
		fmt.Fprintln(stdout, "principles approved and activated for", id)
		return nil
	})
}

func runBootstrapFinalize(ctx context.Context, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("admin bootstrap finalize", flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	residentRaw := flags.String("resident", "", "draft resident ULID")
	persona := flags.String("persona", defaultPersona, "initial persona text")
	memory := flags.String("memory-policy", defaultMemory, "zero-mandatory memory policy JSON")
	selectResident := flags.Bool("select", true, "select resident operationally after activation")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	return withAdminRuntime(ctx, common, func(runtime *runtimeComponents, cfg config.Config) error {
		id, err := requiredID(*residentRaw, "resident")
		if err != nil {
			return err
		}
		if err := runtime.app.FinalizeBootstrap(ctx, id, *persona, *memory); err != nil {
			return err
		}
		if *selectResident {
			if err := runtime.app.SelectResident(ctx, id); err != nil {
				return err
			}
		}
		fmt.Fprintln(stdout, "resident finalized", id)
		return nil
	})
}

func runResidentAdmin(ctx context.Context, operation string, arguments []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("admin resident "+operation, flag.ContinueOnError)
	flags.SetOutput(stderr)
	common := addCommon(flags, false)
	residentRaw := flags.String("resident", "", "resident ULID")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	return withAdminRuntime(ctx, common, func(runtime *runtimeComponents, cfg config.Config) error {
		switch operation {
		case "list":
			residents, err := runtime.app.ListResidents(ctx)
			if err != nil {
				return err
			}
			return writeIndentedJSON(stdout, residentsOutput(residents))
		case "show", "select", "archive":
			id, err := requiredID(*residentRaw, "resident")
			if err != nil {
				return err
			}
			switch operation {
			case "show":
				resident, err := runtime.app.Resident(ctx, id)
				if err != nil {
					return err
				}
				return writeIndentedJSON(stdout, residentOutput(resident))
			case "select":
				if err := runtime.app.SelectResident(ctx, id); err != nil {
					return err
				}
			case "archive":
				if err := runtime.app.ArchiveResident(ctx, id); err != nil {
					return err
				}
			}
			verb := "selected"
			if operation == "archive" {
				verb = "archived"
			}
			fmt.Fprintf(stdout, "resident %s %s\n", id, verb)
			return nil
		default:
			return errors.New("unknown resident operation")
		}
	})
}

func withAdminRuntime(ctx context.Context, common commonFlags, action func(*runtimeComponents, config.Config) error) error {
	cfg, err := common.load()
	if err != nil {
		return err
	}
	runtime, err := openRuntime(ctx, cfg, nil)
	if err != nil {
		return err
	}
	actionErr := action(runtime, cfg)
	return errors.Join(actionErr, runtime.Close(cfg.Server.ShutdownTimeout))
}

func withAdminGenerationRuntime(ctx context.Context, common commonFlags, action func(*runtimeComponents, config.Config) error) error {
	cfg, err := common.load()
	if err != nil {
		return err
	}
	provider, err := chatcompletions.NewWithCapabilities(
		cfg.Generation.BaseURL, cfg.Generation.APIKey,
		cfg.Generation.RequestTimeout, cfg.Generation.MaxConcurrency, generationHTTPClient(cfg.Generation),
		generation.Capabilities{SupportsJSONSchema: cfg.Generation.SupportsJSONSchema},
	)
	if err != nil {
		return err
	}
	runtime, err := openRuntime(ctx, cfg, provider)
	if err != nil {
		return err
	}
	actionErr := action(runtime, cfg)
	return errors.Join(actionErr, runtime.Close(cfg.Server.ShutdownTimeout))
}

func requiredID(raw, name string) (canonical.ID, error) {
	if raw == "" {
		return canonical.ID{}, fmt.Errorf("--%s is required", name)
	}
	return canonical.ParseID(raw)
}

func writeIndentedJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func bootstrapOutput(state domain.BootstrapState) map[string]any {
	return map[string]any{
		"initialized":         state.Initialized,
		"system_principal_id": optionalID(state.SystemPrincipalID),
		"owner_principal_id":  optionalID(state.OwnerPrincipalID),
		"pipeline_version_id": optionalID(state.PipelineVersionID),
		"session_policy_id":   optionalID(state.SessionPolicyID),
		"residents":           residentsOutput(state.Residents),
	}
}

func residentsOutput(residents []domain.ResidentSnapshot) []map[string]any {
	output := make([]map[string]any, 0, len(residents))
	for _, resident := range residents {
		output = append(output, residentOutput(resident))
	}
	return output
}

func residentOutput(resident domain.ResidentSnapshot) map[string]any {
	return map[string]any{
		"resident_id":               resident.ResidentID.String(),
		"resident_principal_id":     resident.ResidentPrincipalID.String(),
		"owner_principal_id":        resident.OwnerPrincipalID.String(),
		"name":                      resident.Name,
		"seed_key":                  resident.SeedKey,
		"status":                    resident.Status,
		"principles_revision_id":    optionalID(resident.PrinciplesRevisionID),
		"persona_revision_id":       optionalID(resident.PersonaRevisionID),
		"memory_policy_revision_id": optionalID(resident.MemoryPolicyRevisionID),
		"pipeline_version_id":       optionalID(resident.PipelineVersionID),
		"session_policy_id":         optionalID(resident.SessionPolicyID),
	}
}

func optionalID(id canonical.ID) any {
	if id.IsZero() {
		return nil
	}
	return id.String()
}

func printUsage(writer io.Writer) {
	fmt.Fprintln(writer, `Mahoroba commands:
  mahoroba serve [--config FILE] [--data-dir DIR] [--listen HOST:PORT] [--allow-remote] [--container-listen]
  mahoroba admin serve [--config FILE] [--data-dir DIR] [--listen HOST:PORT] [--dialogue-allow-remote] [--container-listen]
  mahoroba admin healthcheck [--url http://127.0.0.1:PORT/healthz]
  mahoroba db verify [--config FILE] [--data-dir DIR]
  mahoroba ledger verify [--resident ULID]
  mahoroba blob recover [--config FILE] [--data-dir DIR]
  mahoroba blob gc --resident ULID [--apply --confirm sha256:DIGEST] [--config FILE] [--data-dir DIR]
  mahoroba backup create --output DIR [--include-projections] [--config FILE] [--data-dir DIR]
  mahoroba backup verify --input DIR
  mahoroba backup restore --input DIR --target-data-dir DIR [--config FILE]
  mahoroba export jsonl --output FILE [--config FILE] [--data-dir DIR]
  mahoroba healthcheck [--config FILE] [--url http://127.0.0.1:PORT/healthz]
  mahoroba projection status [--resident ULID] [--name NAME]
  mahoroba projection rebuild --resident ULID (--name NAME | --all)
  mahoroba admin integrity scan (--all | --resident ULID) [--config FILE] [--data-dir DIR]
  mahoroba admin recovery terminalize [--config FILE] [--data-dir DIR]
  mahoroba admin runtime session-policy select --version ULID [--config FILE] [--data-dir DIR]
  mahoroba admin erasure plan content --resident ULID --content ULID [--content ULID...] --reason CODE --output FILE [--config FILE] [--data-dir DIR]
  mahoroba admin erasure plan resident --resident ULID --reason CODE --output FILE [--config FILE] [--data-dir DIR]
  mahoroba admin erasure decide --plan FILE --decision IMPACT_ID=retain|erase [--decision ...] --output FILE [--config FILE] [--data-dir DIR]
  mahoroba admin erasure apply --plan FILE --confirm sha256:DIGEST [--config FILE] [--data-dir DIR]
  mahoroba admin diagnostics (--all | --resident ULID) [--format text|json] [--config FILE] [--data-dir DIR]
  mahoroba admin bootstrap init|approve|finalize [options]
  mahoroba admin resident list|show|select|archive [options]
  mahoroba admin memory policy activate-v0 --resident ULID
  mahoroba admin memory policy activate-autonomy-v0 --resident ULID
  mahoroba admin memory policy activate-v4 --resident ULID --from VERSION [--ack-enable-recall] [--ack-self-talk-extraction]
  mahoroba admin memory claim list --resident ULID [--stage STAGE] [--status STATUS] [--scope SCOPE]
  mahoroba admin memory claim show --resident ULID --claim ULID
  mahoroba admin memory claim scope --resident ULID --claim ULID --scope resident_ui|admin_only
  mahoroba admin memory claim status --resident ULID --claim ULID --to STATUS --reason CODE [--replacement CLAIM]
  mahoroba admin memory reextract --resident ULID --event ULID
  mahoroba admin memory abstract --resident ULID --source CLAIM --source CLAIM [--source CLAIM...]
  mahoroba admin memory split --resident ULID --source CLAIM
  mahoroba admin memory persona propose --resident ULID
  mahoroba admin memory persona list --resident ULID
  mahoroba admin autonomy status --resident ULID
  mahoroba admin autonomy retention candidates --resident ULID

Provider secrets are accepted through exactly one of MAHOROBA_PROVIDER_API_KEY or MAHOROBA_PROVIDER_API_KEY_FILE.
OpenAI Speech accepts its independent secret through exactly one of MAHOROBA_TTS_API_KEY or MAHOROBA_TTS_API_KEY_FILE.`)
}
