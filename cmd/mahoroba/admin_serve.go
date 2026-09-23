package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"mahoroba.local/mahoroba/internal/config"
	"mahoroba.local/mahoroba/internal/httpui"
)

// Administration holds no data-directory lock while dialogue is stopped. Its
// owned dialogue runtime and finite CLI operations retain their existing locks.
func runAdminServe(ctx context.Context, arguments []string, stdout, stderr io.Writer) (returnErr error) {
	flags := flag.NewFlagSet("admin serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "TOML configuration path")
	dataDir := flags.String("data-dir", "", "absolute Mahoroba data directory")
	listen := flags.String("listen", "127.0.0.1:8788", "administration listen address (loopback unless --container-listen)")
	dialogueAllowRemote := flags.Bool("dialogue-allow-remote", false, "allow remote access only to managed dialogue; administration retains local Host checks")
	containerListen := flags.Bool("container-listen", false, "allow container binding for both servers; publish ports on host loopback and retain local Host checks")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("admin serve accepts flags only")
	}
	if *configPath != "" {
		absolute, err := filepath.Abs(*configPath)
		if err != nil {
			return err
		}
		*configPath = absolute
	}
	cfg, err := config.Load(config.Overrides{ConfigPath: *configPath, DataDir: *dataDir})
	if err != nil {
		return err
	}
	adminCtx, cancelAdmin := context.WithCancel(ctx)
	defer cancelAdmin()
	service := newAdminCLIService(*configPath, cfg.DataDir)
	managementURL := ""
	serveArguments := adminDialogueServeArguments(*configPath, cfg.DataDir, *dialogueAllowRemote, *containerListen)
	service.runtime = newAdminRuntimeController(adminCtx, localUIConnectURL(cfg.Server.Listen, *containerListen)+"/", service.admission,
		func(runtimeCtx context.Context, ready func(string)) error {
			return runServeWithLifecycle(runtimeCtx, serveArguments, io.Discard, stderr, serveLifecycleHooks{Ready: ready, ManagementURL: managementURL})
		})
	defer func() {
		cancelAdmin()
		// serve drains HTTP, TTS, and runtime workers in separate bounded phases.
		shutdown, cancel := context.WithTimeout(context.Background(), 3*cfg.Server.ShutdownTimeout+time.Second)
		defer cancel()
		returnErr = errors.Join(returnErr, service.runtime.Close(shutdown))
	}()
	server, err := httpui.NewAdmin(service, httpui.Options{
		Address: *listen, BaseContext: adminCtx,
		ContainerListen:   *containerListen,
		ManagementDataDir: cfg.DataDir,
		ShutdownTimeout:   cfg.Server.ShutdownTimeout,
		Logger:            slog.New(slog.NewTextHandler(stderr, nil)),
		DialogueURL:       localUIConnectURL(cfg.Server.Listen, *containerListen) + "/",
	})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", server.Address())
	if err != nil {
		return err
	}
	managementURL = localUIConnectURL(listener.Addr().String(), *containerListen) + "/"
	fmt.Fprintf(stdout, "Mahoroba administration: %s\n", managementURL)
	result := make(chan error, 1)
	go func() { result <- server.Serve(listener) }()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		cancelAdmin()
		shutdown, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
		defer cancel()
		return errors.Join(server.Shutdown(shutdown), <-result)
	}
}

func adminDialogueServeArguments(configPath, dataDir string, allowRemote, containerListen bool) []string {
	arguments := []string{"--data-dir=" + dataDir}
	if configPath != "" {
		arguments = append(arguments, "--config="+configPath)
	}
	if allowRemote {
		arguments = append(arguments, "--allow-remote")
	}
	if containerListen {
		arguments = append(arguments, "--container-listen")
	}
	return arguments
}

type adminCLICommand struct {
	httpui.AdminCommand
	argv []string
	// Not all command families accept these source flags.
	configFlag  bool
	dataDirFlag bool
}

type adminCLIService struct {
	configPath string
	dataDir    string
	commands   []adminCLICommand
	admission  chan struct{}
	runtime    *adminRuntimeController
}

func newAdminCLIService(configPath, dataDir string) *adminCLIService {
	return &adminCLIService{
		configPath: configPath, dataDir: dataDir,
		commands: adminCLICommands(), admission: make(chan struct{}, 1),
	}
}

func (service *adminCLIService) Commands() []httpui.AdminCommand {
	result := make([]httpui.AdminCommand, len(service.commands))
	for index, command := range service.commands {
		result[index] = command.AdminCommand
		result[index].Fields = append([]httpui.AdminField(nil), command.Fields...)
		for field := range result[index].Fields {
			result[index].Fields[field].Choices = append([]string(nil), command.Fields[field].Choices...)
		}
	}
	return result
}

func (service *adminCLIService) Execute(ctx context.Context, commandID string, values map[string][]string) (httpui.AdminResult, error) {
	argv, err := service.arguments(commandID, values)
	if err != nil {
		return httpui.AdminResult{}, err
	}
	select {
	case service.admission <- struct{}{}:
		defer func() { <-service.admission }()
	case <-ctx.Done():
		return httpui.AdminResult{}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return httpui.AdminResult{}, err
	}
	if service.runtime != nil {
		offline := false
		for _, command := range service.commands {
			if command.ID == commandID {
				offline = command.Offline
				break
			}
		}
		if err := service.runtime.AllowCommand(offline); err != nil {
			return httpui.AdminResult{ExitCode: 1, Stderr: err.Error() + "\n"}, nil
		}
	}
	stdout, stderr := &adminCommandOutput{}, &adminCommandOutput{}
	// No shell, process, or user-supplied command name is involved. run retains
	// CLI validation, host locks, dry-runs, exact digests and result envelopes.
	exitCode := run(ctx, argv, stdout, stderr)
	return httpui.AdminResult{ExitCode: exitCode, Stdout: stdout.String(), Stderr: stderr.String()}, nil
}

func (service *adminCLIService) arguments(commandID string, values map[string][]string) ([]string, error) {
	var selected *adminCLICommand
	for index := range service.commands {
		if service.commands[index].ID == commandID {
			selected = &service.commands[index]
			break
		}
	}
	if selected == nil {
		return nil, errors.New("unknown administration command")
	}
	known := make(map[string]httpui.AdminField, len(selected.Fields))
	for _, field := range selected.Fields {
		known[field.Name] = field
	}
	for name := range values {
		if _, ok := known[name]; !ok {
			return nil, fmt.Errorf("unknown field %q", name)
		}
	}
	argv := append([]string(nil), selected.argv...)
	for _, field := range selected.Fields {
		entries, supplied := values[field.Name]
		if !supplied && field.Default != "" {
			entries = []string{field.Default}
		}
		if len(entries) > 1 && !field.Repeatable {
			return nil, fmt.Errorf("%s accepts exactly one value", field.Label)
		}
		if len(entries) > 1000 {
			return nil, fmt.Errorf("%s has too many values", field.Label)
		}
		added := 0
		for _, value := range entries {
			if !utf8.ValidString(value) || strings.ContainsRune(value, '\x00') || len(value) > 1024*1024 {
				return nil, fmt.Errorf("%s contains invalid or oversized text", field.Label)
			}
			if value == "" {
				continue
			}
			switch field.Type {
			case "checkbox":
				if value != "true" && value != "false" {
					return nil, fmt.Errorf("%s must be true or false", field.Label)
				}
			case "number":
				if _, err := strconv.Atoi(value); err != nil {
					return nil, fmt.Errorf("%s must be an integer", field.Label)
				}
			case "select":
				found := false
				for _, choice := range field.Choices {
					found = found || choice == value
				}
				if !found {
					return nil, fmt.Errorf("%s has an invalid choice", field.Label)
				}
			}
			// An equals sign keeps leading dashes, spaces, and shell metacharacters
			// inside a single flag value. They can never introduce another flag.
			argv = append(argv, "--"+field.Name+"="+value)
			added++
		}
		if field.Required && added == 0 {
			return nil, fmt.Errorf("%s is required", field.Label)
		}
	}
	if selected.configFlag && service.configPath != "" {
		argv = append(argv, "--config="+service.configPath)
	}
	if selected.dataDirFlag {
		argv = append(argv, "--data-dir="+service.dataDir)
	}
	return argv, nil
}

// Large claim/diagnostic results must not consume unbounded HTTP response memory.
// Keep the CLI writer contract and label truncation instead of reporting a
// committed operation as failed because its display was too large.
type adminCommandOutput struct {
	buffer    bytes.Buffer
	truncated bool
}

func (output *adminCommandOutput) Write(value []byte) (int, error) {
	const maximum = 8 * 1024 * 1024
	count := len(value)
	remaining := maximum - output.buffer.Len()
	if len(value) > remaining {
		value = value[:remaining]
		output.truncated = true
	}
	_, _ = output.buffer.Write(value)
	return count, nil
}

func (output *adminCommandOutput) String() string {
	if output.truncated {
		return output.buffer.String() + "\n[Output truncated at 8 MiB.]\n"
	}
	return output.buffer.String()
}

func adminCLICommands() []adminCLICommand {
	text := func(name, label string, required bool) httpui.AdminField {
		return httpui.AdminField{Name: name, Label: label, Type: "text", Required: required}
	}
	area := func(name, label, value string) httpui.AdminField {
		return httpui.AdminField{Name: name, Label: label, Type: "textarea", Default: value, Required: true}
	}
	choice := func(name, label string, required bool, options ...string) httpui.AdminField {
		return httpui.AdminField{Name: name, Label: label, Type: "select", Required: required, Choices: options}
	}
	check := func(name, label string, enabled bool) httpui.AdminField {
		return httpui.AdminField{Name: name, Label: label, Type: "checkbox", Default: strconv.FormatBool(enabled)}
	}
	repeat := func(name, label string) httpui.AdminField {
		return httpui.AdminField{Name: name, Label: label, Type: "textarea", Required: true, Repeatable: true, Description: "Enter one value per line."}
	}
	resident := text("resident", "Resident ID (ULID)", true)
	claim := text("claim", "Claim ID (ULID)", true)
	optionalResident := text("resident", "Resident ID (blank for all)", false)
	timezone := text("timezone", "Timezone (optional)", false)
	scope := choice("scope", "Visibility", true, "resident_ui", "admin_only")
	statuses := []string{"active", "invalidated", "superseded", "quarantined"}
	reason := choice("reason", "Decision reason", true, "human_invalidation", "human_supersession", "human_quarantine", "human_reactivation")
	memoryVersions := []string{"memory-policy-v1", "memory-policy-v2", "memory-policy-v3", "memory-policy-v4"}
	mutation := "I have reviewed the inputs and confirm this operation."
	offline := "Stop the dialogue server and wait for Stopped before running this operation. Separately started runtimes retain their existing exclusive lock."

	var commands []adminCLICommand
	add := func(id, group, label, description string, mutates, exclusive bool, fields ...httpui.AdminField) {
		confirmation := ""
		if mutates {
			confirmation = mutation
		}
		if exclusive {
			description += " " + offline
		}
		commands = append(commands, adminCLICommand{
			AdminCommand: httpui.AdminCommand{ID: id, Group: group, Label: label, Description: description,
				Fields: fields, Mutates: mutates, Offline: exclusive, Confirmation: confirmation},
			argv:       strings.Split(strings.ReplaceAll(id, ".", " "), " "),
			configFlag: true, dataDirFlag: true,
		})
	}
	add("admin.bootstrap.init", "Resident", "Bootstrap init", "Create a draft resident. Review the principles before creation; approval and finalization are separate operations.", true, true,
		httpui.AdminField{Name: "owner", Label: "Owner name", Type: "text", Required: true, Default: "Owner"},
		httpui.AdminField{Name: "name", Label: "Resident name", Type: "text", Required: true, Default: "Mahoroba"},
		httpui.AdminField{Name: "seed", Label: "Seed", Type: "text", Required: true, Default: "blank-v1"},
		area("principles", "Principles", defaultPrinciples), timezone)
	add("admin.bootstrap.approve", "Resident", "Bootstrap approve", "Approve the principles supplied when the draft was created.", true, true, resident)
	commands[len(commands)-1].Confirmation = "I have reviewed and approve this resident's principles."
	add("admin.bootstrap.finalize", "Resident", "Bootstrap finalize", "Activate an approved draft with its initial persona and memory policy.", true, true,
		resident, area("persona", "Persona", defaultPersona), area("memory-policy", "Initial memory policy (JSON)", defaultMemory),
		check("select", "Select this resident after activation", true))
	add("admin.resident.list", "Resident", "List residents", "List all resident states and identifiers.", false, true)
	add("admin.resident.show", "Resident", "Show resident", "Show the resident's state and revision identifiers.", false, true, resident)
	add("admin.resident.select", "Resident", "Select resident", "Select an active resident for dialogue.", true, true, resident)
	add("admin.resident.archive", "Resident", "Archive resident", "Archive the resident and terminate its pending work.", true, true, resident)
	commands[len(commands)-1].Confirmation = "I confirm that this resident should be archived."

	add("admin.memory.policy.activate-v0", "Memory", "Activate memory policy v0", "Run the existing activate-v0 memory policy operation.", true, true, resident)
	add("admin.memory.policy.activate-autonomy-v0", "Memory", "Activate autonomy memory policy", "Activate the autonomy memory policy. Feature settings still come from TOML.", true, true, resident)
	add("admin.memory.policy.activate-v4", "Memory", "Activate memory policy v4", "Migrate from the specified current policy to v4, preserving the CLI acknowledgement requirements.", true, true,
		resident, choice("from", "Current memory policy", true, memoryVersions...),
		check("ack-enable-recall", "Acknowledge enabling Recall", false),
		check("ack-self-talk-extraction", "Acknowledge mandatory self-talk extraction", false))
	add("admin.memory.claim.list", "Memory", "List memory claims", "Filter claims by stage, status, and visibility.", false, false,
		resident, choice("stage", "Stage (optional)", false, "floating", "sediment", "settled"),
		choice("status", "Status (optional)", false, statuses...), choice("scope", "Visibility (optional)", false, "resident_ui", "admin_only"),
		httpui.AdminField{Name: "limit", Label: "Maximum claims (1–1000)", Type: "number", Default: "100"})
	add("admin.memory.claim.show", "Memory", "Show memory claim", "Show the claim and its provenance.", false, false, resident, claim)
	add("admin.memory.claim.scope", "Memory", "Change claim visibility", "Change a claim's visibility.", true, true, resident, claim, scope)
	add("admin.memory.claim.status", "Memory", "Change claim status", "A replacement ID is accepted only with superseded / human_supersession.", true, true,
		resident, claim, choice("to", "New status", true, statuses...), reason, text("replacement", "Replacement claim ID (optional)", false))
	add("admin.memory.reextract", "Memory", "Re-extract memory", "Re-extract memory with the configured generation provider.", true, true, resident, text("event", "Event ID (ULID)", true))
	add("admin.memory.abstract", "Memory", "Abstract memory", "Generate an abstraction from multiple claims.", true, true, resident, repeat("source", "Source claim ID"))
	add("admin.memory.split", "Memory", "Split memory", "Generate differentiation from one claim.", true, true, resident, text("source", "Source claim ID", true))
	add("admin.memory.persona.propose", "Memory", "Propose persona", "Propose a persona using the configured generation provider.", true, true, resident)
	add("admin.memory.persona.list", "Memory", "List persona revisions", "Show persona revision history.", false, false, resident)
	add("admin.autonomy.status", "Autonomy", "Autonomy status", "Show configuration, effective state, counters, and blocking reasons.", false, false, resident, timezone)
	add("admin.autonomy.retention.candidates", "Autonomy", "Retention candidates", "List retention candidates without deleting content.", false, false, resident, timezone)

	add("db.verify", "Inspection and maintenance", "Verify database", "Inspect the database schema without writing.", false, false)
	add("ledger.verify", "Inspection and maintenance", "Verify ledger", "Verify the Canonical ledger without writing.", false, false, optionalResident)
	add("projection.status", "Inspection and maintenance", "Projection status", "Inspect projection status without writing.", false, false,
		optionalResident, text("name", "Projection name (blank for all)", false))
	add("projection.rebuild", "Inspection and maintenance", "Rebuild projections", "Specify either one projection name or all projections.", true, true,
		resident, text("name", "Projection name (blank when rebuilding all)", false), check("all", "Rebuild all", false))
	add("admin.integrity.scan", "Inspection and maintenance", "Scan integrity", "Choose one resident or all residents. Findings are recorded in Canonical state.", true, true,
		text("resident", "Resident ID (blank when selecting all)", false), check("all", "Scan all", false))
	add("admin.recovery.terminalize", "Inspection and maintenance", "Terminalize recovery", "Recover interrupted generation and mandatory work.", true, true)
	add("admin.runtime.session-policy.select", "Inspection and maintenance", "Select session policy", "Select an existing session policy by its version ID.", true, true, text("version", "Session policy version ID", true))
	add("admin.diagnostics", "Inspection and maintenance", "Diagnostics", "Choose one resident or all residents for offline diagnostics.", false, true,
		text("resident", "Resident ID (blank when selecting all)", false), check("all", "Inspect all", false),
		httpui.AdminField{Name: "format", Label: "Format", Type: "select", Default: "json", Choices: []string{"json", "text"}})
	add("healthcheck", "Inspection and maintenance", "Check dialogue server health", "Check the dialogue server's loopback /healthz endpoint.", false, false, text("url", "Health URL (blank for configured address)", false))
	commands[len(commands)-1].dataDirFlag = false

	add("backup.create", "Backup and export", "Create backup", "Save a backup to a new directory. Existing paths are never overwritten.", true, true,
		text("output", "Output absolute path", true), check("include-projections", "Include rebuildable projections", false))
	add("backup.verify", "Backup and export", "Verify backup", "Verify an existing backup bundle.", false, false, text("input", "Backup absolute path", true))
	commands[len(commands)-1].configFlag = false
	commands[len(commands)-1].dataDirFlag = false
	add("backup.restore", "Backup and export", "Restore backup", "Restore to a new data directory. Current data is not overwritten.", true, false,
		text("input", "Backup absolute path", true), text("target-data-dir", "Target data directory absolute path", true))
	commands[len(commands)-1].dataDirFlag = false
	add("export.jsonl", "Backup and export", "Export JSONL", "Export Canonical data to a new JSONL file.", true, true, text("output", "Output absolute path", true))
	add("blob.recover", "Blobs and erasure", "Recover blobs", "Reconcile incomplete blobs against Canonical references and clean up unreferenced objects.", true, true)
	add("blob.gc", "Blobs and erasure", "Blob garbage collection", "Leave Apply off to inspect a fresh plan first. Deletion requires its exact digest.", true, true,
		resident, check("apply", "Apply deletion", false), text("confirm", "Confirmation sha256 digest (required for Apply)", false))
	add("admin.erasure.plan.content", "Blobs and erasure", "Plan content erasure", "Save an erasure plan for the specified content to a new file.", true, true,
		resident, repeat("content", "Content ID"), text("reason", "Erasure reason code (for example, owner_request)", true), text("output", "Plan output absolute path", true))
	add("admin.erasure.plan.resident", "Blobs and erasure", "Plan resident erasure", "Save an erasure plan for the entire resident to a new file.", true, true,
		resident, text("reason", "Erasure reason code (for example, owner_request)", true), text("output", "Plan output absolute path", true))
	add("admin.erasure.decide", "Blobs and erasure", "Review erasure plan", "Decide retain or erase for each impact and save a new reviewed plan.", true, true,
		text("plan", "Input plan absolute path", true), repeat("decision", "Decisions (IMPACT_ID=retain or IMPACT_ID=erase)"), text("output", "Reviewed plan output absolute path", true))
	add("admin.erasure.apply", "Blobs and erasure", "Apply erasure plan", "Validate a reviewed plan and its exact digest before applying erasure.", true, true,
		text("plan", "Reviewed plan absolute path", true), text("confirm", "Plan sha256 digest", true))
	commands[len(commands)-1].Confirmation = "I confirm the erasure described by this plan. Erased content cannot be recovered."
	return commands
}
