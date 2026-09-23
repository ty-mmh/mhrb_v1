(() => {
  "use strict";

  const selector = document.getElementById("admin-command");
  const form = document.getElementById("admin-form");
  const fields = document.getElementById("operation-fields");
  const inputs = document.getElementById("operation-inputs");
  const confirmation = document.getElementById("operation-confirmed");
  const notice = document.getElementById("admin-notice");
  const state = document.getElementById("operation-state");
  const submit = document.getElementById("operation-submit");
  const resultSection = document.getElementById("admin-result");
  const runtimeStart = document.getElementById("runtime-start");
  const runtimeStop = document.getElementById("runtime-stop");
  const runtimeRefresh = document.getElementById("runtime-refresh");
  const runtimeConfirmed = document.getElementById("runtime-confirmed");
  const runtimeError = document.getElementById("runtime-error");
  const dialogueLink = document.getElementById("dialogue-link");
  let commands = [];
  let active = null;
  let lastResult = null;
  let running = false;
  let runtimeStatus = null;
  let runtimePending = false;
  let runtimeLoading = false;
  let runtimeRevision = 0;

  const showNotice = (message) => {
    notice.textContent = message;
    notice.classList.toggle("hidden", !message);
  };

  const runtimeTransitioning = () => Boolean(runtimeStatus && ["starting", "stopping"].includes(runtimeStatus.state));
  const offlineBlocked = () => Boolean(active && active.offline && runtimeStatus &&
    (runtimeStatus.restart_required || runtimeStatus.managed && ["starting", "running", "stopping"].includes(runtimeStatus.state)));

  const syncControls = () => {
    const busy = running || runtimePending;
    const canStart = Boolean(runtimeStatus && !runtimeStatus.restart_required && ["stopped", "failed"].includes(runtimeStatus.state));
    const canStop = Boolean(runtimeStatus && !runtimeStatus.restart_required && runtimeStatus.managed && ["starting", "running"].includes(runtimeStatus.state));
    runtimeStart.disabled = busy || !canStart || !runtimeConfirmed.checked;
    runtimeStop.disabled = busy || !canStop || !runtimeConfirmed.checked;
    runtimeConfirmed.disabled = busy || !canStart && !canStop;
    runtimeRefresh.disabled = runtimePending;
    document.getElementById("runtime-confirmation").textContent = canStop
      ? "I want to stop this managed dialogue server and drain its active work."
      : "I want to start the dialogue server with the current configuration and enabled background activity.";
    selector.disabled = commands.length === 0 || busy || runtimeTransitioning();
    inputs.disabled = busy || runtimeTransitioning();
    confirmation.disabled = busy || runtimeTransitioning() || offlineBlocked();
    submit.disabled = !active || busy || runtimeTransitioning() || offlineBlocked();
    if (active && active.offline) {
      document.getElementById("operation-offline").textContent = offlineBlocked()
        ? runtimeStatus.restart_required
          ? "Restart the management process before this operation. Shutdown did not release the runtime safely."
          : "Stop the managed dialogue server above and wait for shutdown to complete before running this operation."
        : "Stop dialogue first. This operation needs exclusive access to the data directory. A server started separately must be stopped in its own terminal.";
    }
  };

  const showRuntimeError = (message) => {
    runtimeError.textContent = message;
    runtimeError.classList.toggle("hidden", !message);
  };

  const invalidateRuntime = () => {
    runtimeStatus = null;
    runtimeConfirmed.checked = false;
    document.getElementById("runtime-state").textContent = "Dialogue server status is unavailable.";
    dialogueLink.removeAttribute("href");
    dialogueLink.setAttribute("aria-disabled", "true");
    dialogueLink.setAttribute("tabindex", "-1");
    syncControls();
  };

  const renderRuntime = (status) => {
    if (!runtimeStatus || runtimeStatus.state !== status.state || runtimeStatus.managed !== status.managed ||
        runtimeStatus.restart_required !== status.restart_required) {
      runtimeConfirmed.checked = false;
      confirmation.checked = false;
    }
    runtimeStatus = status;
    const labels = {
      stopped: "No dialogue server is managed here.",
      starting: "Starting dialogue server…",
      running: "Dialogue server is running.",
      stopping: "Stopping dialogue server… Waiting for active work to drain.",
      failed: "Dialogue server failed."
    };
    document.getElementById("runtime-state").textContent = status.restart_required
      ? "Restart the management process before starting another dialogue server."
      : labels[status.state] || "Dialogue server state is unavailable.";
    document.getElementById("runtime-message").textContent = status.message ||
      (status.managed ? "This dialogue server is owned by the management process. Closing this tab does not stop it."
        : "A server started separately is outside these controls and must be stopped in its own terminal.");
    showRuntimeError(status.error || "");
    const available = status.state === "running" && status.ready && !status.restart_required;
    dialogueLink.setAttribute("aria-disabled", String(!available));
    if (available) {
      dialogueLink.href = status.url || dialogueLink.dataset.dialogueUrl;
      dialogueLink.removeAttribute("tabindex");
    } else {
      dialogueLink.removeAttribute("href");
      dialogueLink.setAttribute("tabindex", "-1");
    }
    syncControls();
  };

  const refreshRuntime = async () => {
    if (runtimePending || runtimeLoading) return;
    runtimeLoading = true;
    const revision = ++runtimeRevision;
    try {
      const response = await fetch("/admin/runtime", { headers: { "Accept": "application/json" }, cache: "no-store" });
      const status = await response.json();
      if (!response.ok || !status.state) throw new Error(status.error || "Could not read dialogue server status.");
      if (revision === runtimeRevision) renderRuntime(status);
    } catch (error) {
      if (revision !== runtimeRevision) return;
      invalidateRuntime();
      showRuntimeError(error.message || "Could not read dialogue server status.");
    } finally {
      runtimeLoading = false;
    }
  };

  const changeRuntime = async (action) => {
    const button = action === "start" ? runtimeStart : runtimeStop;
    if (button.disabled || runtimePending || running || !runtimeConfirmed.checked) return;
    runtimePending = true;
    ++runtimeRevision; // Ignore a status response captured before this action.
    syncControls();
    showRuntimeError("");
    let receivedStatus = false;
    try {
      const response = await fetch(`/admin/runtime/${action}`, {
        method: "POST",
        headers: { "Accept": "application/json", "Content-Type": "application/json" },
        body: JSON.stringify({ confirmed: true })
      });
      const status = await response.json();
      if (status.state) {
        receivedStatus = true;
        renderRuntime(status);
      }
      if (!response.ok) throw new Error(status.error || `Server action failed (${response.status}).`);
      if (!status.state) throw new Error("The dialogue server returned an invalid state.");
    } catch (error) {
      if (!receivedStatus) invalidateRuntime();
      showRuntimeError(`${error.message || "The server action result is unavailable."} Refresh the status before trying again.`);
    } finally {
      runtimePending = false;
      runtimeConfirmed.checked = false;
      syncControls();
    }
  };

  runtimeConfirmed.checked = false;
  runtimeConfirmed.addEventListener("change", syncControls);
  runtimeStart.addEventListener("click", () => changeRuntime("start"));
  runtimeStop.addEventListener("click", () => changeRuntime("stop"));
  runtimeRefresh.addEventListener("click", refreshRuntime);
  const pollRuntime = async () => {
    await refreshRuntime();
    setTimeout(pollRuntime, 2000);
  };

  const renderCommand = () => {
    active = commands.find((command) => command.id === selector.value);
    if (!active) return;
    document.getElementById("operation-title").textContent = active.label;
    document.getElementById("operation-command").textContent = `mahoroba ${active.id.replaceAll(".", " ")}`;
    document.getElementById("operation-description").textContent = active.description;
    document.getElementById("operation-access").textContent = active.mutates ? "Changes state or writes files" : "Read only";
    document.getElementById("operation-offline").classList.toggle("hidden", !active.offline);
    document.getElementById("operation-review").classList.toggle("hidden", !active.confirmation);
    document.getElementById("operation-confirmation").textContent = active.confirmation || "";
    confirmation.checked = false;
    confirmation.required = Boolean(active.confirmation);
    fields.replaceChildren();
    for (const field of active.fields || []) {
      const wrapper = document.createElement("div");
      wrapper.className = "operation-field";
      const label = document.createElement("label");
      label.className = "field-label";
      label.htmlFor = `field-${field.name}`;
      label.textContent = field.label + (field.required ? " (required)" : "");
      let input;
      if (field.repeatable || field.type === "textarea") {
        input = document.createElement("textarea");
        input.rows = field.repeatable ? 3 : 5;
      } else if (field.type === "select") {
        input = document.createElement("select");
        if (!field.required && !field.default) input.appendChild(new Option("Use command default", ""));
        for (const choice of field.choices || []) input.appendChild(new Option(choice, choice));
      } else {
        input = document.createElement("input");
        input.type = ["number", "checkbox"].includes(field.type) ? field.type : "text";
        if (field.type === "number") input.step = "1";
      }
      input.id = label.htmlFor;
      input.name = field.name;
      input.required = Boolean(field.required);
      input.autocomplete = "off";
      if (field.type === "checkbox") {
        input.checked = field.default === "true";
        wrapper.classList.add("checkbox-field");
      } else {
        input.value = field.default || "";
      }
      wrapper.append(label, input);
      const description = [field.description, field.repeatable ? "Enter one value per line." : ""].filter(Boolean).join(" ");
      if (description) {
        const help = document.createElement("p");
        help.className = "field-help";
        help.id = `help-${field.name}`;
        help.textContent = description;
        input.setAttribute("aria-describedby", help.id);
        wrapper.appendChild(help);
      }
      fields.appendChild(wrapper);
    }
    submit.textContent = `Run ${active.label}`;
    form.classList.remove("hidden");
    showNotice("");
    syncControls();
  };

  const readValues = () => {
    const values = {};
    for (const field of active.fields || []) {
      const input = document.getElementById(`field-${field.name}`);
      if (field.type === "checkbox") {
        // Send false explicitly: omitting an unchecked default-true flag would
        // cause the CLI to silently restore its true default.
        values[field.name] = [String(input.checked)];
      } else if (field.repeatable) {
        const items = input.value.split(/\r?\n/).map((value) => value.trim()).filter(Boolean);
        if (items.length) values[field.name] = items;
      } else if (input.value !== "") {
        values[field.name] = [input.value];
      }
    }
    return values;
  };

  const prettyOutput = (value) => {
    if (!value) return "No output.";
    try { return JSON.stringify(JSON.parse(value), null, 2); } catch (_) { return value; }
  };

  selector.addEventListener("change", renderCommand);
  form.addEventListener("input", (event) => {
    // A confirmation belongs to the reviewed inputs, not later edits.
    if (event.target !== confirmation) confirmation.checked = false;
  });
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (running || runtimePending || runtimeTransitioning() || offlineBlocked() || !active || !form.reportValidity()) return;
    const command = active;
    const values = readValues();
    running = true;
    syncControls();
    state.textContent = "Running… Keep this page open until the result appears.";
    showNotice("");
    try {
      const response = await fetch("/admin/execute", {
        method: "POST",
        headers: { "Accept": "application/json", "Content-Type": "application/json" },
        body: JSON.stringify({ command: command.id, values, confirmed: confirmation.checked })
      });
      const result = await response.json();
      if (!response.ok) throw new Error(result.error || `Request failed (${response.status}).`);
      lastResult = { command: command.id, ...result };
      document.getElementById("result-title").textContent = `${command.label} · Result`;
      document.getElementById("result-status").textContent = result.exit_code === 0
        ? "Completed successfully (exit code 0)."
        : `Exit code ${result.exit_code}. Review the output and diagnostics, including any partial changes or required actions, before trying again.`;
      document.getElementById("result-output").textContent = prettyOutput(result.stdout);
      document.getElementById("result-diagnostics").textContent = result.stderr || "";
      document.getElementById("result-diagnostics-section").classList.toggle("hidden", !result.stderr);
      resultSection.classList.remove("hidden");
      resultSection.focus({ preventScroll: true });
      state.textContent = result.exit_code === 0 ? "Operation completed." : "Operation returned a nonzero exit code. Check the result.";
    } catch (error) {
      showNotice(`${error.message || "The result could not be received."} If execution began, its outcome may be unknown. Inspect the current state before retrying; refreshing does not retry the operation.`);
      state.textContent = "Result unavailable.";
    } finally {
      running = false;
      confirmation.checked = false;
      syncControls();
    }
  });

  document.getElementById("result-download").addEventListener("click", () => {
    if (!lastResult) return;
    const url = URL.createObjectURL(new Blob([JSON.stringify(lastResult, null, 2)], { type: "application/json" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = "mahoroba-operation-result.json";
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  });

  const load = async () => {
    try {
      const response = await fetch("/admin/commands", { headers: { "Accept": "application/json" }, cache: "no-store" });
      if (!response.ok) throw new Error(`Could not load operations (${response.status}).`);
      commands = await response.json();
      if (!Array.isArray(commands) || !commands.length) throw new Error("No management operations are available.");
      selector.replaceChildren();
      const groups = new Map();
      for (const command of commands) {
        if (!groups.has(command.group)) {
          const group = document.createElement("optgroup");
          group.label = command.group;
          groups.set(command.group, group);
          selector.appendChild(group);
        }
        groups.get(command.group).appendChild(new Option(command.label, command.id));
      }
      selector.disabled = false;
      renderCommand();
    } catch (error) {
      showNotice(error.message || "Could not load operations. Reload the page to try again.");
    }
  };
  load();
  pollRuntime();
})();
