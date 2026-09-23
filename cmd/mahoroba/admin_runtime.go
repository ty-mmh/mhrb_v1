package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"mahoroba.local/mahoroba/internal/httpui"
)

var errServeCleanupIncomplete = errors.New("runtime cleanup incomplete; restart the administration process before starting again")

type managedRuntimeRunner func(context.Context, func(string)) error

// The controller owns only the runtime it starts. It never discovers process
// IDs, takes over another listener, or releases another runtime's host lock.
type adminRuntimeController struct {
	parent    context.Context
	admission chan struct{}
	run       managedRuntimeRunner

	mu     sync.Mutex
	status httpui.AdminRuntimeStatus
	cancel context.CancelFunc
	done   chan struct{}
	closed bool
}

func newAdminRuntimeController(parent context.Context, url string, admission chan struct{}, run managedRuntimeRunner) *adminRuntimeController {
	return &adminRuntimeController{
		parent: parent, admission: admission, run: run,
		status: httpui.AdminRuntimeStatus{State: "stopped", URL: url,
			Message: "No dialogue server is managed by this administration process. Separately started servers are not controlled here."},
	}
}

func (controller *adminRuntimeController) Status(ctx context.Context) (httpui.AdminRuntimeStatus, error) {
	if err := ctx.Err(); err != nil {
		return httpui.AdminRuntimeStatus{}, err
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.status, nil
}

func (controller *adminRuntimeController) Start(ctx context.Context) (httpui.AdminRuntimeStatus, error) {
	if err := ctx.Err(); err != nil {
		return httpui.AdminRuntimeStatus{}, err
	}
	// Do not queue an unexpected later start behind a lengthy maintenance
	// operation. Its operator can retry after that operation has returned.
	select {
	case controller.admission <- struct{}{}:
		defer func() { <-controller.admission }()
	default:
		status, _ := controller.Status(ctx)
		return status, errors.New("another management operation is running; wait for its result before starting dialogue")
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.closed || controller.parent.Err() != nil {
		return controller.status, errors.New("administration is shutting down")
	}
	if controller.status.RestartRequired {
		return controller.status, errServeCleanupIncomplete
	}
	switch controller.status.State {
	case "starting", "running":
		return controller.status, nil
	case "stopping":
		return controller.status, errors.New("the dialogue server is still stopping")
	}
	runCtx, cancel := context.WithCancel(controller.parent)
	controller.cancel = cancel
	controller.done = make(chan struct{})
	controller.status.State = "starting"
	controller.status.Managed = true
	controller.status.Ready = false
	controller.status.Error = ""
	controller.status.Message = "Starting dialogue with the configured resident, readiness checks, and exclusive data-directory lock."
	done := controller.done
	go controller.runOwned(runCtx, done)
	return controller.status, nil
}

func (controller *adminRuntimeController) runOwned(ctx context.Context, done chan struct{}) {
	err := controller.run(ctx, func(url string) {
		controller.mu.Lock()
		defer controller.mu.Unlock()
		if controller.status.State == "starting" && ctx.Err() == nil {
			controller.status.State = "running"
			controller.status.Ready = true
			controller.status.URL = url
			controller.status.Message = "Dialogue startup completed. This administration process owns the server."
		}
	})
	controller.mu.Lock()
	defer controller.mu.Unlock()
	stoppedByOwner := controller.status.State == "stopping" || controller.closed || controller.parent.Err() != nil
	controller.cancel()
	controller.cancel = nil
	controller.status.Ready = false
	controller.status.Managed = false
	controller.status.RestartRequired = errors.Is(err, errServeCleanupIncomplete)
	if err == nil && !stoppedByOwner {
		err = errors.New("the dialogue server exited unexpectedly")
	}
	if stoppedByOwner && onlyContextCancellation(err) {
		err = nil
	}
	if err != nil {
		controller.status.State = "failed"
		controller.status.Error = err.Error()
		controller.status.Message = "The dialogue server failed. Review the error before starting it again."
		if controller.status.RestartRequired {
			controller.status.Managed = true
			controller.status.Message = "Cleanup could not finish safely. Restart the administration process before retrying; retained resources have not been released."
		}
	} else {
		controller.status.State = "stopped"
		controller.status.Error = ""
		controller.status.Message = "The managed dialogue server has stopped and released its resources."
	}
	close(done)
}

func onlyContextCancellation(err error) bool {
	if err == nil {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if !onlyContextCancellation(child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return onlyContextCancellation(wrapped.Unwrap())
	}
	return err == context.Canceled
}

func (controller *adminRuntimeController) Stop(ctx context.Context) (httpui.AdminRuntimeStatus, error) {
	if err := ctx.Err(); err != nil {
		return httpui.AdminRuntimeStatus{}, err
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.cancel == nil {
		// There is no owned context to cancel. In particular, a runtime that was
		// launched in another process remains untouched.
		return controller.status, nil
	}
	controller.status.State = "stopping"
	controller.status.Ready = false
	controller.status.Message = "Stopping dialogue and draining its workers, connections, and data-directory lock."
	controller.cancel()
	return controller.status, nil
}

// Called with the finite-command admission token held, so Start cannot pass
// between the state check and execution of an offline command.
func (controller *adminRuntimeController) AllowCommand(offline bool) error {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.closed {
		return errors.New("administration is shutting down")
	}
	if offline && (controller.cancel != nil || controller.status.RestartRequired) {
		return errors.New("stop the managed dialogue server and wait for Stopped before running this offline operation")
	}
	return nil
}

func (controller *adminRuntimeController) Close(ctx context.Context) error {
	controller.mu.Lock()
	controller.closed = true
	done := controller.done
	if controller.cancel != nil {
		controller.status.State = "stopping"
		controller.status.Ready = false
		controller.cancel()
	}
	controller.mu.Unlock()
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return fmt.Errorf("wait for managed dialogue shutdown: %w", ctx.Err())
		}
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if controller.status.RestartRequired {
		return fmt.Errorf("%w: %s", errServeCleanupIncomplete, controller.status.Error)
	}
	return nil
}

func (service *adminCLIService) RuntimeStatus(ctx context.Context) (httpui.AdminRuntimeStatus, error) {
	if service.runtime == nil {
		return httpui.AdminRuntimeStatus{State: "stopped", Message: "Runtime controls are not configured."}, errors.New("runtime controls are not configured")
	}
	return service.runtime.Status(ctx)
}

func (service *adminCLIService) StartRuntime(ctx context.Context) (httpui.AdminRuntimeStatus, error) {
	if service.runtime == nil {
		return service.RuntimeStatus(ctx)
	}
	return service.runtime.Start(ctx)
}

func (service *adminCLIService) StopRuntime(ctx context.Context) (httpui.AdminRuntimeStatus, error) {
	if service.runtime == nil {
		return service.RuntimeStatus(ctx)
	}
	return service.runtime.Stop(ctx)
}
