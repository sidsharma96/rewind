//go:build unix

package main

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

func searchProcess(cmd *exec.Cmd) (func(), error) {
	// A new session has its own process group and no controlling terminal.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	stop := func() error {
		if cmd.Process == nil {
			return nil
		}
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.Cancel = stop
	return func() { _ = stop() }, nil
}

func searchSignal(state *os.ProcessState) string {
	if state == nil {
		return ""
	}
	if status, ok := state.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return status.Signal().String()
	}
	return ""
}
