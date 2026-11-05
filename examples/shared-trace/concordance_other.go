//go:build !unix

package main

import (
	"errors"
	"os"
	"os/exec"
)

func searchProcess(*exec.Cmd) (func(), error) {
	return nil, errors.New("bob-search requires Unix process-group cleanup")
}
func searchSignal(*os.ProcessState) string { return "" }
