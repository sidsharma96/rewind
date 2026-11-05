// This controlled executable exercises the example's process boundary. It does
// not implement Concordance search or stand in for a real-corpus observation.
package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		os.Exit(90)
	}
	if os.Args[1] == "child" {
		fmt.Println(os.Getpid())
		time.Sleep(time.Hour)
		return
	}
	if os.Args[1] != "search" {
		os.Exit(91)
	}
	query := os.Args[len(os.Args)-1]
	switch query {
	case "success":
		_, _ = os.Stdout.Write([]byte{0, 255, '4', '2', '\n'})
		_, _ = os.Stderr.Write([]byte{128, 'd'})
	case "failure":
		fmt.Fprintln(os.Stderr, "selected edition unavailable")
		os.Exit(17)
	case "empty":
	case "overflow":
		_, _ = os.Stdout.Write(bytes.Repeat([]byte{'x'}, 1<<20))
	case "wait":
		time.Sleep(time.Hour)
	case "drain":
		child := exec.Command(os.Args[0], "child")
		child.Stdout, child.Stderr = os.Stdout, os.Stderr
		if child.Start() != nil {
			os.Exit(92)
		}
	case "environment":
		if os.Getenv("REWIND_SERVICE_SECRET") != "" || os.Getenv("SSH_AUTH_SOCK") != "" || os.Getenv("CONCORDANCE_COLLECTION") != "fixture" {
			os.Exit(93)
		}
		fmt.Print("filtered")
	default:
		if strings.Contains(query, "ContainerRestartRules") {
			fmt.Print("Searching captured documentation | edition 1.36\nContainerRestartRules: retained fixture quote\n")
			return
		}
		fmt.Print("Showing 0 indexed matches\n")
	}
}
