package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/sidsharma96/rewind/internal/access"
	"github.com/sidsharma96/rewind/internal/httpapi"
)

var errArguments = errors.New("invalid command configuration; use rewind help")

const clientByteLimit = 16 << 20

func readPrivate(path string, max int) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > int64(max) {
		return nil, errArguments
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errArguments
	}
	defer file.Close()
	info, err = file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > int64(max) {
		return nil, errArguments
	}
	data, err := io.ReadAll(io.LimitReader(file, int64(max)+1))
	if err != nil || len(data) > max {
		return nil, errArguments
	}
	return data, nil
}

func readPublic(path string, max int) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errArguments
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > int64(max) {
		return nil, errArguments
	}
	data, err := io.ReadAll(io.LimitReader(file, int64(max)+1))
	if err != nil || len(data) > max {
		return nil, errArguments
	}
	return data, nil
}

func writePrivate(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return errors.New("could not create new private output file")
	}
	_, writeErr := file.Write(data)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		return errors.New("private output write or close failed")
	}
	return nil
}
func decodeInput(data []byte, out any) error { return httpapi.DecodeDocument(data, out) }

func clientCommand(ctx context.Context, name string, args []string, out io.Writer) error {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	endpoint := flags.String("endpoint", "", "HTTPS service origin")
	tokenFile := flags.String("token-file", "", "private credential file")
	caFile := flags.String("ca-file", "", "optional service CA certificate")
	project := flags.String("project", "", "project literal bytes")
	projectHex := flags.String("project-hex", "", "project hexadecimal bytes")
	trace := flags.String("trace", "", "trace ID")
	span := flags.String("span", "", "span ID")
	run := flags.String("run", "", "registered run")
	instance := flags.String("instance", "", "registered instance")
	file := flags.String("file", "", "private request JSON file")
	position := flags.String("cursor", "", "trace continuation")
	output := flags.String("out", "", "new private response file")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *project != "" && *projectHex != "" {
		return errArguments
	}
	client, transport, err := readerClient(*endpoint, *tokenFile, *caFile)
	if err != nil {
		return err
	}
	defer transport.CloseIdleConnections()
	key := *projectHex
	if *project != "" {
		key = httpapi.Opaque(*project)
	}
	e := httpapi.Execution{Project: key, Trace: *trace, Span: *span}
	var result any
	switch name {
	case "register":
		if *run == "" || *instance == "" {
			return errArguments
		}
		result, err = client.Register(ctx, e, httpapi.RegisterInput{Run: httpapi.Opaque(*run), Instance: httpapi.Opaque(*instance)})
	case "save-span":
		var in httpapi.SpanInput
		var data []byte
		data, err = readPrivate(*file, clientByteLimit)
		if err == nil {
			err = decodeInput(data, &in)
		}
		if err == nil {
			result, err = client.SaveSpan(ctx, e, in)
		}
	case "save-capture":
		var in httpapi.CaptureInput
		var data []byte
		data, err = readPrivate(*file, clientByteLimit)
		if err == nil {
			err = decodeInput(data, &in)
		}
		if err == nil {
			result, err = client.SaveCapture(ctx, e, in)
		}
	case "trace":
		result, err = client.Trace(ctx, key, *trace, *position)
	case "capture":
		result, err = client.Capture(ctx, e)
	case "playback":
		var saved httpapi.Result
		saved, err = client.Playback(ctx, e)
		if err == nil && *output != "" {
			var data []byte
			data, err = saved.Bytes()
			if err == nil {
				err = writePrivate(*output, data)
			}
			if err == nil {
				result = struct {
					Execution    httpapi.Execution `json:"execution"`
					Receipt      httpapi.Receipt   `json:"receipt"`
					RequestState string            `json:"request_state"`
					SavedBytes   int               `json:"saved_bytes"`
				}{saved.Execution, saved.Receipt, string(saved.RequestState), len(data)}
			}
		} else {
			result = saved
		}
	default:
		return errArguments
	}
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

// readerClient builds the HTTPS client shared by credentialed commands. The caller
// closes the transport's idle connections when it is done.
func readerClient(endpoint, tokenFile, caFile string) (*httpapi.Client, *http.Transport, error) {
	token, err := readPrivate(tokenFile, 65)
	if err != nil {
		return nil, nil, err
	}
	if len(token) == 65 && token[64] == '\n' {
		token = token[:64]
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		data, err := readPublic(caFile, 1<<20)
		if err != nil || len(data) > 1<<20 {
			return nil, nil, errArguments
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(data) {
			return nil, nil, errArguments
		}
		tlsConfig.RootCAs = roots
	}
	transport := &http.Transport{DisableCompression: true, TLSClientConfig: tlsConfig, DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 20 * time.Second}).DialContext,
		TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 20 * time.Second, MaxResponseHeaderBytes: 16 << 10, MaxConnsPerHost: 2, MaxIdleConns: 2, IdleConnTimeout: 20 * time.Second}
	client, err := httpapi.NewClient(endpoint, string(token), &http.Client{Transport: transport, Timeout: 30 * time.Second}, clientByteLimit, clientByteLimit)
	if err != nil {
		return nil, nil, errArguments
	}
	return client, transport, nil
}
func tokenCommand(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("token", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	path := flags.String("out", "", "new private credential file")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *path == "" {
		return errArguments
	}
	token, digest, err := access.NewToken()
	if err != nil {
		return err
	}
	if err = writePrivate(*path, []byte(token+"\n")); err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, "Credential SHA-256:", hex.EncodeToString(digest[:]))
	return err
}
