package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
	"github.com/CloudEdgeCore/Fenced/internal/platform/slo"
)

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "fenced-slo:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("fenced-slo", flag.ContinueOnError)
	flags.SetOutput(stderr)
	samplePath := flags.String("sample", "", "measured fenced.slo/v1 sample JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *samplePath == "" {
		return errors.New("-sample is required")
	}
	encoded, err := os.ReadFile(*samplePath)
	if err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var sample slo.Sample
	if err := decoder.Decode(&sample); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("sample must contain exactly one JSON document")
	}
	report, err := slo.Evaluate(sample)
	if err != nil {
		return err
	}
	result, err := json.Marshal(report)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(stdout, string(result)); err != nil {
		return err
	}
	if !report.Passed {
		return errors.New("one or more v1 SLO objectives failed")
	}
	return nil
}
