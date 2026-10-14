// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2.0 License.
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2026 Datadog, Inc.

// Command crdcontract verifies that the committed compatibility CRD has not
// been rewritten by a generator or formatter.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
)

func main() {
	path := flag.String("path", "", "path to the compatibility CRD")
	want := flag.String("sha256", "", "expected lowercase SHA-256 digest")
	flag.Parse()

	if *path == "" || len(*want) != sha256.Size*2 {
		fmt.Fprintln(os.Stderr, "path and a 64-character sha256 digest are required")
		os.Exit(2)
	}
	if _, err := hex.DecodeString(*want); err != nil {
		fmt.Fprintf(os.Stderr, "invalid sha256 digest: %v\n", err)
		os.Exit(2)
	}
	data, err := os.ReadFile(*path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read CRD contract: %v\n", err)
		os.Exit(1)
	}
	got := fmt.Sprintf("%x", sha256.Sum256(data))
	if got != *want {
		fmt.Fprintf(os.Stderr, "CRD contract changed: got %s, want %s\n", got, *want)
		os.Exit(1)
	}
}
