package util

import (
	"bytes"
	"encoding/json"
	"io"
)

func ReadString(r io.Reader) string {
	var b bytes.Buffer
	b.ReadFrom(r)
	return b.String()
}

func ReadBytes(r io.Reader) []byte {
	var b bytes.Buffer
	b.ReadFrom(r)
	return b.Bytes()
}

func MustMarshal(data any) string {
	marshalled, err := json.Marshal(data)
	if err != nil {
		Log.Fatalf("failed to marshal json: %v", err)
	}
	return string(marshalled)
}
