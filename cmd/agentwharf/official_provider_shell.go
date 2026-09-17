package main

import (
	"errors"
	"strings"
)

func quoteOfficialWindowsBatchLine(elements []string) (string, error) {
	quoted := make([]string, 0, len(elements))
	for _, element := range elements {
		if strings.ContainsAny(element, "\r\n\x00") {
			return "", errors.New("official provider argument contains a forbidden control character")
		}
		quoted = append(quoted, `"`+strings.ReplaceAll(element, `"`, `""`)+`"`)
	}
	return strings.Join(quoted, " "), nil
}
