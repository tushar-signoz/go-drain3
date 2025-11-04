package drain

import (
	"strings"
	"time"
	"unicode"
)

// nowFunc is a variable that can be overridden for testing
var nowFunc = time.Now

func Tokenize(input string) []string {
	var tokens []string
	var buf []rune
	runes := []rune(input)
	n := len(runes)

	i := 0
	for i < n {
		r := runes[i]

		// --- Handle multi-character delimiters ---
		// Check 3-char: " - "
		if i+2 < n && runes[i] == ' ' && runes[i+1] == '-' && runes[i+2] == ' ' {
			if len(buf) > 0 {
				tokens = append(tokens, string(buf))
				buf = buf[:0]
			}
			tokens = append(tokens, " - ")
			i += 3
			continue
		}

		// Check 2-char delimiters: " :", ": ", " -", " |", "| "
		if i+1 < n {
			pair := string(runes[i : i+2])
			switch pair {
			case " :", ": ", " -", " |", "| ":
				if len(buf) > 0 {
					tokens = append(tokens, string(buf))
					buf = buf[:0]
				}
				tokens = append(tokens, pair)
				i += 2
				continue
			}
		}

		// --- Handle single-character delimiters ---
		switch r {
		case ':', '=', ',', '|', ';', '/':
			if len(buf) > 0 {
				tokens = append(tokens, string(buf))
				buf = buf[:0]
			}
			tokens = append(tokens, string(r))
			i++
			continue

		default:
			if unicode.IsSpace(r) {
				if len(buf) > 0 {
					tokens = append(tokens, string(buf))
					buf = buf[:0]
				}
				// optional: skip or include whitespace token
				// tokens = append(tokens, string(r))
				i++
				continue
			}
			// Normal character → append to buffer
			buf = append(buf, r)
			i++
		}
	}

	if len(buf) > 0 {
		tokens = append(tokens, string(buf))
	}
	return tokens
}

// HasNumbers checks if a token contains numbers
func HasNumbers(s string) bool {
	for _, r := range s {
		if unicode.IsDigit(r) {
			return true
		}
	}
	return false
}

// IsHexNumber checks if a token is a hexadecimal number
func IsHexNumber(s string) bool {
	if len(s) < 2 {
		return false
	}

	// Check for 0x prefix
	if strings.HasPrefix(strings.ToLower(s), "0x") {
		s = s[2:]
	}

	for _, r := range s {
		if !unicode.IsDigit(r) && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
			return false
		}
	}
	return len(s) > 0
}

// TokenCategory categorizes a token for tree navigation
func TokenCategory(token string) string {
	// Check for common patterns
	if token == "" {
		return "<EMPTY>"
	}

	// Check if it's all digits
	if isAllDigits(token) {
		return "<NUM>"
	}

	// Check if it contains digits
	if HasNumbers(token) {
		return "<*>"
	}

	// Check if it's a hex number
	if IsHexNumber(token) {
		return "<HEX>"
	}

	// Return the token itself if it's a pure text token
	return token
}

func isAllDigits(s string) bool {
	if len(s) == 0 {
		return false
	}

	for _, r := range s {
		if !unicode.IsDigit(r) && r != '.' && r != '-' {
			return false
		}
	}
	return true
}

// NormalizeToken normalizes a token for comparison
func NormalizeToken(token string) string {
	token = strings.TrimSpace(token)
	return strings.ToLower(token)
}
