package drain

import (
	"strings"
	"unicode"
)

// Tokenizer handles log message tokenization
type Tokenizer struct {
	extraDelimiters map[rune]bool
}

// NewTokenizer creates a new tokenizer
func NewTokenizer(extraDelimiters []string) *Tokenizer {
	dset := make(map[rune]bool)
	for _, s := range extraDelimiters {
		for _, r := range s {
			dset[r] = true
		}
	}
	return &Tokenizer{extraDelimiters: dset}
}

// Tokenize splits input into tokens using a generated lexer-style FSM.
func (t *Tokenizer) Tokenize(input string) []string {
	var tokens []string
	var buf []rune

	for _, r := range input {
		switch {
		case unicode.IsSpace(r):
			// Emit current token
			if len(buf) > 0 {
				tokens = append(tokens, string(buf))
				buf = buf[:0]
			}
			tokens = append(tokens, string(r))
		case t.extraDelimiters[r]:
			// Delimiter ends current token
			if len(buf) > 0 {
				tokens = append(tokens, string(buf))
				buf = buf[:0]
			}
			// Optionally emit delimiter as a token
			tokens = append(tokens, string(r))
		default:
			buf = append(buf, r)
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
