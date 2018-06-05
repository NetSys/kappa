package util

import (
	cryptoRand "crypto/rand"
	"encoding/binary"
	"math/rand"
	"strings"
)

// MakeRandomASCII returns a pseudo-random string, containing digits and lower-case letters, with >= 128-bit randomness.
func MakeRandomASCII128() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	const length = 26 // Each byte has log2(26+10) = 5 bits of randomness, so need 128/5 = 26 bytes.
	b := make([]byte, length)
	for i := range b {
		b[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return string(b)
}

// parsePlatformPrefix turns "pf:/some/path" into ("pf", "/some/path"), and "/some/path" into ("", "/some/path").
func parsePlatformPrefix(path string) (prefix string, actualPath string) {
	split := strings.SplitN(path, ":", 2)
	if len(split) == 1 { // No prefix.
		return "", path
	}
	return split[0], split[1]
}

// ParseFilterPathByPlatform filters out the actual paths for the given platform.
// Such paths either have no prefix or have the platform name as prefix.
func ParseFilterPathByPlatform(platform string, paths []string) []string {
	var pp []string
	for _, p := range paths {
		prefix, actualPath := parsePlatformPrefix(p)
		if prefix == "" || prefix == platform {
			pp = append(pp, actualPath)
		}
	}
	return pp
}

func init() {
	var seed int64
	binary.Read(cryptoRand.Reader, binary.LittleEndian, &seed)
	rand.Seed(seed)
}
