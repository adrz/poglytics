package util

import (
	"math/rand"
)

// GenerateRandomString generates a random string of specified length and character type
func GenerateRandomString(length int, charType string) string {
	var chars string
	switch charType {
	case "letters":
		chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	case "digits":
		chars = "0123456789"
	}

	result := make([]byte, length)
	for i := range result {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}
