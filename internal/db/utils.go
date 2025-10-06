package db

// joinStrings is a helper function to join string slices
func joinStrings(slice []string, sep string) string {
	result := ""
	for i, s := range slice {
		if i > 0 {
			result += sep
		}
		result += s
	}
	return result
}
