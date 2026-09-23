package main

// buildGitRevision is set only by the reviewed static OCI build. Local and
// Windows builds retain Go's authenticated VCS build settings instead.
var buildGitRevision string

func validBuildGitRevision(value string) bool {
	if len(value) != 40 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			if character < 'a' || character > 'f' {
				return false
			}
		}
	}
	return true
}
