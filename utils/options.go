package utils

// Options passed in arguments
type Options struct {
	IgnoreMissingTopics bool // Don't fail if topic name was provided in the arguments, but not found in cluster, print warning instead
}
