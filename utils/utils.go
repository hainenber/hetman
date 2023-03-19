package utils

import "os"

func IsReadable(filepaths []string) []error {
	errors := []error{}
	for _, filepath := range filepaths {
		_, err := os.Open(filepath)
		if err != nil {
			errors = append(errors, err)
		}
	}
	return errors
}
