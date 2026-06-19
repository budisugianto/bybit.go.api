package handlers

import (
	"fmt"
)

func ValidateParams(params map[string]any) error {
	for key, value := range params {
		if key == "" {
			return fmt.Errorf("empty key found in parameters")
		}
		if value == nil {
			return fmt.Errorf("parameter for key '%s' is nil", key)
		}
	}
	return nil
}
