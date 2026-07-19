package ferricstore

import "fmt"

func validateSameSlotStringKeys(command string, keys []string) error {
	if len(keys) < 2 {
		return nil
	}
	firstSlot := routeSlotForString(keys[0])
	for _, key := range keys[1:] {
		if routeSlotForString(key) != firstSlot {
			return fmt.Errorf("%s requires keys in one hash slot", command)
		}
	}
	return nil
}
