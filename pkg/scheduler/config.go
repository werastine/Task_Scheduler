package scheduler

import "time"

// Config gives posibility to choose:
//
// failRepeat is how much ....
type Config struct {
	FailRepeat  int
	FailTimeout time.Duration
}
