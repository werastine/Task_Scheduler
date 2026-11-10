package worker

// Config gives posibility to choose:
//
// workerNum is number of parallel working goroutines
type Config struct {
	WorkerNum int
}
