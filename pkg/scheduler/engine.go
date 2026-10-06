package scheduler

type Engine struct {
	//pgxpool
	//redis
}

func NewEngine() {}

func SendNow(
	taskType string,
	payload []byte,
) (taskID string, err error) {
	return "", nil
}
