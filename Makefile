.PHONY: proto clean

MODULE = github.com/werastine/Task_Scheduler

proto:
	protoc -I api/proto \
		   --go_out=pkg/api/scheduler/v1 --go_opt=paths=source_relative \
    	   --go-grpc_out=pkg/api/scheduler/v1 --go-grpc_opt=paths=source_relative \
    	   api/proto/scheduler.proto

proto-clean:
	rm -f pkg/api/scheduler/v1/*.pb.go