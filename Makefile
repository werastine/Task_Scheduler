.PHONY: proto clean

MODULE = github.com/werastine/Task_Scheduler

proto:
	protoc -I api/proto \
		   --go_out=internal/pb --go_opt=paths=source_relative \
    	   --go-grpc_out=internal/pb --go-grpc_opt=paths=source_relative \
    	   api/proto/scheduler.proto

proto-clean:
	rm -f internal/pb/*.pb.go