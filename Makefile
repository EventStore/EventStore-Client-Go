proto::
	mkdir -p protos/subscription
	protoc -I ./protos --go-grpc_out=./protos/subscription --go-grpc_opt=paths=source_relative persistent.proto
	protoc --proto_path=protos --go_out=./protos/subscription --go_opt=paths=source_relative persistent.proto
	mkdir -p protos/projections
	protoc -I ./protos --go-grpc_out=./protos/projections --go-grpc_opt=paths=source_relative projections.proto
	protoc --proto_path=protos --go_out=./protos/projections --go_opt=paths=source_relative projections.proto