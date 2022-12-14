proto:
	protoc pb/pb.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

check:
	gofumpt -l -w .
	golines -w .
	staticcheck ./...

dcache:
	go build -o dcache ./cmd/dcache/main.go

client:
	go build -o dcache-client ./cmd/client/main.go

dcache-stripped:
	go build -o dcache -ldflags="-s -w" ./cmd/dcache/main.go

test:
	go test -v ./...
