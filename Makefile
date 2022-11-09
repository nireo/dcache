compile:
	protoc api/api.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.

check:
	gofumpt -l -w .
	golines -w .
	staticcheck ./...
