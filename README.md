# dcache

A distributed cache built in Go, which tries to keep the code as simple as possible. The cache can perform really fast as it contains options to keep everything in memory. There are still options that allow for persistance on for example Raft logs, but this shouldn't effect performance too much.

## Dependencies

This project uses quite a few dependencies to make the codebase more maintainable since code stays short. The most important dependencies are:

- [grpc](https://grpc.io) and Google's protocol buffers.
- [serf](https://github.com/hashicorp/serf) for cluster management.
- [raft](https://github.com/hashicorp/raft) for distributed consensus.
- [bigcache](https://github.com/allergo/bigcache) an efficient cache for storing gigabytes of data.
- [cmux](https://github.com/soheilhy/cmux) connection multipexer to make working with multiple connections easier.

The code tries to be simple where it can. For example the store uses its own serialization/deserialization code that is faster than a more generic format since it's for a specific data format and doesn't need to general. Rolling our own format allows us to not rely fully on a heavier library such as protobuf for simple encoding/decoding. This project also tries to use the Golang standard library in favour of external dependencies (not feasible for everything).

## Running server

First you need to compile the dcache server binary.

```
# for normal binary
make dcache

# for stripped binary
make dcache-stripped
```

Now you can start the server with either a configuration file or command line flags. Since configuration is handled by [viper](https://github.com/spf13/viper) dcache supports every configuration file format that viper supports!

```
Usage:
  dcache [flags]

Flags:
      --addr string       Address where serf is binded. (default "127.0.0.1:9000")
      --grpc              Enable gRPC server and use of grpc clients.
      --http              Enable HTTP service.
      --bootstrap         Whether this node should bootstrap the cluster.
      --conf string       Path to a configuration file.
      --data-dir string   Where to store raft logs. (default "/tmp/dcache")
  -h, --help              help for dcache
      --id string         Identifier on the cluster. (default "arch")
      --in-memory         Whether to keep even raft logs in memory. Improves performance but makes system less tolerant to failures. (default true)
      --join strings      Existing addresses in the cluster where you want this node to attempt connection
      --rpc-port int      Port for gRPC clients and Raft connections. (default 9200)
```

dcache supports using both gRPC and HTTP by using a connection multiplexer. Meaning that communication related to the service runs on the same port.

## gRPC server

### Client

Quick overview of the configuration flags:

```
Usage of ./dcache-client:
  -addr string
    	Address for the gRPC server (default "localhost:9200")
  -get-servers
    	If set to true, retrieve raft servers instead of writing a key-value pair into cache.
  -key string
      The key to be used with stdin to write a key-value pair.
```

### Examples

```
# get all servers in raft cluster.
./client --get-servers --addr="localhost:9001"
```

```
# write a file into the cache.
./client --key="cachedfile123" < path/to/file
```

### Using a custom client

```go
package main

import (
  "fmt"
  "log"

  "github.com/nireo/dcache/pb"
  "google.golang.org/grpc"
  "google.golang.org/grpc/credentials/insecure"
)

// NOTE:
// pb.Empty is just a struct without any fields, since protobuf doesn't
// have null. Just fill the param with &pb.Empty{}
//
// All endpoints:
// Set(ctx context.Context, req *pb.SetRequest) (*pb.Empty, error)
// Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error)
// GetServers(ctx context.Context, req *pb.Empty) (*pb.GetServer, error)

func main() {
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("cannot dial addr: %s", err)
	}
	client := pb.NewCacheClient(conn)
	res, err := client.GetServers(context.Background(), &pb.Empty{})
	if err != nil {
		log.Fatalf("error getting servers from server: %s", err)
	}

  for _, s := range res.Server {
    fmt.Println(s)
  }
  return
}
```

## HTTP Server

dcache also supports a HTTP interface. It is enabled by passing the `--http` flag into the `dcache` server binary.

### Usage

The server only supports `GET` and `POST` methods. Few examples will help you understand how the routes work:

```
# write the value "this is the value" with key "hello" in the cache.
curl -v -X POST -d 'this is the value' http://localhost:9200/hello

# get key from cache
curl -v http://localhost:9200/hello
```
