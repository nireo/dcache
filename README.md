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
      --bootstrap         Whether this node should bootstrap the cluster.
      --conf string       Path to a configuration file.
      --data-dir string   Where to store raft logs. (default "/tmp/dcache")
  -h, --help              help for dcache
      --id string         Identifier on the cluster. (default "arch")
      --in-memory         Whether to keep even raft logs in memory. Improves performance but makes system less tolerant to failures. (default true)
      --join strings      Existing addresses in the cluster where you want this node to attempt connection
      --rpc-port int      Port for gRPC clients and Raft connections. (default 9200)
```

Now you can connect to the server using a gRPC client. (HTTP support coming soon)

## Using the client

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
