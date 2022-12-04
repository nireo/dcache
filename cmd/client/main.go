package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/nireo/dcache/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// configuration flags.
	addr := flag.String("addr", "localhost:9200", "Address for the gRPC server")

	// if the client should retrieve the servers.
	getServers := flag.Bool("get-servers", false, "Get servers")

	// key is given as flag, but value is read from stdin.
	key := flag.String("key", "", "Key for set operation.")
	flag.Parse()

	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("cannot dial addr: %s", err)
	}
	client := pb.NewCacheClient(conn)

	if *getServers {
		res, err := client.GetServers(context.Background(), &pb.Empty{})
		if err != nil {
			log.Fatalf("error getting servers from server: %s", err)
		}

		for _, s := range res.Server {
			fmt.Println(s)
		}
		return
	}

	if key == nil {
		log.Fatalf("key needs to be set.")
	}

	// if not get servers operation send key-value pair.
	val, err := io.ReadAll(os.Stdin)
	if err != nil {
		log.Fatalf("error reading stdin: %s", err)
	}

	_, err = client.Set(context.Background(), &pb.SetRequest{
		Key:   *key,
		Value: val,
	})
	if err != nil {
		log.Fatalf("failed setting value: %s", err)
	}

	log.Printf("set value successfully.")
}
