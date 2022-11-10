package http

// http.go - A very simple HTTP interface to interact with the store.

import (
	"github.com/nireo/dcache/store"
	"github.com/valyala/fasthttp"
)

type Server struct {
	store *store.Store
}

// New creates a Server instance with given raft store.
func New(s *store.Store) (*Server, error) {
	return &Server{store: s}, nil
}

// Handler handles HTTP requests in the following way:
//
//   - POST = Create entry, key is the request URI so 'localhost:0/testkey' key = "testkey"
//     and the body of the request will be the key-value pair's value.
//
//   - GET = Same thing with keys, but the value will be written as a response.
func (s *Server) Handler(ctx *fasthttp.RequestCtx) {
	key := string(ctx.RequestURI()[1:])
	if ctx.IsPost() {
		var postData []byte
		copy(postData, ctx.PostBody())

		err := s.store.Set(key, postData)
		if err != nil {
			ctx.Error("error writing to cluster", fasthttp.StatusInternalServerError)
			return
		}
		ctx.SetStatusCode(fasthttp.StatusOK)
		return
	}

	data, err := s.store.Get(key)
	if err != nil {
		ctx.Error("error getting from cluster", fasthttp.StatusInternalServerError)
		return
	}

	ctx.Write(data)
	ctx.SetStatusCode(fasthttp.StatusOK)
}
