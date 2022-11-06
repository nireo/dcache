package server

import (
	"github.com/hashicorp/raft"
	"github.com/nireo/dcache/store"
	"github.com/valyala/fasthttp"
)

type Server struct {
	store *store.Store
	addr  string
}

func New(s *store.Store, addr string) (*Server, error) {
	return &Server{store: s, addr: addr}, nil
}

func (s *Server) handler(ctx *fasthttp.RequestCtx) {
	if ctx.IsPost() {
		var postData []byte
		copy(postData, ctx.PostBody())

		key := string(ctx.RequestURI()[1:])
		err := s.store.Set(key, postData)
		if err != nil {
			if err == raft.ErrNotLeader {
				// TODO: redirect request
			}
			ctx.Error("error writing to cluster", fasthttp.StatusInternalServerError)
			return
		}
		ctx.SetStatusCode(fasthttp.StatusOK)

		// set value
		return
	}

	key := string(ctx.RequestURI()[1:])
	data, err := s.store.Get(key)
	if err != nil {
		if err == raft.ErrNotLeader {
			// TODO: redirect request
		}
		ctx.Error("error getting from cluster", fasthttp.StatusInternalServerError)
		return
	}

	ctx.Write(data)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func (s *Server) Start() error {
	return fasthttp.ListenAndServe(s.addr, s.handler)
}
