package http

import (
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

func (s *Server) Start() error {
	return fasthttp.ListenAndServe(s.addr, s.handler)
}
