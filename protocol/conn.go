package protocol

import (
	"log"
	"net"
	"runtime/debug"

	"github.com/go-mysql-org/go-mysql/server"

	"lns.com/bptree/catalog"
	"lns.com/bptree/storage"
	"lns.com/bptree/txn"
)

// Server wraps the go-mysql server.
type Server struct {
	listener net.Listener
	svr      *server.Server
	engine   *storage.StorageEngine
	mgr      *txn.Manager
	cat      *catalog.Catalog
}

func NewServer(addr string, engine *storage.StorageEngine, mgr *txn.Manager, cat *catalog.Catalog) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Server{
		listener: ln,
		svr:      server.NewDefaultServer(),
		engine:   engine,
		mgr:      mgr,
		cat:      cat,
	}, nil
}

// Accept accepts and serves a single connection.
func (s *Server) Accept() error {
	conn, err := s.listener.Accept()
	if err != nil {
		return err
	}

	auth := server.NewInMemoryAuthenticationHandler()
	auth.AddUser("root", "")

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("connection panic recovered: %v\nStack: %s", r, debug.Stack())
			}
			conn.Close()
		}()

		// Per-connection handler — fully independent executor/txn state.
		handler := NewLnsHandler(s.engine, s.mgr, s.cat)
		defer handler.CloseConn()

		c, err := server.NewCustomizedConn(conn, s.svr, auth, handler)
		if err != nil {
			log.Printf("connection setup error: %v", err)
			return
		}

		log.Printf("client connected from %s", conn.RemoteAddr())
		for {
			if err := c.HandleCommand(); err != nil {
				break
			}
		}
		log.Printf("client disconnected from %s", conn.RemoteAddr())
	}()

	return nil
}

// Serve accepts connections in a loop.
func (s *Server) Serve() error {
	for {
		if err := s.Accept(); err != nil {
			return err
		}
	}
}

// Close closes the server.
func (s *Server) Close() error {
	return s.listener.Close()
}

// Addr returns the listen address.
func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}
