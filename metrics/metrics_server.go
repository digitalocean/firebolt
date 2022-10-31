package metrics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

// Server is the HTTP server for prometheus metrics
type Server struct {
	server *http.Server
}

// StartServer starts the MetricsServer using the passed context to handle shutdown
func StartServer(ctx context.Context, port int) error {
	log.Info("starting prometheus http server")
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	s := &Server{
		server: server,
	}

	c := make(chan error)

	go s.run(c)
	log.Info("started prometheus http server")

	select {
	case err := <-c:
		return err
	case <-ctx.Done():
		return s.server.Shutdown(context.Background())
	}
}

func (s *Server) run(c chan<- error) {
	if err := s.server.ListenAndServe(); err != nil {
		c <- err
	}
}
