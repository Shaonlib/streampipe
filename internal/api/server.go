package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/Shaonlib/streampipe/internal/config"
)

// Controller is the interface the API uses to interact with the pipeline.
type Controller interface {
	Pause()
	Resume()
	LSN() uint64
}

type statusResponse struct {
	Status    string    `json:"status"`
	Paused    bool      `json:"paused"`
	LSN       string    `json:"lsn"`
	Slot      string    `json:"slot"`
	StartedAt time.Time `json:"started_at"`
}

// Server exposes the HTTP API.
type Server struct {
	cfg        *config.Config
	ctrl       Controller
	paused     bool
	startedAt  time.Time
	httpServer *http.Server
}

func New(cfg *config.Config, ctrl Controller) *Server {
	return &Server{
		cfg:       cfg,
		ctrl:      ctrl,
		startedAt: time.Now(),
	}
}

func (s *Server) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /metrics", promhttp.Handler().ServeHTTP)
	mux.HandleFunc("POST /pause", s.handlePause)
	mux.HandleFunc("POST /resume", s.handleResume)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	})

	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.cfg.API.Port),
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(shutCtx)
	}()

	slog.Info("API server listening", "port", s.cfg.API.Port)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("api server: %w", err)
	}
	return nil
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	lsn := pglogrepl.LSN(s.ctrl.LSN())
	resp := statusResponse{
		Status:    "running",
		Paused:    s.paused,
		LSN:       lsn.String(),
		Slot:      s.cfg.Source.SlotName,
		StartedAt: s.startedAt,
	}
	if s.paused {
		resp.Status = "paused"
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handlePause(w http.ResponseWriter, r *http.Request) {
	s.ctrl.Pause()
	s.paused = true
	slog.Info("pipeline paused via API")
	writeJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

func (s *Server) handleResume(w http.ResponseWriter, r *http.Request) {
	s.ctrl.Resume()
	s.paused = false
	slog.Info("pipeline resumed via API")
	writeJSON(w, http.StatusOK, map[string]string{"status": "running"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
