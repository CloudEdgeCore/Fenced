// Package runtimesdk provides the developer kit for building third-party Fenced Runtimes
// conforming to fenced.runtime.interface/v1.
package runtimesdk

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/CloudEdgeCore/Fenced/sdk/agent"
)

// LifecycleHandler defines the 7 explicit lifecycle methods a third-party runtime implements:
// health / start / event / result / checkpoint / restore / stop.
type LifecycleHandler interface {
	Health(ctx context.Context) (agent.HealthResponse, error)
	Start(ctx context.Context, req agent.StartRequest) (agent.StartResponse, error)
	Event(ctx context.Context, executionID string, after int64) (agent.EventList, error)
	Result(ctx context.Context, executionID string) (agent.Result, error)
	Checkpoint(ctx context.Context, executionID string) (agent.CheckpointResponse, error)
	Restore(ctx context.Context, req agent.RestoreRequest) (agent.RestoreResponse, error)
	Stop(ctx context.Context, executionID string) (agent.StopResponse, error)
}

// HandlerToHTTP creates an http.Handler serving the fenced.runtime.interface/v1 specification
// from a LifecycleHandler implementation.
func HandlerToHTTP(h LifecycleHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		trimmed := path
		if strings.HasPrefix(path, "/v1/") {
			trimmed = strings.TrimPrefix(path, "/v1")
		} else if strings.HasPrefix(path, "/v1alpha1/") {
			trimmed = strings.TrimPrefix(path, "/v1alpha1")
		}

		switch {
		case r.Method == http.MethodGet && (trimmed == "/health" || path == "/health" || path == "/v1/health"):
			resp, err := h.Health(r.Context())
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			writeJSON(w, http.StatusOK, resp)

		case r.Method == http.MethodPost && (trimmed == "/executions:start" || path == "/start" || path == "/v1/executions:start"):
			var req agent.StartRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			// Capability verification: default-deny capabilities
			if req.Capabilities.Secrets == nil {
				writeError(w, http.StatusUnprocessableEntity, errors.New("implicit capabilities denied: secrets array cannot be nil"))
				return
			}
			resp, err := h.Start(r.Context(), req)
			if err != nil {
				if errors.Is(err, agent.ErrExecutionConflict) {
					writeError(w, http.StatusConflict, err)
					return
				}
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			writeJSON(w, http.StatusAccepted, resp)

		case strings.HasPrefix(trimmed, "/executions/"):
			rest := strings.TrimPrefix(trimmed, "/executions/")
			var executionID, action string
			if idx := strings.Index(rest, ":"); idx != -1 {
				executionID = rest[:idx]
				action = rest[idx:]
			} else if idx := strings.Index(rest, "/"); idx != -1 {
				executionID = rest[:idx]
				action = rest[idx+1:]
			} else {
				executionID = rest
			}
			unescapedID, err := url.PathUnescape(executionID)
			if err == nil && unescapedID != "" {
				executionID = unescapedID
			}

			switch {
			case r.Method == http.MethodPost && action == ":stop":
				resp, err := h.Stop(r.Context(), executionID)
				if err != nil {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
				writeJSON(w, http.StatusAccepted, resp)

			case r.Method == http.MethodPost && action == ":checkpoint":
				resp, err := h.Checkpoint(r.Context(), executionID)
				if err != nil {
					if errors.Is(err, agent.ErrExecutionNotFound) {
						writeError(w, http.StatusNotFound, err)
						return
					}
					writeError(w, http.StatusInternalServerError, err)
					return
				}
				writeJSON(w, http.StatusOK, resp)

			case r.Method == http.MethodPost && action == ":restore":
				var req agent.RestoreRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					writeError(w, http.StatusBadRequest, err)
					return
				}
				resp, err := h.Restore(r.Context(), req)
				if err != nil {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
				writeJSON(w, http.StatusOK, resp)

			case r.Method == http.MethodGet && action == "events":
				afterStr := r.URL.Query().Get("after")
				var after int64
				if afterStr != "" {
					parsed, _ := strconv.ParseInt(afterStr, 10, 64)
					after = parsed
				}
				resp, err := h.Event(r.Context(), executionID, after)
				if err != nil {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
				writeJSON(w, http.StatusOK, resp)

			case r.Method == http.MethodGet && action == "result":
				resp, err := h.Result(r.Context(), executionID)
				if err != nil {
					if errors.Is(err, agent.ErrExecutionNotFound) {
						writeError(w, http.StatusNotFound, err)
						return
					}
					writeError(w, http.StatusInternalServerError, err)
					return
				}
				if resp.Status == agent.StatusAccepted || resp.Status == agent.StatusRunning {
					writeJSON(w, http.StatusAccepted, resp)
					return
				}
				writeJSON(w, http.StatusOK, resp)

			default:
				writeError(w, http.StatusNotFound, errors.New("route not found"))
			}

		// Simple flat endpoints for development convenience
		case r.Method == http.MethodGet && path == "/events":
			executionID := r.URL.Query().Get("executionId")
			afterStr := r.URL.Query().Get("after")
			var after int64
			if afterStr != "" {
				parsed, _ := strconv.ParseInt(afterStr, 10, 64)
				after = parsed
			}
			resp, err := h.Event(r.Context(), executionID, after)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			writeJSON(w, http.StatusOK, resp)

		case r.Method == http.MethodGet && path == "/result":
			executionID := r.URL.Query().Get("executionId")
			resp, err := h.Result(r.Context(), executionID)
			if err != nil {
				if errors.Is(err, agent.ErrExecutionNotFound) {
					writeError(w, http.StatusNotFound, err)
					return
				}
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			if resp.Status == agent.StatusAccepted || resp.Status == agent.StatusRunning {
				writeJSON(w, http.StatusAccepted, resp)
				return
			}
			writeJSON(w, http.StatusOK, resp)

		case r.Method == http.MethodPost && path == "/checkpoint":
			var req struct {
				ExecutionID string `json:"executionId"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			resp, err := h.Checkpoint(r.Context(), req.ExecutionID)
			if err != nil {
				if errors.Is(err, agent.ErrExecutionNotFound) {
					writeError(w, http.StatusNotFound, err)
					return
				}
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			writeJSON(w, http.StatusOK, resp)

		case r.Method == http.MethodPost && path == "/restore":
			var req agent.RestoreRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			resp, err := h.Restore(r.Context(), req)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			writeJSON(w, http.StatusOK, resp)

		case r.Method == http.MethodPost && path == "/stop":
			var req struct {
				ExecutionID string `json:"executionId"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				writeError(w, http.StatusBadRequest, err)
				return
			}
			resp, err := h.Stop(r.Context(), req.ExecutionID)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			writeJSON(w, http.StatusAccepted, resp)

		default:
			writeError(w, http.StatusNotFound, errors.New("route not found"))
		}
	})
}

// Serve starts an HTTP server for the given LifecycleHandler on the specified address.
func Serve(h LifecycleHandler, addr string) error {
	server := &http.Server{
		Addr:              addr,
		Handler:           HandlerToHTTP(h),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return server.ListenAndServe()
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Fenced-Runtime-Interface", agent.ProtocolVersion)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Fenced-Runtime-Interface", agent.ProtocolVersion)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":  err.Error(),
		"status": status,
	})
}
