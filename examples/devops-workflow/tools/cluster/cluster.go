// Package cluster implements the DevOps reference workload's deterministic
// tools as a webhook-backed tool endpoint behind the Fenced Tool Gateway:
//
//	kubernetes.get@1.0.0       inspect a service's pod health
//	kubernetes.logs@1.0.0      recent log lines for a service
//	kubernetes.restart@1.0.0   restart a service's pods (heals the incident)
//	kubernetes.scale@1.0.0     scale a service's replicas
//	server.exec@1.0.0          run a shell command on the target host
//
// The fake cluster carries one in-memory service ("checkout") that starts in
// an unhealthy state; restart heals it unless the scenario marks the cluster
// as stubborn (restart fails to heal), which exercises the rollback path.
package cluster

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
)

// State is one snapshot of the fake cluster service.
type State struct {
	Service       string `json:"service"`
	Healthy       bool   `json:"healthy"`
	Replicas      int    `json:"replicas"`
	ReadyReplicas int    `json:"readyReplicas"`
	RestartCount  int    `json:"restartCount"`
	Reason        string `json:"reason,omitempty"`
	ExecCalls     int    `json:"execCalls"`
}

// Cluster is the deterministic in-memory target of the DevOps tools.
type Cluster struct {
	mu sync.Mutex
	// Stubborn makes restart fail to heal, forcing the rollback path.
	Stubborn bool
	state    State
	logs     []string
}

// New builds a cluster with a checkout service that starts unhealthy.
func New(stubborn bool) *Cluster {
	return &Cluster{
		Stubborn: stubborn,
		state: State{
			Service: "checkout", Healthy: false, Replicas: 3, ReadyReplicas: 2,
			RestartCount: 0, Reason: "OOMKilled",
		},
		logs: []string{
			"[checkout-2] OOMKilled: container exceeded memory limit",
			"[checkout-2] crash-loop: restarting in 5s",
			"[checkout-1] ready",
			"[checkout-0] ready",
		},
	}
}

// Snapshot returns a copy of the current state.
func (c *Cluster) Snapshot() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// Get implements kubernetes.get.
func (c *Cluster) Get(service string) (State, error) {
	if service != "checkout" {
		return State{}, fmt.Errorf("service %q not found", service)
	}
	return c.Snapshot(), nil
}

// Logs implements kubernetes.logs.
func (c *Cluster) Logs(service string, lines int) ([]string, error) {
	if service != "checkout" {
		return nil, fmt.Errorf("service %q not found", service)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if lines <= 0 || lines > len(c.logs) {
		lines = len(c.logs)
	}
	out := append([]string(nil), c.logs[len(c.logs)-lines:]...)
	return out, nil
}

// Restart implements kubernetes.restart. It heals the service unless the
// cluster is stubborn (the rollback acceptance uses this).
func (c *Cluster) Restart(service string) (State, error) {
	if service != "checkout" {
		return State{}, fmt.Errorf("service %q not found", service)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.RestartCount++
	c.state.ReadyReplicas = c.state.Replicas
	if !c.Stubborn {
		c.state.Healthy = true
		c.state.Reason = ""
		c.logs = append(c.logs, "[checkout-2] restarted: healthy")
	} else {
		c.state.Healthy = false
		c.state.Reason = "OOMKilled (stubborn)"
		c.logs = append(c.logs, "[checkout-2] restarted but OOMKilled again")
	}
	return c.state, nil
}

// Scale implements kubernetes.scale (used by rollback to restore replicas).
func (c *Cluster) Scale(service string, replicas int) (State, error) {
	if service != "checkout" {
		return State{}, fmt.Errorf("service %q not found", service)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if replicas < 1 || replicas > 20 {
		return State{}, fmt.Errorf("replicas must be 1..20")
	}
	c.state.Replicas = replicas
	c.state.ReadyReplicas = replicas
	return c.state, nil
}

// Exec implements server.exec.
func (c *Cluster) Exec(command string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state.ExecCalls++
	return fmt.Sprintf("[exec] %d: %s -> exit 0", c.state.ExecCalls, command), nil
}

// -- webhook server ----------------------------------------------------------

// Server serves the tool contract over HTTP(S).
type Server struct {
	cluster *Cluster
}

// NewServer builds the webhook server over the given cluster.
func NewServer(cluster *Cluster) *Server {
	return &Server{cluster: cluster}
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writeError(writer, http.StatusMethodNotAllowed, "POST only")
		return
	}
	var payload struct {
		Action   string          `json:"action"`
		Resource string          `json:"resource"`
		Args     json.RawMessage `json:"args"`
	}
	if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid tool request: "+err.Error())
		return
	}
	verb := payload.Action
	if verb == "invoke" {
		switch {
		case strings.HasPrefix(payload.Resource, "k8s:get"):
			verb = "k8s-get"
		case strings.HasPrefix(payload.Resource, "k8s:logs"):
			verb = "k8s-logs"
		case strings.HasPrefix(payload.Resource, "k8s:restart"):
			verb = "k8s-restart"
		case strings.HasPrefix(payload.Resource, "k8s:scale"):
			verb = "k8s-scale"
		case strings.HasPrefix(payload.Resource, "server:exec"):
			verb = "server-exec"
		case strings.HasPrefix(payload.Resource, "hello:echo"):
			verb = "hello-echo"
		default:
			writeError(writer, http.StatusBadRequest, "unknown resource: "+payload.Resource)
			return
		}
	}
	switch verb {
	case "k8s-get":
		s.handleGet(writer, payload.Args)
	case "k8s-logs":
		s.handleLogs(writer, payload.Args)
	case "k8s-restart":
		s.handleRestart(writer, payload.Args)
	case "k8s-scale":
		s.handleScale(writer, payload.Args)
	case "server-exec":
		s.handleExec(writer, payload.Args)
	case "hello-echo":
		s.handleEcho(writer, payload.Args)
	default:
		writeError(writer, http.StatusBadRequest, "unknown action: "+payload.Action)
	}
}

func (s *Server) handleGet(writer http.ResponseWriter, raw json.RawMessage) {
	var args struct {
		Namespace string `json:"namespace"`
		Service   string `json:"service"`
	}
	if err := json.Unmarshal(raw, &args); err != nil || args.Service == "" {
		writeError(writer, http.StatusBadRequest, "service is required")
		return
	}
	state, err := s.cluster.Get(args.Service)
	if err != nil {
		writeError(writer, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"namespace": args.Namespace, "state": state,
	})
}

func (s *Server) handleLogs(writer http.ResponseWriter, raw json.RawMessage) {
	var args struct {
		Namespace string `json:"namespace"`
		Service   string `json:"service"`
		Lines     int    `json:"lines,omitempty"`
	}
	if err := json.Unmarshal(raw, &args); err != nil || args.Service == "" {
		writeError(writer, http.StatusBadRequest, "service is required")
		return
	}
	logs, err := s.cluster.Logs(args.Service, args.Lines)
	if err != nil {
		writeError(writer, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"namespace": args.Namespace, "service": args.Service, "lines": logs,
	})
}

func (s *Server) handleRestart(writer http.ResponseWriter, raw json.RawMessage) {
	var args struct {
		Namespace string `json:"namespace"`
		Service   string `json:"service"`
	}
	if err := json.Unmarshal(raw, &args); err != nil || args.Service == "" {
		writeError(writer, http.StatusBadRequest, "service is required")
		return
	}
	state, err := s.cluster.Restart(args.Service)
	if err != nil {
		writeError(writer, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"namespace": args.Namespace, "state": state, "applied": true, "sideEffect": "restart",
	})
}

func (s *Server) handleScale(writer http.ResponseWriter, raw json.RawMessage) {
	var args struct {
		Namespace string `json:"namespace"`
		Service   string `json:"service"`
		Replicas  int    `json:"replicas"`
	}
	if err := json.Unmarshal(raw, &args); err != nil || args.Service == "" {
		writeError(writer, http.StatusBadRequest, "service is required")
		return
	}
	state, err := s.cluster.Scale(args.Service, args.Replicas)
	if err != nil {
		writeError(writer, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"namespace": args.Namespace, "state": state, "applied": true, "sideEffect": "scale",
	})
}

func (s *Server) handleEcho(writer http.ResponseWriter, raw json.RawMessage) {
	var args struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &args); err != nil || args.Message == "" {
		writeError(writer, http.StatusBadRequest, "message is required")
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"echo": args.Message, "tool": "hello.echo"})
}

func (s *Server) handleExec(writer http.ResponseWriter, raw json.RawMessage) {
	var args struct {
		Command string `json:"command"`
	}
	if err := json.Unmarshal(raw, &args); err != nil || strings.TrimSpace(args.Command) == "" {
		writeError(writer, http.StatusBadRequest, "command is required")
		return
	}
	output, err := s.cluster.Exec(args.Command)
	if err != nil {
		writeError(writer, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"output": output, "exitCode": 0, "sideEffect": "exec",
	})
}

func writeJSON(writer http.ResponseWriter, status int, body any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(body)
}

func writeError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, map[string]any{"error": message})
}

// SelfSignedTLSListener mirrors the research webtools helper: serves the
// handler on a loopback HTTPS listener trusted only by the returned client.
func SelfSignedTLSListener(handler http.Handler) (net.Listener, *http.Client, string, error) {
	return tlsListener(handler)
}
