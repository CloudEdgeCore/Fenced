package ipc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// AuditRecord logs security and lifecycle events for IPC operations without payload.
type AuditRecord struct {
	Timestamp   time.Time   `json:"timestamp"`
	MessageID   string      `json:"message_id"`
	TenantID    string      `json:"tenant_id"`
	Namespace   string      `json:"namespace"`
	Sender      string      `json:"sender"`
	Receiver    string      `json:"receiver"`
	MessageType MessageType `json:"message_type"`
	Action      string      `json:"action"` // "send", "receive", "ack", "request", "reply", "signal"
	Result      string      `json:"result"` // "success", "denied", "expired", "failed"
	ReasonCode  string      `json:"reason_code,omitempty"`
	Reason      string      `json:"reason,omitempty"`
	TraceID     string      `json:"trace_id,omitempty"`
}

// Auditor defines the sink for IPC audit logging.
type Auditor interface {
	LogAudit(ctx context.Context, record AuditRecord)
}

// MemoryAuditor is an in-memory thread-safe auditor suitable for testing and verification.
type MemoryAuditor struct {
	mu      sync.RWMutex
	records []AuditRecord
}

// NewMemoryAuditor creates a new MemoryAuditor.
func NewMemoryAuditor() *MemoryAuditor {
	return &MemoryAuditor{
		records: make([]AuditRecord, 0),
	}
}

// LogAudit appends an audit record.
func (a *MemoryAuditor) LogAudit(ctx context.Context, record AuditRecord) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.records = append(a.records, record)
}

// Records returns a slice of all recorded audit records.
func (a *MemoryAuditor) Records() []AuditRecord {
	a.mu.RLock()
	defer a.mu.RUnlock()
	copied := make([]AuditRecord, len(a.records))
	copy(copied, a.records)
	return copied
}

// Count returns the number of audit records logged.
func (a *MemoryAuditor) Count() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.records)
}

// Metrics captures telemetry counters and durations for IPC operations.
type Metrics interface {
	RecordSent(ctx context.Context, tenantID string, msgType MessageType)
	RecordReceived(ctx context.Context, tenantID string, msgType MessageType)
	RecordAcked(ctx context.Context, tenantID string, count int)
	RecordExpired(ctx context.Context, tenantID string)
	RecordDenied(ctx context.Context, tenantID string, reason string)
	RecordDeliveryDuration(ctx context.Context, tenantID string, duration time.Duration)
}

// StandardMetrics provides in-memory thread-safe metric tracking and OTel instrument integration.
type StandardMetrics struct {
	sentRequest atomic.Int64
	sentReply   atomic.Int64
	sentSignal  atomic.Int64

	receivedRequest atomic.Int64
	receivedReply   atomic.Int64
	receivedSignal  atomic.Int64

	acked   atomic.Int64
	expired atomic.Int64
	denied  atomic.Int64

	// Optional OTel instruments
	otelSentCounter     metric.Int64Counter
	otelReceivedCounter metric.Int64Counter
	otelAckedCounter    metric.Int64Counter
	otelExpiredCounter  metric.Int64Counter
	otelDeniedCounter   metric.Int64Counter
	otelDurationHist    metric.Float64Histogram
}

// NewStandardMetrics creates a new StandardMetrics collector.
func NewStandardMetrics() *StandardMetrics {
	m := &StandardMetrics{}
	meter := otel.Meter("fenced.kernel.ipc")

	m.otelSentCounter, _ = meter.Int64Counter("ipc_message_sent_total")
	m.otelReceivedCounter, _ = meter.Int64Counter("ipc_message_received_total")
	m.otelAckedCounter, _ = meter.Int64Counter("ipc_message_acked_total")
	m.otelExpiredCounter, _ = meter.Int64Counter("ipc_message_expired_total")
	m.otelDeniedCounter, _ = meter.Int64Counter("ipc_message_denied_total")
	m.otelDurationHist, _ = meter.Float64Histogram("ipc_delivery_duration_seconds")

	return m
}

func (m *StandardMetrics) RecordSent(ctx context.Context, tenantID string, msgType MessageType) {
	switch msgType {
	case MessageTypeRequest:
		m.sentRequest.Add(1)
	case MessageTypeReply:
		m.sentReply.Add(1)
	case MessageTypeSignal:
		m.sentSignal.Add(1)
	}
	if m.otelSentCounter != nil {
		m.otelSentCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("tenant_id", tenantID),
			attribute.String("message_type", string(msgType)),
		))
	}
}

func (m *StandardMetrics) RecordReceived(ctx context.Context, tenantID string, msgType MessageType) {
	switch msgType {
	case MessageTypeRequest:
		m.receivedRequest.Add(1)
	case MessageTypeReply:
		m.receivedReply.Add(1)
	case MessageTypeSignal:
		m.receivedSignal.Add(1)
	}
	if m.otelReceivedCounter != nil {
		m.otelReceivedCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("tenant_id", tenantID),
			attribute.String("message_type", string(msgType)),
		))
	}
}

func (m *StandardMetrics) RecordAcked(ctx context.Context, tenantID string, count int) {
	m.acked.Add(int64(count))
	if m.otelAckedCounter != nil {
		m.otelAckedCounter.Add(ctx, int64(count), metric.WithAttributes(
			attribute.String("tenant_id", tenantID),
		))
	}
}

func (m *StandardMetrics) RecordExpired(ctx context.Context, tenantID string) {
	m.expired.Add(1)
	if m.otelExpiredCounter != nil {
		m.otelExpiredCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("tenant_id", tenantID),
		))
	}
}

func (m *StandardMetrics) RecordDenied(ctx context.Context, tenantID string, reason string) {
	m.denied.Add(1)
	if m.otelDeniedCounter != nil {
		m.otelDeniedCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("tenant_id", tenantID),
			attribute.String("reason", reason),
		))
	}
}

func (m *StandardMetrics) RecordDeliveryDuration(ctx context.Context, tenantID string, duration time.Duration) {
	if m.otelDurationHist != nil {
		m.otelDurationHist.Record(ctx, duration.Seconds(), metric.WithAttributes(
			attribute.String("tenant_id", tenantID),
		))
	}
}

// Counts for testing assertions
func (m *StandardMetrics) SentCount(msgType MessageType) int64 {
	switch msgType {
	case MessageTypeRequest:
		return m.sentRequest.Load()
	case MessageTypeReply:
		return m.sentReply.Load()
	case MessageTypeSignal:
		return m.sentSignal.Load()
	default:
		return m.sentRequest.Load() + m.sentReply.Load() + m.sentSignal.Load()
	}
}

func (m *StandardMetrics) ReceivedCount(msgType MessageType) int64 {
	switch msgType {
	case MessageTypeRequest:
		return m.receivedRequest.Load()
	case MessageTypeReply:
		return m.receivedReply.Load()
	case MessageTypeSignal:
		return m.receivedSignal.Load()
	default:
		return m.receivedRequest.Load() + m.receivedReply.Load() + m.receivedSignal.Load()
	}
}

func (m *StandardMetrics) AckedCount() int64 {
	return m.acked.Load()
}

func (m *StandardMetrics) ExpiredCount() int64 {
	return m.expired.Load()
}

func (m *StandardMetrics) DeniedCount() int64 {
	return m.denied.Load()
}

// Service is the unified kernel-level Agent IPC service.
type Service struct {
	mailbox Mailbox
	policy  Policy
	auditor Auditor
	metrics Metrics
	tracer  trace.Tracer

	mu           sync.RWMutex
	replyWaiters map[string]chan *AgentMessage // correlationID -> chan
}

// ServiceConfig defines options when constructing an IPC Service.
type ServiceConfig struct {
	Mailbox Mailbox
	Policy  Policy
	Auditor Auditor
	Metrics Metrics
	Tracer  trace.Tracer
}

// NewService constructs an IPC Service.
func NewService(cfg ServiceConfig) *Service {
	if cfg.Mailbox == nil {
		cfg.Mailbox = NewDurableMemoryMailbox()
	}
	if cfg.Policy == nil {
		cfg.Policy = NewDefaultPolicy(nil, nil)
	}
	if cfg.Auditor == nil {
		cfg.Auditor = NewMemoryAuditor()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = NewStandardMetrics()
	}
	if cfg.Tracer == nil {
		cfg.Tracer = otel.Tracer("fenced/kernel/ipc")
	}

	return &Service{
		mailbox:      cfg.Mailbox,
		policy:       cfg.Policy,
		auditor:      cfg.Auditor,
		metrics:      cfg.Metrics,
		tracer:       cfg.Tracer,
		replyWaiters: make(map[string]chan *AgentMessage),
	}
}

func (s *Service) registerReplyWaiter(correlationID string) chan *AgentMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := make(chan *AgentMessage, 1)
	s.replyWaiters[correlationID] = ch
	return ch
}

func (s *Service) unregisterReplyWaiter(correlationID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.replyWaiters, correlationID)
}

func (s *Service) notifyReplyWaiter(msg *AgentMessage) {
	if msg == nil || msg.CorrelationID == "" {
		return
	}
	s.mu.RLock()
	ch, ok := s.replyWaiters[msg.CorrelationID]
	s.mu.RUnlock()
	if ok {
		select {
		case ch <- msg.Clone():
		default:
		}
	}
}

// Send validates, authorizes, audits, and persists an IPC message.
func (s *Service) Send(ctx context.Context, msg *AgentMessage) error {
	if msg == nil {
		return fmt.Errorf("%w: nil message", ErrInvalidMessage)
	}

	ctx, span := s.tracer.Start(ctx, "ipc.send",
		trace.WithAttributes(
			attribute.String("ipc.message_type", string(msg.Type)),
			attribute.String("ipc.sender", msg.Sender.String()),
			attribute.String("ipc.receiver", msg.Receiver.String()),
		),
	)
	defer span.End()

	if err := msg.Validate(); err != nil {
		if errors.Is(err, ErrCrossTenantDenied) {
			s.metrics.RecordDenied(ctx, msg.TenantID, err.Error())
			s.logAudit(ctx, msg, "send", "denied", err.Error())
		} else {
			s.logAudit(ctx, msg, "send", "failed", err.Error())
		}
		return err
	}

	// Set TraceID if unset
	if msg.TraceID == "" && span.SpanContext().HasTraceID() {
		msg.TraceID = span.SpanContext().TraceID().String()
	}

	// Policy authorization
	if decisionPolicy, ok := s.policy.(interface {
		AuthorizeSendWithDecision(ctx context.Context, msg *AgentMessage) (*AuthorizationDecision, error)
	}); ok {
		decision, err := decisionPolicy.AuthorizeSendWithDecision(ctx, msg)
		if err != nil {
			reasonCode := "DENIED"
			if decision != nil && decision.ReasonCode != "" {
				reasonCode = decision.ReasonCode
			}
			s.metrics.RecordDenied(ctx, msg.TenantID, reasonCode)
			s.logAuditWithDecision(ctx, msg, "send", "denied", decision)
			return err
		}
	} else if err := s.policy.AuthorizeSend(ctx, msg); err != nil {
		s.metrics.RecordDenied(ctx, msg.TenantID, err.Error())
		s.logAudit(ctx, msg, "send", "denied", err.Error())
		return err
	}

	// Persist into mailbox
	if err := s.mailbox.Send(ctx, msg); err != nil {
		if errors.Is(err, ErrMessageExpired) {
			s.metrics.RecordExpired(ctx, msg.TenantID)
			s.logAudit(ctx, msg, "send", "expired", err.Error())
		} else {
			s.logAudit(ctx, msg, "send", "failed", err.Error())
		}
		return err
	}

	s.metrics.RecordSent(ctx, msg.TenantID, msg.Type)
	s.logAudit(ctx, msg, "send", "success", "")

	// Event-driven reactive notification for local reply waiters
	if msg.Type == MessageTypeReply {
		s.notifyReplyWaiter(msg)
	}

	return nil
}

// Receive retrieves pending messages for receiver.
func (s *Service) Receive(ctx context.Context, receiver AgentAddress, limit int) ([]*AgentMessage, error) {
	ctx, span := s.tracer.Start(ctx, "ipc.receive",
		trace.WithAttributes(
			attribute.String("ipc.receiver", receiver.String()),
		),
	)
	defer span.End()

	if err := receiver.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidAddress, err)
	}

	if err := s.policy.AuthorizeReceive(ctx, receiver); err != nil {
		s.metrics.RecordDenied(ctx, receiver.TenantID, err.Error())
		s.auditor.LogAudit(ctx, AuditRecord{
			Timestamp: time.Now().UTC(),
			TenantID:  receiver.TenantID,
			Namespace: receiver.EffectiveNamespace(),
			Receiver:  receiver.String(),
			Action:    "receive",
			Result:    "denied",
			Reason:    err.Error(),
		})
		return nil, err
	}

	msgs, err := s.mailbox.Receive(ctx, receiver, limit)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	for _, m := range msgs {
		s.metrics.RecordReceived(ctx, m.TenantID, m.Type)
		s.metrics.RecordDeliveryDuration(ctx, m.TenantID, now.Sub(m.CreatedAt))
		s.logAudit(ctx, m, "receive", "success", "")
	}

	return msgs, nil
}

// Ack acknowledges received messages.
func (s *Service) Ack(ctx context.Context, receiver AgentAddress, messageIDs []string) error {
	ctx, span := s.tracer.Start(ctx, "ipc.ack",
		trace.WithAttributes(
			attribute.String("ipc.receiver", receiver.String()),
			attribute.Int("ipc.ack_count", len(messageIDs)),
		),
	)
	defer span.End()

	if err := receiver.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAddress, err)
	}

	if err := s.policy.AuthorizeReceive(ctx, receiver); err != nil {
		s.metrics.RecordDenied(ctx, receiver.TenantID, err.Error())
		s.auditor.LogAudit(ctx, AuditRecord{
			Timestamp: time.Now().UTC(),
			TenantID:  receiver.TenantID,
			Namespace: receiver.EffectiveNamespace(),
			Receiver:  receiver.String(),
			Action:    "ack",
			Result:    "denied",
			Reason:    err.Error(),
		})
		return err
	}

	if err := s.mailbox.Ack(ctx, receiver, messageIDs); err != nil {
		return err
	}

	s.metrics.RecordAcked(ctx, receiver.TenantID, len(messageIDs))
	s.auditor.LogAudit(ctx, AuditRecord{
		Timestamp: time.Now().UTC(),
		TenantID:  receiver.TenantID,
		Namespace: receiver.EffectiveNamespace(),
		Receiver:  receiver.String(),
		Action:    "ack",
		Result:    "success",
	})

	return nil
}

// SendRequest sends a request and synchronously awaits the matching reply or timeout.
func (s *Service) SendRequest(ctx context.Context, req *AgentMessage, timeout time.Duration) (*AgentMessage, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: nil request message", ErrInvalidMessage)
	}
	if req.CorrelationID == "" {
		req.CorrelationID = req.ID
	}
	req.Type = MessageTypeRequest

	waiter := s.registerReplyWaiter(req.CorrelationID)
	defer s.unregisterReplyWaiter(req.CorrelationID)

	if err := s.Send(ctx, req); err != nil {
		return nil, err
	}

	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, ErrTimeout
	case reply := <-waiter:
		return reply, nil
	}
}

// SendReply validates and sends a reply message to an existing request.
func (s *Service) SendReply(ctx context.Context, reply *AgentMessage) error {
	if reply == nil {
		return fmt.Errorf("%w: nil reply message", ErrInvalidMessage)
	}
	if reply.Type != MessageTypeReply {
		return fmt.Errorf("%w: expected reply message type, got %s", ErrInvalidReply, reply.Type)
	}
	if reply.CorrelationID == "" {
		return fmt.Errorf("%w: reply must specify correlation_id", ErrInvalidReply)
	}
	return s.Send(ctx, reply)
}

// SendSignal dispatches a lifecycle or control signal to a target agent or instance.
func (s *Service) SendSignal(ctx context.Context, sender, receiver AgentAddress, sigType SignalType, reason string, metadata map[string]string) (*AgentMessage, error) {
	msg, err := NewSignal(sender, receiver, sigType, reason, metadata, 5*time.Minute)
	if err != nil {
		return nil, err
	}
	if err := s.Send(ctx, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (s *Service) logAudit(ctx context.Context, msg *AgentMessage, action, result, reason string) {
	if s.auditor == nil || msg == nil {
		return
	}
	s.auditor.LogAudit(ctx, AuditRecord{
		Timestamp:   time.Now().UTC(),
		MessageID:   msg.ID,
		TenantID:    msg.TenantID,
		Namespace:   msg.Namespace,
		Sender:      msg.Sender.String(),
		Receiver:    msg.Receiver.String(),
		MessageType: msg.Type,
		Action:      action,
		Result:      result,
		Reason:      reason,
		TraceID:     msg.TraceID,
		// Explicit: NO PAYLOAD IS LOGGED.
	})
}

func (s *Service) logAuditWithDecision(ctx context.Context, msg *AgentMessage, action, result string, dec *AuthorizationDecision) {
	if s.auditor == nil || msg == nil {
		return
	}
	reasonCode := ""
	reason := ""
	traceID := msg.TraceID
	if dec != nil {
		reasonCode = dec.ReasonCode
		reason = dec.Reason
		if traceID == "" {
			traceID = dec.TraceID
		}
	}
	s.auditor.LogAudit(ctx, AuditRecord{
		Timestamp:   time.Now().UTC(),
		MessageID:   msg.ID,
		TenantID:    msg.TenantID,
		Namespace:   msg.Namespace,
		Sender:      msg.Sender.String(),
		Receiver:    msg.Receiver.String(),
		MessageType: msg.Type,
		Action:      action,
		Result:      result,
		ReasonCode:  reasonCode,
		Reason:      reason,
		TraceID:     traceID,
		// Explicit: NO PAYLOAD IS LOGGED.
	})
}
