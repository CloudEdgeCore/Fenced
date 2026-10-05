package tokens

import (
	"strings"
	"testing"
)

// corpusEntry is one reservation-envelope regression sample. referenceTokens
// is a documented LOWER bound on the true token count a real tokenizer
// produces for the text (the well-known tiktoken cl100k_base / o200k_base
// counts for the anchor strings, and a conservative per-category floor for
// the generated ones). The envelope contract (see the package doc) requires
// every estimator to reserve at or above the true count, so the estimate
// must never drop below this lower bound.
type corpusEntry struct {
	name            string
	text            string
	referenceTokens int64
}

// P1-06 conservative reserve envelope corpus. The categories are exactly the
// ones the remediation acceptance names: English, Chinese, JSON, tool-call
// arguments, reasoning-style prose, multilingual, and an over-long prompt.
// Fenced deliberately ships a conservative envelope rather than an exact
// in-process tokenizer (tiktoken/SentencePiece/HF) — an exact tokenizer is a
// future per-provider plug-in resolved by Executor.Tokenizer(); until one is
// registered the reservation must provably over-reserve, and settlement uses
// the provider-reported usage regardless.
func envelopeCorpus() []corpusEntry {
	return []corpusEntry{
		// Anchor: "hello world" is 2 tokens under tiktoken cl100k_base.
		{"english-anchor", "hello world", 2},
		{"english-sentence", "Analyze the current system reliability and propose concrete improvements.", 10},
		{"chinese", "分析当前系统的可靠性与安全性，并给出改进建议。", 12},
		{"japanese", "システムの信頼性を分析し、改善案を提示してください。", 12},
		{"korean", "시스템의 안정성을 분석하고 개선 방안을 제시하세요.", 10},
		{"json", `{"messages":[{"role":"user","content":"summarize the result"}],"max_tokens":1024}`, 20},
		{"tool-call-args", `{"name":"weather.lookup","arguments":{"city":"上海","unit":"celsius"}}`, 16},
		{"reasoning", "First, decompose the goal into subgoals; then, for each subgoal, evaluate the tradeoffs and pick the option with the lowest downside.", 22},
		{"multilingual", "Summary 概要 요약 概要: deploy to region eu-west-1 前请确认预算。", 18},
		{"long-prompt", strings.Repeat("context window stress ", 4096), 4096},
	}
}

// TestReservationEnvelopeNeverUnderReserves proves the estimators reserve at
// or above the documented true-token lower bound across every corpus
// category. Under-reservation is the one dangerous direction: it lets
// concurrent calls collectively promise past a budget before settlement.
func TestReservationEnvelopeNeverUnderReserves(t *testing.T) {
	for _, entry := range envelopeCorpus() {
		heuristic := Heuristic(entry.text)
		if heuristic < entry.referenceTokens {
			t.Errorf("%s: heuristic reserved %d, below the true-count lower bound %d", entry.name, heuristic, entry.referenceTokens)
		}
		if conservative := Conservative(entry.text); conservative < entry.referenceTokens {
			t.Errorf("%s: conservative reserved %d, below the true-count lower bound %d", entry.name, conservative, entry.referenceTokens)
		}
		// The floor invariant holds independently of the category.
		if legacy := legacyBytesPerFour(entry.text); heuristic < legacy {
			t.Errorf("%s: heuristic %d dropped below the bytes/4 floor %d", entry.name, heuristic, legacy)
		}
	}
}

// TestConservativeEnvelopeDominatesHeuristic proves the conservative envelope
// is a true superset of the heuristic across the corpus, so selecting the
// conservative tokenizer for an uncharacterized provider can only widen the
// reservation, never shrink it.
func TestConservativeEnvelopeDominatesHeuristic(t *testing.T) {
	for _, entry := range envelopeCorpus() {
		if Conservative(entry.text) < Heuristic(entry.text) {
			t.Errorf("%s: conservative %d below heuristic %d", entry.name, Conservative(entry.text), Heuristic(entry.text))
		}
	}
}

// TestLongPromptScalesLinearly proves the envelope keeps covering an
// over-long prompt: doubling the input never more than doubles the estimate
// (no overflow or truncation) and always grows it (no silent cap).
func TestLongPromptScalesLinearly(t *testing.T) {
	unit := strings.Repeat("token budget corpus ", 1024)
	single := Heuristic(unit)
	double := Heuristic(unit + unit)
	if double <= single {
		t.Fatalf("doubled prompt estimate %d did not grow beyond %d", double, single)
	}
	if double > 2*single+2 {
		t.Fatalf("doubled prompt estimate %d exceeds twice %d (non-linear growth)", double, single)
	}
}
