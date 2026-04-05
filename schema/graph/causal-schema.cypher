// ============================================================
// MDA Causal Chain Analysis Engine — Memgraph Schema
// Extends the base MDA schema with causal-specific node types,
// edge types, and indexes for Pearl hierarchy levels 1-3.
// ============================================================

// ── Constraints ─────────────────────────────────────────────

CREATE CONSTRAINT ON (e:CausalEvent) ASSERT e.event_id IS UNIQUE;
CREATE CONSTRAINT ON (c:CausalChain) ASSERT c.chain_id IS UNIQUE;
CREATE CONSTRAINT ON (p:CausalPattern) ASSERT p.pattern_id IS UNIQUE;
CREATE CONSTRAINT ON (s:CausalScenario) ASSERT s.scenario_id IS UNIQUE;
CREATE CONSTRAINT ON (pred:CausalPrediction) ASSERT pred.prediction_id IS UNIQUE;
CREATE CONSTRAINT ON (et:EventTypeTaxonomy) ASSERT et.code IS UNIQUE;
CREATE CONSTRAINT ON (mi:MarketInstrument) ASSERT mi.ticker IS UNIQUE;
CREATE CONSTRAINT ON (cet:CausalEventType) ASSERT cet.event_type IS UNIQUE;

// ── Indexes ─────────────────────────────────────────────────

CREATE INDEX ON :CausalEvent(event_type);
CREATE INDEX ON :CausalEvent(occurred_at);
CREATE INDEX ON :CausalEvent(location_region);
CREATE INDEX ON :CausalEvent(location_country_iso);
CREATE INDEX ON :CausalEvent(confidence);
CREATE INDEX ON :CausalEvent(causal_weight);
CREATE INDEX ON :CausalEvent(domain);
CREATE INDEX ON :CausalEvent(risk_score);
CREATE INDEX ON :CausalEvent(status);

CREATE INDEX ON :CausalChain(chain_type);
CREATE INDEX ON :CausalChain(trigger_event_type);

CREATE INDEX ON :CausalPattern(trigger_event_type);
CREATE INDEX ON :CausalPattern(confidence);
CREATE INDEX ON :CausalPattern(domain);
CREATE INDEX ON :CausalPattern(status);

CREATE INDEX ON :CausalPrediction(predicted_event_type);
CREATE INDEX ON :CausalPrediction(confidence);
CREATE INDEX ON :CausalPrediction(domain);
CREATE INDEX ON :CausalPrediction(predicted_location_region);

CREATE INDEX ON :CausalScenario(scenario_type);
CREATE INDEX ON :CausalScenario(domain);

CREATE INDEX ON :MarketInstrument(instrument_type);

CREATE INDEX ON :EventTypeTaxonomy(domain);
CREATE INDEX ON :EventTypeTaxonomy(tier);
