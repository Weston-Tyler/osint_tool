// Reconciliation of predicted vs realized events.
// A realized :Event that fulfils a :PredictedEvent is linked
// (:PredictedEvent)-[:REALIZED_BY]->(:Event) with distance_km, lead_time_days,
// and confidence_at_prediction on the relationship. See
// processors/graph/reconciliation.py. Realized interdictions arrive on
// mda.interdictions.new and are upserted as :Event nodes.

CREATE INDEX ON :Event(event_id);
CREATE INDEX ON :PredictedEvent(realized);
