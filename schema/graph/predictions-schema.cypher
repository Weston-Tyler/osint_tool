// WorldFish predictive events — Memgraph graph schema.
// Consumed from mda.predictions.worldfish by processors/graph/graph_consumer.py.
// See worldfish/PREDICTION_CONTRACT.md for the message contract (v1.0).
//
// A :PredictedEvent is a forecast, distinct from a realized :Event. It links back
// to its trigger event via (:PredictedEvent)-[:PREDICTED_FROM]->(:Event) and
// carries confidence, timeframe, and the causal chain that produced it.

CREATE INDEX ON :PredictedEvent(prediction_id);
CREATE INDEX ON :PredictedEvent(domain);
CREATE INDEX ON :PredictedEvent(predicted_event_type);
CREATE INDEX ON :PredictedEvent(simulation_run_id);
