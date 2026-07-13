# WorldFish predictive-event output contract

**Version:** `1.0` · **Kafka topic:** `mda.predictions.worldfish` · **Producer:** `worldfish`

WorldFish publishes one message per predicted event. Each message is the JSON
envelope produced by `worldfish/contract.py::build_prediction_event`. The
graph-processor consumes this topic (it is the *only* writer to Memgraph/PostGIS —
WorldFish never writes to the stores directly). Consumers must tolerate unknown
keys; additive changes bump the **minor** version, breaking changes the **major**.

## Envelope

| Field | Type | Notes |
|---|---|---|
| `schema` | string | Always `"mda.prediction"`. |
| `schema_version` | string | `"1.0"`. |
| `event_type` | string | Always `"predicted_event"`. |
| `produced_at` | string (ISO-8601 UTC) | When the envelope was built. |
| `producer` | string | Always `"worldfish"`. |
| `prediction_id` | string (uuid) | Stable id for the prediction. |
| `simulation_run_id` | string (uuid) | The simulation that produced it. |
| `domain` | string | `maritime` \| `territorial` \| … |
| `confidence` | float 0–1 | Point confidence at generation time. |
| `confidence_label` | string | `very_low`…`very_high`. |
| `predicted_event_type` | string | e.g. `INTERDICTION_OPERATION`, `MARITIME_VIOLENT_INCIDENT`. |
| `payload` | object | The full assertion (below). |

## `payload` (from `CausalPrediction.to_obi_assertion_dict`)

| Field | Type | Notes |
|---|---|---|
| `assertion_type` | string | `"CausalPrediction"`. |
| `assertion_id` | string | = `prediction_id`. |
| `source` | string | `"worldfish:<simulation_run_id>"`. |
| `confidence` | float 0–1 | |
| `generated_at` | string (ISO-8601) | |
| `payload.predicted_event_type` | string | |
| `payload.description` | string | Human-readable forecast. |
| `payload.location` | object | `{lat: float\|null, lon: float\|null, region: string}`. |
| `payload.timeframe` | object | `{min_days: int, max_days: int}` (also `median` on the object model). |
| `payload.causal_chain` | object | `{trigger: string, path: [string], summary: string}`. |
| `payload.simulation` | object | `{n_agents: int, n_steps: int, domain: string}`. |
| `payload.alternatives` | array | Alternative outcomes: `{description, probability, predicted_event_type, divergence_point}`. |

## Example (abridged)

```json
{
  "schema": "mda.prediction",
  "schema_version": "1.0",
  "event_type": "predicted_event",
  "producer": "worldfish",
  "domain": "maritime",
  "confidence": 0.42,
  "confidence_label": "moderate",
  "predicted_event_type": "INTERDICTION_OPERATION",
  "payload": {
    "assertion_type": "CausalPrediction",
    "payload": {
      "location": {"lat": 12.5, "lon": -90.3, "region": "Eastern Pacific"},
      "timeframe": {"min_days": 40, "max_days": 120},
      "causal_chain": {"trigger": "demo-evt-ais-disable-0001", "path": ["...", "ais_disable"]}
    }
  }
}
```
