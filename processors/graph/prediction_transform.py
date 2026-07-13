"""Pure transform: a WorldFish prediction envelope -> Memgraph Cypher.

No Memgraph / Kafka / DB imports, so this module is importable and unit-testable
anywhere. ``graph_consumer`` calls these helpers to upsert a :PredictedEvent node
from an ``mda.predictions.worldfish`` message (contract v1.0 — see
``worldfish/PREDICTION_CONTRACT.md``). Invalid envelopes raise
:class:`PredictionContractError`, which the consumer routes to the DLQ.
"""

from __future__ import annotations

SCHEMA = "mda.prediction"
SUPPORTED_MAJOR = "1"
NODE_LABEL = "PredictedEvent"


class PredictionContractError(ValueError):
    """Envelope does not satisfy the v1 prediction contract (-> DLQ)."""


def validate_envelope(env: dict) -> None:
    """Raise :class:`PredictionContractError` unless ``env`` is a v1 prediction."""
    if not isinstance(env, dict):
        raise PredictionContractError("envelope is not an object")
    if env.get("schema") != SCHEMA:
        raise PredictionContractError(f"unexpected schema {env.get('schema')!r}")
    major = str(env.get("schema_version", "")).split(".")[0]
    if major != SUPPORTED_MAJOR:
        raise PredictionContractError(f"unsupported schema_version {env.get('schema_version')!r}")
    if not env.get("prediction_id"):
        raise PredictionContractError("missing prediction_id")


def _inner(env: dict) -> dict:
    """The CausalPrediction body: env['payload']['payload']."""
    return (env.get("payload") or {}).get("payload") or {}


def build_predicted_event_cypher(env: dict) -> tuple[str, dict]:
    """Return ``(cypher, params)`` that upserts the :PredictedEvent node.

    Validates the envelope first; unknown minor versions are accepted
    (forward-compatible), unknown major versions raise.
    """
    validate_envelope(env)
    inner = _inner(env)
    loc = inner.get("location") or {}
    tf = inner.get("timeframe") or {}
    chain = inner.get("causal_chain") or {}
    params = {
        "pid": env["prediction_id"],
        "etype": env.get("predicted_event_type") or inner.get("predicted_event_type", "UNKNOWN"),
        "desc": inner.get("description", ""),
        "conf": float(env.get("confidence", 0.0) or 0.0),
        "conf_label": env.get("confidence_label", ""),
        "domain": env.get("domain", ""),
        "sim_run": env.get("simulation_run_id", ""),
        "lat": loc.get("lat"),
        "lon": loc.get("lon"),
        "region": loc.get("region", ""),
        "tmin": int(tf.get("min_days", 0) or 0),
        "tmax": int(tf.get("max_days", 0) or 0),
        "chain_path": [str(x) for x in (chain.get("path") or [])],
        "chain_summary": chain.get("summary", ""),
        "sver": env.get("schema_version", ""),
    }
    cypher = f"""
    MERGE (p:{NODE_LABEL} {{prediction_id: $pid}})
    ON CREATE SET p.system_created_at = localDateTime(),
                  p.valid_from = localDateTime()
    SET p.predicted_event_type = $etype,
        p.description = $desc,
        p.confidence = $conf,
        p.confidence_label = $conf_label,
        p.domain = $domain,
        p.simulation_run_id = $sim_run,
        p.predicted_lat = $lat,
        p.predicted_lon = $lon,
        p.predicted_region = $region,
        p.timeframe_min_days = $tmin,
        p.timeframe_max_days = $tmax,
        p.causal_chain_path = $chain_path,
        p.causal_chain_summary = $chain_summary,
        p.schema_version = $sver,
        p.source_ids = ['worldfish'],
        p.system_updated_at = localDateTime()
    """
    return cypher, params


def build_trigger_link_cypher(env: dict) -> tuple[str, dict] | None:
    """Return ``(cypher, params)`` linking the prediction to its trigger Event.

    Returns ``None`` when the envelope carries no trigger. Uses OPTIONAL MATCH so
    a missing trigger event never drops the prediction node.
    """
    trigger = (_inner(env).get("causal_chain") or {}).get("trigger")
    if not trigger:
        return None
    cypher = f"""
    MATCH (p:{NODE_LABEL} {{prediction_id: $pid}})
    OPTIONAL MATCH (t:Event {{event_id: $trigger}})
    FOREACH (_ IN CASE WHEN t IS NULL THEN [] ELSE [1] END |
        MERGE (p)-[:PREDICTED_FROM]->(t))
    """
    return cypher, {"pid": env["prediction_id"], "trigger": trigger}


def has_location(env: dict) -> bool:
    """True when the prediction carries a usable lat/lon (for the PostGIS row)."""
    loc = _inner(env).get("location") or {}
    return loc.get("lat") is not None and loc.get("lon") is not None
