# WorldFish — predictive-event foundation

WorldFish is an agent-based simulation that projects how a seeded intelligence
scenario evolves and emits **forward-looking predictive events**. It is the
predictive-event foundation of the MDA platform: seeds come from the causal
graph (Memgraph), agents act (rule-based by default, or LLM-driven via Ollama),
and the resulting trajectory is turned into `CausalPrediction`s that are
published to Kafka for the graph-processor to ingest.

## Run it

```bash
python -m worldfish                      # offline demo seed, deterministic, dry-run
python -m worldfish --steps 60 --agents 40
python -m worldfish --json               # also print full prediction envelopes
python -m worldfish --from-memgraph EVT  # seed from a live Memgraph trigger event
python -m worldfish --llm                # drive agent decisions with Ollama
python -m worldfish --publish            # emit to Kafka (mda.predictions.worldfish)
```

By default the run is fully offline and **deterministic** (fixed `--rng-seed`):
synthetic demo seed, rule-based agents, no Kafka — so it works without Memgraph,
Ollama, or a broker. `--publish`, `--llm`, and `--from-memgraph` opt into the
live dependencies.

## Output contract

Predictions are published to **`mda.predictions.worldfish`** using the versioned
envelope documented in [`PREDICTION_CONTRACT.md`](PREDICTION_CONTRACT.md). The
graph-processor is the sole consumer/writer to the stores; WorldFish never writes
to Memgraph/PostGIS directly.

## Configuration (environment variables)

| Variable | Default | Used for |
|---|---|---|
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama host (`--llm` and persona generation) |
| `OLLAMA_LLM_MODEL` | `qwen2.5:7b` | Ollama model (fully configurable, not pinned) |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Broker for `--publish` |
| `WORLDFISH_DEFAULT_N_AGENTS` | `24` | Default agent count |
| `WORLDFISH_DEFAULT_STEPS` | `40` | Default simulation steps |
| `MEMGRAPH_HOST` / `MEMGRAPH_PORT` | `localhost` / `7687` | `--from-memgraph` seed extraction |

## Modules

- `seed_extractor.py` — `OBISeedExtractor` (Memgraph → `SimulationSeed`) and `build_demo_seed` (offline).
- `persona_generator.py` — LLM personas (`OBIAgentPersonaGenerator`) and deterministic `build_synthetic_personas`.
- `simulation_engine.py` — the model, the internal scheduler, and the pluggable decision policies.
- `prediction.py` — `CausalPrediction` and the deterministic `generate_predictions`.
- `contract.py` / `publisher.py` — the Kafka envelope and publisher.
- `memory.py` — pgvector-backed long-term agent memory (optional; step-level memory is a bounded deque).

## Status

The offline pipeline (seed → personas → simulation → predictions → envelope) is
complete and tested. Wiring the graph-processor consumer for
`mda.predictions.worldfish` is a follow-up (see capsule `202607-worldfish-buildout`).
