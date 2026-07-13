"""Command-line entrypoint for WorldFish.

Run a predictive-event simulation and print / publish the resulting predictions::

    python -m worldfish                        # offline demo seed, deterministic, dry-run
    python -m worldfish --steps 60 --agents 40
    python -m worldfish --publish              # emit to Kafka (mda.predictions.worldfish)
    python -m worldfish --from-memgraph EVT    # seed from a live Memgraph trigger event
    python -m worldfish --llm                  # drive agent decisions with Ollama

By default it runs fully offline (synthetic demo seed, deterministic rule-based
agents, no Kafka), so it works without Memgraph, Ollama, or a broker.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from .publisher import PredictionPublisher
from .seed_extractor import build_demo_seed
from .simulation_engine import DEFAULT_STEPS, WorldFishSimulation, build_ollama_policy


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="worldfish", description="WorldFish predictive-event simulation")
    parser.add_argument("--agents", type=int, default=None, help="number of agents (default: WORLDFISH_DEFAULT_N_AGENTS)")
    parser.add_argument("--steps", type=int, default=DEFAULT_STEPS, help="simulation steps")
    parser.add_argument("--rng-seed", type=int, default=0, help="seed for reproducible offline runs")
    parser.add_argument("--domain", default="maritime", choices=["maritime", "territorial"])
    parser.add_argument("--from-memgraph", metavar="TRIGGER_ID", default=None, help="seed from a live Memgraph trigger event")
    parser.add_argument("--llm", action="store_true", help="use Ollama for agent decisions")
    parser.add_argument("--publish", action="store_true", help="publish predictions to Kafka (default: dry-run)")
    parser.add_argument("--json", action="store_true", help="also print the full prediction envelopes as JSON")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    if args.from_memgraph:
        from .seed_extractor import OBISeedExtractor

        seed = OBISeedExtractor().build_seed(args.from_memgraph)
    else:
        seed = build_demo_seed(domain=args.domain)

    policy = build_ollama_policy() if args.llm else None
    sim = WorldFishSimulation(seed, n_agents=args.agents, rng_seed=args.rng_seed, decision_policy=policy)
    result = sim.run(n_steps=args.steps)

    publisher = PredictionPublisher(dry_run=not args.publish)
    envelopes = publisher.publish(result.predictions)
    publisher.close()

    tail = "" if args.publish else " (dry-run; pass --publish to emit to Kafka)"
    print(
        f"WorldFish run {result.simulation_id[:8]} | domain={result.domain} | "
        f"agents={result.n_agents} | steps={result.n_steps_completed} | "
        f"predictions={len(result.predictions)}{tail}"
    )
    for pred in result.predictions:
        print(
            f"  - [{pred.confidence_label:>9}] {pred.predicted_event_type} "
            f"in ~{pred.predicted_timeframe_median_days}d @ "
            f"{pred.predicted_location_region or 'n/a'} (conf {pred.confidence:.2f})"
        )
    if args.json:
        print(json.dumps(envelopes, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
