"""WorldFish agent memory store — local pgvector replacement for Zep Cloud.

Uses sentence-transformers all-MiniLM-L6-v2 (384 dims) for embeddings
and PostgreSQL pgvector for cosine-similarity retrieval.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import numpy as np

logger = logging.getLogger("mda.worldfish.memory")

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://mda:mda@localhost:5432/mda")
EMBEDDING_DIM = 384
DEFAULT_TOP_K = 5
DECAY_FACTOR = 0.995


@dataclass
class MemoryItem:
    memory_text: str
    importance_score: float = 0.5
    memory_type: str = "episodic"
    embedding: Optional[np.ndarray] = field(default=None, repr=False)


class AgentMemoryStore:
    """Local pgvector agent memory (replaces Zep Cloud)."""

    def __init__(self, dsn: str = POSTGRES_DSN):
        self.dsn = dsn
        self._encoder = None
        self._pool = None

    def _get_encoder(self):
        if self._encoder is None:
            try:
                from sentence_transformers import SentenceTransformer
                self._encoder = SentenceTransformer("all-MiniLM-L6-v2")
            except ImportError:
                logger.warning("sentence-transformers not installed, using random embeddings")
                self._encoder = "fallback"
        return self._encoder

    def encode(self, text: str) -> np.ndarray:
        encoder = self._get_encoder()
        if encoder == "fallback":
            np.random.seed(hash(text) % 2**31)
            return np.random.randn(EMBEDDING_DIM).astype(np.float32)
        return encoder.encode(text, normalize_embeddings=True)

    def add_memory_sync(self, simulation_id: str, agent_id: str, item: MemoryItem,
                        step_number: int = 0) -> None:
        """Synchronous memory insert (for use in Mesa step())."""
        import psycopg2
        if item.embedding is None:
            item.embedding = self.encode(item.memory_text)
        vec_str = "[" + ",".join(str(v) for v in item.embedding.tolist()) + "]"

        conn = psycopg2.connect(self.dsn)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO worldfish_agent_memory
                        (simulation_id, agent_id, memory_text, memory_type,
                         memory_embedding, importance_score, step_number)
                    VALUES (%s, %s, %s, %s, %s::vector, %s, %s)
                """, (simulation_id, agent_id, item.memory_text, item.memory_type,
                      vec_str, item.importance_score, step_number))
            conn.commit()
        finally:
            conn.close()

    def retrieve_relevant_sync(self, simulation_id: str, agent_id: str,
                               query: str, top_k: int = DEFAULT_TOP_K) -> list[str]:
        """Synchronous retrieval for Mesa agents."""
        import psycopg2
        q_vec = self.encode(query)
        vec_str = "[" + ",".join(str(v) for v in q_vec.tolist()) + "]"

        conn = psycopg2.connect(self.dsn)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT memory_text
                    FROM worldfish_agent_memory
                    WHERE simulation_id = %s AND agent_id = %s
                    ORDER BY (0.7 * (1 - (memory_embedding <=> %s::vector))
                            + 0.3 * importance_score) DESC
                    LIMIT %s
                """, (simulation_id, agent_id, vec_str, top_k))
                rows = cur.fetchall()
            return [r[0] for r in rows]
        finally:
            conn.close()

    def apply_decay_sync(self, simulation_id: str) -> int:
        """Apply daily decay to all memories in a simulation."""
        import psycopg2
        conn = psycopg2.connect(self.dsn)
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE worldfish_agent_memory
                    SET importance_score = GREATEST(0.01, importance_score * %s)
                    WHERE simulation_id = %s
                """, (DECAY_FACTOR, simulation_id))
                count = cur.rowcount
            conn.commit()
            return count
        finally:
            conn.close()
