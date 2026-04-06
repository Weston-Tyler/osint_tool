"""Playwright-based multi-jurisdiction business registry collector.

Base class + Panama Registro Público implementation.
Uses headless Chromium for registries that don't have bulk APIs.
"""

import asyncio
import hashlib
import json
import logging
import os
import re
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional

logger = logging.getLogger("mda.registry_collector")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

MONTHS_ES = {
    "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
    "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
    "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
}


@dataclass
class CompanyRecord:
    registry_key: str
    registration_number: str
    name: str
    jurisdiction_code: str
    company_type: Optional[str] = None
    status: Optional[str] = None
    registration_date: Optional[str] = None
    dissolution_date: Optional[str] = None
    registered_address: Optional[str] = None
    directors: List[Dict] = field(default_factory=list)
    shareholders: List[Dict] = field(default_factory=list)
    agent: Optional[str] = None
    collection_timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict:
        return asdict(self)


class BaseRegistryCollector(ABC):
    """Abstract base for jurisdiction-specific registry collectors."""

    REGISTRY_KEY: str = "base"
    JURISDICTION_CODE: str = "XX"
    BASE_URL: str = ""

    def __init__(self, username: Optional[str] = None, password: Optional[str] = None,
                 headless: bool = True, rate_limit: float = 2.0):
        self.username = username
        self.password = password
        self.headless = headless
        self.rate_limit = rate_limit
        self._browser = None
        self._page = None

    async def __aenter__(self):
        from playwright.async_api import async_playwright
        self._pw = await async_playwright().__aenter__()
        self._browser = await self._pw.chromium.launch(
            headless=self.headless,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx = await self._browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            viewport={"width": 1280, "height": 720},
        )
        self._page = await ctx.new_page()
        return self

    async def __aexit__(self, *args):
        if self._browser:
            await self._browser.close()

    @abstractmethod
    async def login(self) -> bool:
        pass

    @abstractmethod
    async def search_by_name(self, name: str) -> List[Dict]:
        pass

    @abstractmethod
    async def search_by_number(self, reg_number: str) -> Optional[CompanyRecord]:
        pass

    async def collect_batch(self, queries: List[str]) -> List[CompanyRecord]:
        results = []
        for q in queries:
            await asyncio.sleep(self.rate_limit)
            try:
                matches = await self.search_by_name(q)
                for m in matches[:3]:
                    if "registration_number" in m:
                        record = await self.search_by_number(m["registration_number"])
                        if record:
                            results.append(record)
            except Exception as e:
                logger.warning("Collect error for '%s': %s", q, e)
        return results


class PanamaRegistryCollector(BaseRegistryCollector):
    """Panama Registro Público collector. Requires free account."""

    REGISTRY_KEY = "panama_registro_publico"
    JURISDICTION_CODE = "PA"
    BASE_URL = "https://www.rpanama.gob.pa"

    async def login(self) -> bool:
        if not self.username or not self.password:
            logger.warning("Panama registry requires credentials")
            return False
        try:
            await self._page.goto(self.BASE_URL, wait_until="networkidle")
            await self._page.fill("input[name='username'], input[type='email']", self.username)
            await self._page.fill("input[name='password'], input[type='password']", self.password)
            await self._page.click("button[type='submit'], .btn-login")
            await self._page.wait_for_load_state("networkidle")
            return True
        except Exception as e:
            logger.error("Panama login failed: %s", e)
            return False

    async def search_by_name(self, name: str) -> List[Dict]:
        try:
            await self._page.goto(f"{self.BASE_URL}/publicaciones/busqueda", wait_until="networkidle")
            await self._page.fill("input[name*='nombre'], #busqueda", name)
            await self._page.click("button[type='submit'], .btn-buscar")
            await self._page.wait_for_load_state("networkidle")
            await asyncio.sleep(1.5)

            results = []
            rows = await self._page.query_selector_all("table tbody tr, .result-row")
            for row in rows:
                cells = await row.query_selector_all("td")
                if len(cells) >= 2:
                    results.append({
                        "name": (await cells[0].inner_text()).strip(),
                        "registration_number": (await cells[1].inner_text()).strip(),
                        "status": (await cells[3].inner_text()).strip() if len(cells) > 3 else "active",
                    })
            return results
        except Exception as e:
            logger.warning("Panama search error: %s", e)
            return []

    async def search_by_number(self, reg_number: str) -> Optional[CompanyRecord]:
        try:
            url = f"{self.BASE_URL}/publicaciones/empresa?ficha={reg_number}"
            await self._page.goto(url, wait_until="networkidle")
            await asyncio.sleep(1)

            name = (await self._page.text_content(".company-name, h1.title") or "").strip()
            status = (await self._page.text_content(".status, .estado") or "active").strip()

            directors = []
            dir_rows = await self._page.query_selector_all(".directors-table tr, .dignatarios tr")
            for row in dir_rows[1:]:
                cells = await row.query_selector_all("td")
                if cells:
                    directors.append({
                        "name": (await cells[0].inner_text()).strip(),
                        "role": (await cells[1].inner_text()).strip() if len(cells) > 1 else "director",
                    })

            return CompanyRecord(
                registry_key=self.REGISTRY_KEY,
                registration_number=reg_number,
                name=name,
                jurisdiction_code="PA",
                status=self._normalize_status(status),
                directors=directors,
            )
        except Exception as e:
            logger.warning("Panama detail error: %s", e)
            return None

    def _normalize_status(self, status: str) -> str:
        return {"activa": "active", "active": "active", "disuelta": "dissolved",
                "inactiva": "inactive"}.get(status.lower().strip(), "unknown")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("Registry collector ready. Use PanamaRegistryCollector for Panama searches.")
