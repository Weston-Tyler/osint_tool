"""SEC EDGAR ownership collector.

Collects beneficial ownership data from SEC EDGAR filings:
- 13D/13G: Beneficial ownership disclosures (>5%)
- 13F: Institutional holdings (quarterly)
- 10-K Exhibit 21: Subsidiary lists
- Form 3/4/5: Insider transactions

API: https://data.sec.gov (no key required, User-Agent header required)
Rate limit: 10 requests/second per IP.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import aiohttp
from bs4 import BeautifulSoup
from kafka import KafkaProducer

logger = logging.getLogger("mda.worker.corporate.sec_edgar")

EDGAR_API_BASE = "https://data.sec.gov"
EDGAR_SEARCH_BASE = "https://efts.sec.gov"
USER_AGENT = os.getenv(
    "EDGAR_USER_AGENT",
    "MDA-Corporate-Research contact@mda-research.example.com",
)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

HEADERS = {"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"}


@dataclass
class EDGARFiling:
    accession_number: str
    cik: str
    company_name: str
    filing_type: str
    filing_date: str
    period_of_report: Optional[str] = None
    primary_document: Optional[str] = None
    edgar_url: Optional[str] = None


@dataclass
class BeneficialOwner:
    filer_name: str
    cik: Optional[str]
    target_company_name: str
    target_company_cik: str
    ownership_percentage: float
    filing_type: str
    filing_date: str
    is_passive: bool = False  # 13G = passive, 13D = active
    source_url: Optional[str] = None


@dataclass
class Subsidiary:
    parent_cik: str
    parent_name: str
    subsidiary_name: str
    jurisdiction: str
    source: str = "sec_edgar_exhibit_21"
    confidence: float = 0.85


class EDGARAPIClient:
    """SEC EDGAR API client. No API key required — just User-Agent header."""

    def __init__(self, rate_limit_per_second: float = 10.0):
        self.min_interval = 1.0 / rate_limit_per_second
        self._last_request = 0.0
        self._lock = asyncio.Lock()

    async def _rate_limit(self) -> None:
        async with self._lock:
            elapsed = time.time() - self._last_request
            if elapsed < self.min_interval:
                await asyncio.sleep(self.min_interval - elapsed)
            self._last_request = time.time()

    async def get_company_submissions(
        self, cik: str, session: aiohttp.ClientSession
    ) -> Optional[dict]:
        """Get all filings for a company by CIK."""
        cik_padded = str(cik).zfill(10)
        url = f"{EDGAR_API_BASE}/submissions/CIK{cik_padded}.json"
        await self._rate_limit()
        try:
            async with session.get(url, headers=HEADERS, timeout=30) as resp:
                if resp.status == 200:
                    return await resp.json()
                logger.warning("EDGAR submissions %s: HTTP %d", cik, resp.status)
        except Exception as e:
            logger.error("EDGAR submissions error: %s", e)
        return None

    async def get_company_tickers(
        self, session: aiohttp.ClientSession
    ) -> dict:
        """Get the full list of company tickers and CIKs."""
        url = "https://www.sec.gov/files/company_tickers.json"
        await self._rate_limit()
        async with session.get(url, headers=HEADERS, timeout=30) as resp:
            return await resp.json()

    async def search_filings(
        self,
        form_type: str,
        date_start: str,
        date_end: str,
        session: aiohttp.ClientSession,
        from_idx: int = 0,
        size: int = 100,
    ) -> dict:
        """Search EDGAR full-text index for filings."""
        params = {
            "q": "",
            "forms": form_type,
            "dateRange": "custom",
            "startdt": date_start,
            "enddt": date_end,
            "from": str(from_idx),
            "size": str(size),
        }
        url = f"{EDGAR_SEARCH_BASE}/LATEST/search-index"
        await self._rate_limit()
        try:
            async with session.get(url, headers=HEADERS, params=params, timeout=30) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception as e:
            logger.error("EDGAR search error: %s", e)
        return {}

    async def get_filing_document(
        self,
        accession_number: str,
        cik: str,
        filename: str,
        session: aiohttp.ClientSession,
    ) -> Optional[str]:
        """Download a specific document from an EDGAR filing."""
        acc_clean = accession_number.replace("-", "")
        url = (
            f"https://www.sec.gov/Archives/edgar/data/"
            f"{int(cik)}/{acc_clean}/{filename}"
        )
        await self._rate_limit()
        try:
            async with session.get(url, headers=HEADERS, timeout=60) as resp:
                if resp.status == 200:
                    return await resp.text()
        except Exception as e:
            logger.error("EDGAR document fetch error: %s", e)
        return None


class Schedule13DParser:
    """Parse SEC Schedule 13D/13G filings for beneficial ownership data."""

    PCT_PATTERNS = [
        re.compile(
            r"(?:aggregate|beneficial)\s+(?:ownership|amount).*?(\d+\.?\d*)\s*%",
            re.IGNORECASE | re.DOTALL,
        ),
        re.compile(
            r"PERCENT\s+OF\s+CLASS.*?(\d+\.?\d*)\s*%",
            re.IGNORECASE | re.DOTALL,
        ),
        re.compile(
            r"item\s+5.*?(\d+\.?\d*)\s*%",
            re.IGNORECASE | re.DOTALL,
        ),
    ]

    NAME_PATTERN = re.compile(
        r"Names?\s+of\s+Reporting\s+Persons?[\s:]*\n?\s*([A-Z][A-Za-z0-9\s,\.\-&']+)",
        re.IGNORECASE,
    )

    def extract_beneficial_owners(
        self, filing_html: str, target_company_cik: str, target_company_name: str = ""
    ) -> list[BeneficialOwner]:
        """Extract beneficial owner information from a 13D/13G filing HTML."""
        soup = BeautifulSoup(filing_html, "html.parser")
        text = soup.get_text(separator=" ")
        text = re.sub(r"\s+", " ", text)

        # Try to extract ownership percentage
        ownership_pct = None
        for pattern in self.PCT_PATTERNS:
            match = pattern.search(text)
            if match:
                try:
                    pct = float(match.group(1))
                    if 0 < pct <= 100:
                        ownership_pct = pct
                        break
                except (ValueError, IndexError):
                    continue

        # Extract filer name
        filer_name = None
        name_match = self.NAME_PATTERN.search(text)
        if name_match:
            filer_name = name_match.group(1).strip()[:200]

        if not filer_name or not ownership_pct:
            return []

        is_passive = "13G" in filing_html[:5000].upper()

        return [
            BeneficialOwner(
                filer_name=filer_name,
                cik=None,
                target_company_name=target_company_name,
                target_company_cik=target_company_cik,
                ownership_percentage=ownership_pct,
                filing_type="13G" if is_passive else "13D",
                filing_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                is_passive=is_passive,
            )
        ]


class Form10KSubsidiaryParser:
    """Parse Exhibit 21 (subsidiary list) from 10-K filings."""

    def extract_subsidiaries(
        self, exhibit_21_text: str, parent_cik: str, parent_name: str = ""
    ) -> list[Subsidiary]:
        """Parse Exhibit 21 to extract subsidiary entities."""
        soup = BeautifulSoup(exhibit_21_text, "html.parser")
        subsidiaries: list[Subsidiary] = []

        # Try table format first
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            for row in rows[1:]:  # Skip header
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    name = cells[0].get_text(strip=True)
                    jurisdiction = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    if name and len(name) > 2:
                        subsidiaries.append(
                            Subsidiary(
                                parent_cik=parent_cik,
                                parent_name=parent_name,
                                subsidiary_name=name,
                                jurisdiction=jurisdiction,
                            )
                        )

        # Fallback: text parsing if no tables
        if not subsidiaries:
            text = soup.get_text(separator="\n")
            for line in text.split("\n"):
                line = line.strip()
                if not line or len(line) < 5:
                    continue
                # Match "Company Name, Delaware" or "Company Name (Delaware)"
                m = re.match(r"^(.+?)[,\(]\s*([A-Z][a-z\s]+?)\)?\s*$", line)
                if m:
                    name = m.group(1).strip()
                    jurisdiction = m.group(2).strip()
                    if 5 < len(name) < 200 and len(jurisdiction) < 50:
                        subsidiaries.append(
                            Subsidiary(
                                parent_cik=parent_cik,
                                parent_name=parent_name,
                                subsidiary_name=name,
                                jurisdiction=jurisdiction,
                            )
                        )

        return subsidiaries


class SECEdgarCollector:
    """Main SEC EDGAR collector orchestrator."""

    def __init__(self):
        self.api = EDGARAPIClient(rate_limit_per_second=10.0)
        self.d13_parser = Schedule13DParser()
        self.subsidiary_parser = Form10KSubsidiaryParser()
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            compression_type="gzip",
        )

    async def collect_recent_13d_13g(
        self, days_back: int = 7, max_filings: int = 100
    ) -> int:
        """Collect recent 13D/13G filings and extract beneficial owners."""
        from datetime import timedelta

        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        start_date = (
            datetime.now(timezone.utc) - timedelta(days=days_back)
        ).strftime("%Y-%m-%d")

        count = 0
        async with aiohttp.ClientSession() as session:
            for form_type in ["SC 13D", "SC 13G"]:
                results = await self.api.search_filings(
                    form_type, start_date, end_date, session, size=max_filings
                )
                hits = results.get("hits", {}).get("hits", [])

                for hit in hits[:max_filings]:
                    src = hit.get("_source", {})
                    accession = src.get("adsh", "")
                    cik = src.get("ciks", [""])[0] if src.get("ciks") else ""
                    if not accession or not cik:
                        continue

                    self.producer.send(
                        "mda.corporate.sec_edgar.raw",
                        {
                            "type": "13d_13g_filing",
                            "accession_number": accession,
                            "cik": cik,
                            "form_type": form_type,
                            "filing_date": src.get("file_date", ""),
                            "company_names": src.get("display_names", []),
                        },
                    )
                    count += 1

        self.producer.flush()
        logger.info("Collected %d 13D/13G filings", count)
        return count

    async def collect_subsidiaries(self, ciks: list[str]) -> int:
        """Collect subsidiary lists from 10-K Exhibit 21 for given CIKs."""
        count = 0
        async with aiohttp.ClientSession() as session:
            for cik in ciks:
                submissions = await self.api.get_company_submissions(cik, session)
                if not submissions:
                    continue

                company_name = submissions.get("name", "")
                recent = submissions.get("filings", {}).get("recent", {})
                forms = recent.get("form", [])

                # Find most recent 10-K
                for i, form in enumerate(forms):
                    if form == "10-K":
                        accession = recent.get("accessionNumber", [""])[i]
                        primary_doc = recent.get("primaryDocument", [""])[i]
                        if accession and primary_doc:
                            doc_text = await self.api.get_filing_document(
                                accession, cik, primary_doc, session
                            )
                            if doc_text:
                                subs = self.subsidiary_parser.extract_subsidiaries(
                                    doc_text, cik, company_name
                                )
                                for sub in subs:
                                    self.producer.send(
                                        "mda.corporate.sec_edgar.raw",
                                        {
                                            "type": "subsidiary",
                                            **sub.__dict__,
                                        },
                                    )
                                    count += 1
                        break

        self.producer.flush()
        logger.info("Collected %d subsidiaries", count)
        return count


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    collector = SECEdgarCollector()

    # Collect recent beneficial ownership filings
    await collector.collect_recent_13d_13g(days_back=30)

    # Collect subsidiaries for top CIKs (would normally come from a watchlist)
    # Apple=0000320193, Microsoft=0000789019, Google=0001652044
    sample_ciks = ["0000320193", "0000789019", "0001652044"]
    await collector.collect_subsidiaries(sample_ciks)


if __name__ == "__main__":
    asyncio.run(main())
