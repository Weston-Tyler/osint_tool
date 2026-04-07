# Data Attributions

This project is for **research and non-commercial use only**.

The MDA OSINT system aggregates data from multiple public sources, each with
its own license and attribution requirements. Anyone using this system or
republishing data derived from it must comply with these requirements.

## Global Fishing Watch

**License:** Creative Commons BY-NC 4.0 (non-commercial only)

**Datasets used:**
- AIS vessel encounters (`public-global-encounters-events:latest`)
- AIS loitering events (`public-global-loitering-events:latest`)
- AIS port visit events (`public-global-port-visits-events:latest`)
- AIS apparent fishing events (`public-global-fishing-events:latest`)
- AIS gap events (`public-global-gaps-events:latest`)
- Vessel identity (`public-global-vessel-identity:latest`)
- Vessel insights including IUU vessel list matching
- SAR vessel detections (`public-global-sar-presence:latest`)
- EEZ, MPA, RFMO reference regions

**Required attribution:**
> "Powered by Global Fishing Watch."
> https://globalfishingwatch.org

When citing in research:
> Global Fishing Watch. 2026, updated daily. [API dataset name and version],
> [DATE RANGE]. Data set accessed YYYY-MM-DD at https://globalfishingwatch.org/our-apis/.

## GLEIF (Global Legal Entity Identifier Foundation)

**License:** CC0 1.0 Universal (public domain)

**Datasets used:**
- LEI-CDF (Level 1 — entity reference data)
- RR-CDF (Level 2 — relationship/ownership data)

Attribution not legally required, but recommended:
> Source: GLEIF Golden Copy. https://www.gleif.org/

## OFAC SDN List

**License:** US Government public domain

**Datasets used:**
- SDN.XML from sanctionslistservice.ofac.treas.gov

## OpenSanctions

**License:** CC BY-NC 4.0 (non-commercial)

## ICIJ Offshore Leaks

**License:** Open Database License (ODbL) v1.0

## UK Companies House

**License:** UK Open Government Licence v3.0

## SEC EDGAR

**License:** US Government public domain

## GDELT Project

**License:** CC BY-NC-SA 3.0 (non-commercial, share-alike)

## ReliefWeb

**License:** Per-record varies; aggregated metadata under CC0
**Required:** Approved appname registered with the ReliefWeb team

## ACLED (Armed Conflict Location & Event Data)

**License:** Per ACLED terms — research use permitted with attribution.
Commercial use requires a separate license.

## NOAA / FAA / NASA FIRMS / Sentinel

US Government public domain.

## CoinGecko

Free tier: attribution requested.

## World Bank, UNHCR, WFP, FEWS NET

Public domain or open licenses.

---

# Project Use Restrictions

Because the project includes GFW (CC BY-NC), OpenSanctions (CC BY-NC), GDELT
(CC BY-NC-SA), and ACLED (research-only) data, **the aggregated MDA system as
a whole is restricted to non-commercial research use.**

If you want to use this for commercial purposes, you must:

1. Remove all CC BY-NC and research-only data sources, OR
2. Acquire separate commercial licenses from each provider

Dual-licensing the MDA codebase is fine — only the data has restrictions.
