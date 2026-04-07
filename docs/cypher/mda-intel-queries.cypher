// ═══════════════════════════════════════════════════════════════════════════
// MDA OSINT Intelligence Query Collection
// ═══════════════════════════════════════════════════════════════════════════
//
// Ready-to-run Cypher investigations against the MDA graph.
// Usage:
//   docker exec -i mda-memgraph mgconsole --host=localhost < docs/cypher/mda-intel-queries.cypher
// Or run individual queries by copy-paste.
//
// Data sources assumed loaded:
//   - GLEIF Golden Copy (corporate entities + addresses + ownership)
//   - GFW v3 events: Encounters, Loitering, PortVisits, Fishing, AISGaps
//   - GFW v3 Vessel Identity (populated by gfw_enrich_vessels.py)
//
// MID = Maritime Identification Digits — the first 3 digits of an MMSI identify
// the flag state. Full list: https://www.itu.int/en/ITU-R/terrestrial/fmd/Pages/mid.aspx
//
// Key MIDs referenced in these queries:
//   401 = Afghanistan           412-413 = China (mainland)
//   403 = Iran                  414 = China (Tibet)
//   273 = Russia                416 = Taiwan
//   574 = Vietnam               419 = India
//   600 = Cambodia              525 = Indonesia
//   311 = Bahamas (FoC)         538 = Marshall Islands (FoC)
//   351-352 = Panama (FoC)      636-637 = Liberia (FoC)
//   215-230 = EU cluster        341-347 = Caribbean FoCs
// ═══════════════════════════════════════════════════════════════════════════


// ───────────────────────────────────────────────────────────────────────────
// INVESTIGATION 1 — IRANIAN DARK FLEET ENCOUNTER PROFILE
// ───────────────────────────────────────────────────────────────────────────
// Iranian-flagged MMSIs (MID 403) with abnormally high encounter rates are
// the signature of the "dark fleet" that moves sanctioned Iranian crude oil
// via ship-to-ship transfers, mostly to Chinese teapot refineries.
// Normal commercial vessels encounter 1-3 other ships per week; dark fleet
// tankers can hit 50-100+ encounters per week because they're constantly
// passing cargo along.
// ───────────────────────────────────────────────────────────────────────────

// 1a. Top Iranian-flagged vessels by encounter volume
MATCH (v:Vessel)-[:INVOLVED_IN]->(e:Encounter)
WHERE v.mmsi STARTS WITH '403'
RETURN v.mmsi, v.name, v.vessel_type, v.owner_name, v.owner_flag,
       count(e) AS encounter_count
ORDER BY encounter_count DESC
LIMIT 25;

// 1b. Who are the Iranian fleet encountering? (counterparties by flag)
MATCH (v1:Vessel)-[:INVOLVED_IN]->(e:Encounter)<-[:ENCOUNTERED]-(v2:Vessel)
WHERE v1.mmsi STARTS WITH '403'
WITH substring(v2.mmsi, 0, 3) AS partner_mid, count(e) AS encounters
RETURN partner_mid, encounters
ORDER BY encounters DESC;

// 1c. Iranian fleet with the specific vessels they met (top pairs)
MATCH (v1:Vessel)-[:INVOLVED_IN]->(e:Encounter)<-[:ENCOUNTERED]-(v2:Vessel)
WHERE v1.mmsi STARTS WITH '403'
RETURN v1.mmsi AS iran_mmsi, v1.name AS iran_name,
       v2.mmsi AS partner_mmsi, v2.name AS partner_name, v2.flag AS partner_flag,
       count(e) AS meetings
ORDER BY meetings DESC
LIMIT 30;


// ───────────────────────────────────────────────────────────────────────────
// INVESTIGATION 2 — EAST CHINA SEA IUU FISHING HOTSPOTS
// ───────────────────────────────────────────────────────────────────────────
// The ~500km x 500km box from roughly (28°N, 120°E) to (37°N, 126°E) is
// a known staging and loitering ground for the Chinese distant-water
// fishing fleet. Heavy loitering volume plus mixed flags = classic
// transshipment / IUU laundering pattern.
// ───────────────────────────────────────────────────────────────────────────

// 2a. Loitering density grid (0.5° cells) for the East China Sea
MATCH (e:LoiteringEvent)
WHERE e.lat >= 25 AND e.lat <= 40
  AND e.lon >= 118 AND e.lon <= 130
RETURN round(e.lat * 2) / 2 AS lat_grid,
       round(e.lon * 2) / 2 AS lon_grid,
       count(e) AS loiter_count
ORDER BY loiter_count DESC
LIMIT 25;

// 2b. Vessels loitering inside the IUU hotspot box, grouped by flag state
MATCH (v:Vessel)-[:LOITERED_AT]->(e:LoiteringEvent)
WHERE e.lat >= 28 AND e.lat <= 37
  AND e.lon >= 120 AND e.lon <= 126
WITH substring(v.mmsi, 0, 3) AS mid, count(DISTINCT v) AS vessel_count, count(e) AS event_count
RETURN mid, vessel_count, event_count
ORDER BY event_count DESC
LIMIT 20;

// 2c. Chinese fishing vessels (MID 412/413) that also appear in Encounter
// events — candidates for trans-shipment inspection
MATCH (v:Vessel)
WHERE v.mmsi STARTS WITH '412' OR v.mmsi STARTS WITH '413'
MATCH (v)-[:LOITERED_AT]->(l:LoiteringEvent)
WHERE l.lat >= 28 AND l.lat <= 37 AND l.lon >= 120 AND l.lon <= 126
WITH v, count(l) AS loiter_count
MATCH (v)-[:INVOLVED_IN]->(e:Encounter)
RETURN v.mmsi, v.name, v.vessel_type, v.owner_name,
       loiter_count, count(e) AS encounter_count
ORDER BY (loiter_count + encounter_count) DESC
LIMIT 25;


// ───────────────────────────────────────────────────────────────────────────
// INVESTIGATION 3 — FLAG-MIXING TRANSSHIPMENT NETWORK
// ───────────────────────────────────────────────────────────────────────────
// Vessels that meet counterparties from many different flag states in a
// short window are candidates for trans-shipment fleets — the behavior
// pattern of carrier vessels that collect catch or cargo from multiple
// small operators and carry it to market.
// ───────────────────────────────────────────────────────────────────────────

// 3a. Vessels with highest partner-flag diversity
MATCH (v1:Vessel)-[:INVOLVED_IN]->(e:Encounter)<-[:ENCOUNTERED]-(v2:Vessel)
WHERE v1.mmsi IS NOT NULL AND v2.mmsi IS NOT NULL
WITH v1, collect(DISTINCT substring(v2.mmsi, 0, 3)) AS partner_mids,
     count(e) AS total_encounters
WHERE size(partner_mids) >= 4
RETURN v1.mmsi, v1.name, v1.flag, v1.vessel_type,
       size(partner_mids) AS distinct_partner_flags,
       partner_mids,
       total_encounters
ORDER BY distinct_partner_flags DESC, total_encounters DESC
LIMIT 20;

// 3b. Flag-of-convenience (FoC) vessels operating as trans-shippers
// (Panama 351-352, Liberia 636-637, Marshall 538, Bahamas 311, etc.)
MATCH (v1:Vessel)-[:INVOLVED_IN]->(e:Encounter)<-[:ENCOUNTERED]-(v2:Vessel)
WHERE v1.mmsi STARTS WITH '351' OR v1.mmsi STARTS WITH '352'
   OR v1.mmsi STARTS WITH '636' OR v1.mmsi STARTS WITH '637'
   OR v1.mmsi STARTS WITH '538'
   OR v1.mmsi STARTS WITH '311' OR v1.mmsi STARTS WITH '341'
WITH v1, count(DISTINCT v2) AS distinct_partners, count(e) AS encounters
WHERE distinct_partners >= 3
RETURN v1.mmsi, v1.name, v1.vessel_type, v1.owner_name,
       distinct_partners, encounters
ORDER BY encounters DESC
LIMIT 20;


// ───────────────────────────────────────────────────────────────────────────
// INVESTIGATION 4 — VESSEL PATTERN-OF-LIFE BEHAVIORAL SCORE
// ───────────────────────────────────────────────────────────────────────────
// Composite score combining encounters, loitering, and gap events.
// High score = vessel doing a lot of "not normal" things.
// ───────────────────────────────────────────────────────────────────────────

// 4a. Behavioral risk score
MATCH (v:Vessel)
OPTIONAL MATCH (v)-[:INVOLVED_IN]->(e:Encounter)
WITH v, count(e) AS encounters
OPTIONAL MATCH (v)-[:LOITERED_AT]->(l:LoiteringEvent)
WITH v, encounters, count(l) AS loiters
OPTIONAL MATCH (v)-[:WENT_DARK_AT]->(g:AISGap)
WITH v, encounters, loiters, count(g) AS dark_events
WHERE (encounters + loiters + dark_events) > 0
RETURN v.mmsi, v.name, v.flag, v.vessel_type,
       encounters, loiters, dark_events,
       (encounters * 2 + loiters + dark_events * 3) AS risk_score
ORDER BY risk_score DESC
LIMIT 30;


// ───────────────────────────────────────────────────────────────────────────
// INVESTIGATION 5 — ENCOUNTER TYPE BREAKDOWN (the money signal)
// ───────────────────────────────────────────────────────────────────────────
// GFW classifies encounters by the TYPE of vessels meeting:
//   CARRIER-FISHING / FISHING-CARRIER = catch transfer (IUU red flag)
//   BUNKER-FISHING / BUNKER-CARRIER = refueling at sea (legal but sanctionable)
//   TANKER-FISHING = highly suspicious (why is a fishing boat meeting a tanker?)
// ───────────────────────────────────────────────────────────────────────────

// 5a. Encounter counts by sub-type
MATCH (e:Encounter)
WHERE e.encounter_type IS NOT NULL
RETURN e.encounter_type, count(e) AS cnt
ORDER BY cnt DESC;

// 5b. Top fishing-meeting-carrier events (classic IUU pattern)
MATCH (v:Vessel)-[:INVOLVED_IN]->(e:Encounter)<-[:ENCOUNTERED]-(v2:Vessel)
WHERE e.encounter_type IN ['CARRIER-FISHING', 'FISHING-CARRIER']
RETURN v.mmsi, v.name, v.vessel_type AS v1_type,
       v2.mmsi, v2.name, v2.vessel_type AS v2_type,
       e.lat, e.lon, e.start_time, e.median_distance_km
ORDER BY e.start_time DESC
LIMIT 25;


// ───────────────────────────────────────────────────────────────────────────
// INVESTIGATION 6 — AIS DARK GAPS >24H (going completely dark)
// ───────────────────────────────────────────────────────────────────────────
// GFW flags vessels that stop broadcasting AIS for >12h in the open ocean.
// Long gaps plus high encounter rate = classic sanctions evasion behavior:
// turn off the transponder, meet another vessel, turn it back on.
// ───────────────────────────────────────────────────────────────────────────

// 6a. Vessels with the longest or most frequent dark periods
MATCH (v:Vessel)-[:WENT_DARK_AT]->(g:AISGap)
WITH v, count(g) AS dark_events, max(g.gap_hours) AS longest_dark_h
RETURN v.mmsi, v.name, v.flag, v.vessel_type,
       dark_events, longest_dark_h
ORDER BY dark_events DESC, longest_dark_h DESC
LIMIT 25;

// 6b. Combined dark+encounter anomaly: vessels that go dark AND encounter a lot
MATCH (v:Vessel)-[:WENT_DARK_AT]->(g:AISGap)
WITH v, count(g) AS dark_count
WHERE dark_count >= 2
MATCH (v)-[:INVOLVED_IN]->(e:Encounter)
WITH v, dark_count, count(e) AS encounter_count
WHERE encounter_count >= 5
RETURN v.mmsi, v.name, v.flag, v.vessel_type,
       dark_count, encounter_count
ORDER BY (dark_count * encounter_count) DESC
LIMIT 20;


// ───────────────────────────────────────────────────────────────────────────
// INVESTIGATION 7 — PORT VISIT CHAIN (where are dark fleet tankers landing?)
// ───────────────────────────────────────────────────────────────────────────
// Port visits by suspicious vessels reveal the on-ramps for sanctioned cargo.
// ───────────────────────────────────────────────────────────────────────────

MATCH (v:Vessel)-[:VISITED]->(p:PortVisit)
WHERE v.mmsi STARTS WITH '403'  // Iran
   OR v.mmsi STARTS WITH '273'  // Russia
RETURN v.mmsi, v.name, v.flag,
       p.port_name, p.port_country,
       p.start_time, p.end_time
ORDER BY p.start_time DESC
LIMIT 30;


// ───────────────────────────────────────────────────────────────────────────
// INVESTIGATION 8 — CROSS-GRAPH: CORPORATE OWNERSHIP OF HIGH-RISK VESSELS
// ───────────────────────────────────────────────────────────────────────────
// Links the GFW vessel layer to the GLEIF corporate graph — shows the
// company that owns a high-encounter vessel and that company's jurisdiction.
// This is where you start seeing shell-company layering in offshore havens.
// ───────────────────────────────────────────────────────────────────────────

MATCH (v:Vessel)-[:INVOLVED_IN]->(e:Encounter)
WITH v, count(e) AS encounters
WHERE encounters >= 10 AND v.owner_name IS NOT NULL
OPTIONAL MATCH (c:Company)
  WHERE toLower(c.legal_name) CONTAINS toLower(v.owner_name)
     OR toLower(v.owner_name) CONTAINS toLower(c.legal_name)
RETURN v.mmsi, v.name, v.flag,
       v.owner_name, v.owner_flag,
       c.lei, c.legal_name, c.country_iso,
       encounters
ORDER BY encounters DESC
LIMIT 25;


// ───────────────────────────────────────────────────────────────────────────
// INVESTIGATION 9 — GEOGRAPHIC CLUSTERS OF ALL EVENT TYPES (heat map data)
// ───────────────────────────────────────────────────────────────────────────
// Export this to GeoServer or a map layer to visualize where everything
// is happening globally.
// ───────────────────────────────────────────────────────────────────────────

// 9a. All events with positions in the last 7 days, bucketed to 1° cells
MATCH (e)
WHERE (e:Encounter OR e:LoiteringEvent OR e:PortVisit OR e:FishingEvent OR e:AISGap)
  AND e.lat IS NOT NULL AND e.lon IS NOT NULL
RETURN round(e.lat) AS lat_1deg,
       round(e.lon) AS lon_1deg,
       labels(e)[0] AS event_type,
       count(*) AS events
ORDER BY events DESC
LIMIT 50;


// ───────────────────────────────────────────────────────────────────────────
// INVESTIGATION 10 — SUSPICIOUS TRIANGLES (A meets B meets C pattern)
// ───────────────────────────────────────────────────────────────────────────
// Three-vessel encounter chains within a short window — the signature of
// a relay: vessel A transfers cargo to B, B transfers to C, distancing
// the origin from the final handler.
// ───────────────────────────────────────────────────────────────────────────

MATCH (a:Vessel)-[:INVOLVED_IN]->(e1:Encounter)<-[:ENCOUNTERED]-(b:Vessel)
MATCH (b)-[:INVOLVED_IN]->(e2:Encounter)<-[:ENCOUNTERED]-(c:Vessel)
WHERE a <> c AND a.mmsi < c.mmsi
WITH a, b, c, e1, e2
RETURN a.mmsi AS A_mmsi, a.name AS A_name, a.flag AS A_flag,
       b.mmsi AS B_mmsi, b.name AS B_name, b.flag AS B_flag,
       c.mmsi AS C_mmsi, c.name AS C_name, c.flag AS C_flag,
       e1.start_time AS t1, e2.start_time AS t2
ORDER BY e2.start_time DESC
LIMIT 20;
