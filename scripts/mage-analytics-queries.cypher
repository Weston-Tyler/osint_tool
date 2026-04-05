// ============================================================
// Memgraph MAGE Analytics — Operational Query Library
// Requires: Memgraph MAGE module (included in memgraph-platform image)
// ============================================================

// ---- COMMUNITY DETECTION (Louvain) ----
// Find organizational clusters in the cartel/entity network
// Useful for discovering hidden network structures
CALL community_detection.get()
YIELD node, community_id
WITH community_id,
     count(node) AS community_size,
     collect(CASE WHEN "Organization" IN labels(node) THEN node.name END)[0..5] AS orgs,
     collect(CASE WHEN "Person" IN labels(node) THEN node.name END)[0..5] AS persons,
     collect(CASE WHEN "Vessel" IN labels(node) THEN node.name END)[0..5] AS vessels
WHERE community_size >= 3
RETURN community_id, community_size, orgs, persons, vessels
ORDER BY community_size DESC
LIMIT 20;

// ---- BETWEENNESS CENTRALITY ----
// Find the most critical connector nodes in the network
// High betweenness = removal would most disrupt the network
CALL betweenness_centrality.get(FALSE, TRUE)
YIELD node, betweenness_centrality
WHERE betweenness_centrality > 0
RETURN labels(node)[0] AS type,
       node.name AS name,
       node.entity_id AS entity_id,
       betweenness_centrality
ORDER BY betweenness_centrality DESC
LIMIT 30;

// ---- PAGERANK ----
// Find the most "important" nodes by link structure
CALL pagerank.get()
YIELD node, rank
WHERE rank > 0.001
RETURN labels(node)[0] AS type,
       node.name AS name,
       rank
ORDER BY rank DESC
LIMIT 30;

// ---- WEAKLY CONNECTED COMPONENTS ----
// Find isolated subgraphs — entities with no connection to the main network
CALL weakly_connected_components.get()
YIELD node, component_id
WITH component_id, collect(node) AS members
WHERE size(members) >= 2
RETURN component_id,
       size(members) AS component_size,
       [m IN members | labels(m)[0] + ': ' + coalesce(m.name, m.entity_id, 'unnamed')][0..5] AS sample
ORDER BY component_size DESC
LIMIT 20;

// ---- SANCTIONED ENTITY NETWORK (2-hop) ----
// Find all entities within 2 hops of any sanctioned entity
MATCH (sanc)
WHERE sanc.sanctions_status = 'SANCTIONED'
MATCH path = (sanc)<-[*1..2]-(v:Vessel)
WHERE v.imo IS NOT NULL
RETURN DISTINCT v.imo AS imo, v.name AS name, v.flag_state AS flag,
       v.risk_score AS risk, length(path) AS hops
ORDER BY v.risk_score DESC, hops
LIMIT 100;

// ---- SHELL COMPANY CHAIN DETECTION ----
// Find ownership chains with no identifiable person at the top
MATCH path = (:Organization)-[:OWNED_BY*3..5]->(:Organization)
WHERE NOT EXISTS {
  MATCH (n)-[:OWNED_BY]->(:Person)
  WHERE n IN nodes(path)
}
RETURN [n IN nodes(path) | n.name] AS chain,
       length(path) AS depth
ORDER BY depth DESC
LIMIT 20;

// ---- PORT OVERLAP ANALYSIS ----
// Vessels visiting same port as sanctioned vessel within 30 days
MATCH (sv:Vessel)-[r1:VISITED]->(p:Port)<-[r2:VISITED]-(v:Vessel)
WHERE sv.sanctions_status = 'SANCTIONED'
  AND v.imo <> sv.imo
  AND r1.arrival_time IS NOT NULL AND r2.arrival_time IS NOT NULL
RETURN v.imo AS vessel_imo, v.name AS vessel_name,
       sv.imo AS sanctioned_imo, sv.name AS sanctioned_name,
       p.locode AS port, p.name AS port_name
LIMIT 50;

// ---- VESSEL DARK FLEET DETECTION ----
// Vessels active (AIS on) but no port visits in 6+ months
MATCH (v:Vessel)
WHERE v.valid_to IS NULL
  AND v.ais_status = 'ACTIVE'
  AND NOT EXISTS {
    MATCH (v)-[r:VISITED]->(:Port)
    WHERE r.arrival_time >= localDateTime() - duration({months: 6})
  }
RETURN v.imo, v.name, v.vessel_type, v.flag_state, v.risk_score
ORDER BY v.risk_score DESC
LIMIT 50;

// ---- CROSS-DOMAIN CORRELATION SUMMARY ----
// All correlations with confidence >= 0.6 in last 30 days
MATCH (a)-[c:CORRELATED_WITH]-(b)
WHERE c.confidence_score >= 0.60
RETURN labels(a)[0] AS entity_a_type,
       labels(b)[0] AS entity_b_type,
       c.correlation_type AS type,
       c.confidence_score AS confidence,
       c.hypothesis AS hypothesis
ORDER BY c.confidence_score DESC
LIMIT 50;

// ---- FINANCIAL FLOW TRACING ----
// Follow money from person through financial entities
MATCH path = (p:Person)-[:CONTROLS|TRANSACTED_WITH*1..4]->(fin:Financial_Entity)
WHERE p.sanctions_status = 'SANCTIONED'
RETURN p.name AS person,
       [n IN nodes(path)[1..] | n.name] AS financial_chain,
       length(path) AS layers
ORDER BY layers DESC
LIMIT 30;

// ---- RISK DASHBOARD DIGEST ----
// Top risk vessels updated in last 24 hours
MATCH (v:Vessel)
WHERE v.risk_score >= 7.0
  AND v.system_updated_at >= localDateTime() - duration({hours: 24})
OPTIONAL MATCH (v)-[:HAS_AIS_GAP]->(gap:AIS_Gap_Event)
  WHERE gap.gap_start_time >= localDateTime() - duration({hours: 24})
OPTIONAL MATCH (v)-[enc:ENCOUNTERED]->(v2:Vessel)
RETURN v.imo, v.name, v.flag_state, v.risk_score,
       v.ais_status, v.sanctions_status,
       count(DISTINCT gap) AS gaps_24h,
       collect(DISTINCT v2.name)[0..3] AS recent_encounters
ORDER BY v.risk_score DESC
LIMIT 25;
