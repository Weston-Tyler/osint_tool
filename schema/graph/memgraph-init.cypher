// ============================================================
// MDA Intelligence Database — Memgraph Schema Initialization
// Run: docker exec mda-memgraph mgconsole < schema/graph/memgraph-init.cypher
// ============================================================

// ============================================================
// UNIQUENESS CONSTRAINTS
// ============================================================

CREATE CONSTRAINT ON (v:Vessel) ASSERT v.imo IS UNIQUE;
CREATE CONSTRAINT ON (v:Vessel) ASSERT v.mmsi IS UNIQUE;
CREATE CONSTRAINT ON (p:Port) ASSERT p.locode IS UNIQUE;
CREATE CONSTRAINT ON (p:Person) ASSERT p.entity_id IS UNIQUE;
CREATE CONSTRAINT ON (o:Organization) ASSERT o.entity_id IS UNIQUE;
CREATE CONSTRAINT ON (e:AIS_Gap_Event) ASSERT e.event_id IS UNIQUE;
CREATE CONSTRAINT ON (e:STS_Transfer) ASSERT e.event_id IS UNIQUE;
CREATE CONSTRAINT ON (e:Interdiction_Event) ASSERT e.event_id IS UNIQUE;
CREATE CONSTRAINT ON (e:UAS_Detection_Event) ASSERT e.event_id IS UNIQUE;
CREATE CONSTRAINT ON (r:Intelligence_Report) ASSERT r.report_id IS UNIQUE;
CREATE CONSTRAINT ON (f:Financial_Entity) ASSERT f.entity_id IS UNIQUE;
CREATE CONSTRAINT ON (d:Drone) ASSERT d.drone_id IS UNIQUE;
CREATE CONSTRAINT ON (s:Sensor_Node) ASSERT s.sensor_id IS UNIQUE;

// ============================================================
// INDEXES (non-unique, for query performance)
// ============================================================

CREATE INDEX ON :Vessel(flag_state);
CREATE INDEX ON :Vessel(risk_score);
CREATE INDEX ON :Vessel(ais_status);
CREATE INDEX ON :Vessel(vessel_type);
CREATE INDEX ON :Vessel(sanctions_status);
CREATE INDEX ON :Port(country_iso);
CREATE INDEX ON :Port(risk_level);
CREATE INDEX ON :Person(sanctions_status);
CREATE INDEX ON :Person(nationality);
CREATE INDEX ON :Organization(threat_level);
CREATE INDEX ON :Organization(org_type);
CREATE INDEX ON :AIS_Gap_Event(gap_start_time);
CREATE INDEX ON :AIS_Gap_Event(probable_cause);
CREATE INDEX ON :UAS_Detection_Event(detection_timestamp);
CREATE INDEX ON :Interdiction_Event(event_date);
CREATE INDEX ON :Intelligence_Report(tlp_color);
CREATE INDEX ON :Intelligence_Report(classification);
CREATE INDEX ON :Seizure(drug_type);
CREATE INDEX ON :Shipment(shipment_type);
CREATE INDEX ON :Shipment(status);
CREATE INDEX ON :Route(route_type);
CREATE INDEX ON :Maritime_Zone(zone_type);
CREATE INDEX ON :Drone(manufacturer);
CREATE INDEX ON :Flight_Path(start_time);
CREATE INDEX ON :Countermeasure_Event(cm_type);
