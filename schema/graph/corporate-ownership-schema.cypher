// ============================================================
// MDA Corporate Ownership Graph — Memgraph Schema
// Extends the base MDA schema with corporate ownership nodes,
// relationships, and indexes for GLEIF, Companies House, ICIJ,
// SEC EDGAR, and property/vessel ownership tracking.
// Run: docker exec mda-memgraph mgconsole < schema/graph/corporate-ownership-schema.cypher
// ============================================================

// ============================================================
// UNIQUENESS CONSTRAINTS
// ============================================================

// ── Company ────────────────────────────────────────────────
CREATE CONSTRAINT ON (c:Company) ASSERT c.lei IS UNIQUE;
CREATE CONSTRAINT ON (c:Company) ASSERT c.company_number IS UNIQUE;
CREATE CONSTRAINT ON (c:Company) ASSERT c.entity_id IS UNIQUE;

// ── Person ─────────────────────────────────────────────────
CREATE CONSTRAINT ON (p:Person) ASSERT p.entity_id IS UNIQUE;

// ── Organization ───────────────────────────────────────────
CREATE CONSTRAINT ON (o:Organization) ASSERT o.entity_id IS UNIQUE;

// ── Ownership ──────────────────────────────────────────────
CREATE CONSTRAINT ON (ow:Ownership) ASSERT ow.ownership_id IS UNIQUE;

// ── Directorship ───────────────────────────────────────────
CREATE CONSTRAINT ON (d:Directorship) ASSERT d.directorship_id IS UNIQUE;

// ── Membership ─────────────────────────────────────────────
CREATE CONSTRAINT ON (m:Membership) ASSERT m.membership_id IS UNIQUE;

// ── Sanction ───────────────────────────────────────────────
CREATE CONSTRAINT ON (s:Sanction) ASSERT s.sanction_id IS UNIQUE;

// ── Address ────────────────────────────────────────────────
CREATE CONSTRAINT ON (a:Address) ASSERT a.address_id IS UNIQUE;

// ── Property ───────────────────────────────────────────────
CREATE CONSTRAINT ON (pr:Property) ASSERT pr.property_id IS UNIQUE;

// ── FinancialAccount ───────────────────────────────────────
CREATE CONSTRAINT ON (fa:FinancialAccount) ASSERT fa.account_id IS UNIQUE;

// ── Vessel ─────────────────────────────────────────────────
// Note: base Vessel constraints (imo, mmsi) exist in memgraph-init.cypher
// We add entity_id for cross-referencing with corporate ownership graph
CREATE CONSTRAINT ON (v:Vessel) ASSERT v.entity_id IS UNIQUE;

// ── VesselOwnership ────────────────────────────────────────
CREATE CONSTRAINT ON (vo:VesselOwnership) ASSERT vo.vessel_ownership_id IS UNIQUE;

// ── PropertyOwnership ──────────────────────────────────────
CREATE CONSTRAINT ON (po:PropertyOwnership) ASSERT po.property_ownership_id IS UNIQUE;

// ── RegistrationRecord ─────────────────────────────────────
CREATE CONSTRAINT ON (rr:RegistrationRecord) ASSERT rr.registration_id IS UNIQUE;

// ============================================================
// INDEXES — Company
// ============================================================

CREATE INDEX ON :Company(legal_name);
CREATE INDEX ON :Company(jurisdiction);
CREATE INDEX ON :Company(status);
CREATE INDEX ON :Company(legal_form);
CREATE INDEX ON :Company(incorporation_date);
CREATE INDEX ON :Company(sic_codes);
CREATE INDEX ON :Company(risk_score);
CREATE INDEX ON :Company(sanctions_status);
CREATE INDEX ON :Company(source);
CREATE INDEX ON :Company(country_iso);
CREATE INDEX ON :Company(is_shell_company);
CREATE INDEX ON :Company(lei_status);
CREATE INDEX ON :Company(corroboration_level);

// ============================================================
// INDEXES — Person
// ============================================================

CREATE INDEX ON :Person(name_full);
CREATE INDEX ON :Person(nationality);
CREATE INDEX ON :Person(sanctions_status);
CREATE INDEX ON :Person(dob);
CREATE INDEX ON :Person(country_of_residence);
CREATE INDEX ON :Person(pep_status);
CREATE INDEX ON :Person(risk_score);

// ============================================================
// INDEXES — Organization
// ============================================================

CREATE INDEX ON :Organization(name);
CREATE INDEX ON :Organization(org_type);
CREATE INDEX ON :Organization(jurisdiction);
CREATE INDEX ON :Organization(sanctions_status);
CREATE INDEX ON :Organization(threat_level);
CREATE INDEX ON :Organization(risk_score);

// ============================================================
// INDEXES — Ownership
// ============================================================

CREATE INDEX ON :Ownership(ownership_type);
CREATE INDEX ON :Ownership(percentage);
CREATE INDEX ON :Ownership(start_date);
CREATE INDEX ON :Ownership(end_date);
CREATE INDEX ON :Ownership(is_beneficial);
CREATE INDEX ON :Ownership(source);
CREATE INDEX ON :Ownership(status);

// ============================================================
// INDEXES — Directorship
// ============================================================

CREATE INDEX ON :Directorship(role);
CREATE INDEX ON :Directorship(appointed_date);
CREATE INDEX ON :Directorship(resigned_date);
CREATE INDEX ON :Directorship(status);
CREATE INDEX ON :Directorship(source);

// ============================================================
// INDEXES — Membership
// ============================================================

CREATE INDEX ON :Membership(role);
CREATE INDEX ON :Membership(start_date);
CREATE INDEX ON :Membership(end_date);
CREATE INDEX ON :Membership(source);

// ============================================================
// INDEXES — Sanction
// ============================================================

CREATE INDEX ON :Sanction(program);
CREATE INDEX ON :Sanction(authority);
CREATE INDEX ON :Sanction(designation_date);
CREATE INDEX ON :Sanction(status);
CREATE INDEX ON :Sanction(sanction_type);
CREATE INDEX ON :Sanction(source);

// ============================================================
// INDEXES — Address
// ============================================================

CREATE INDEX ON :Address(country_iso);
CREATE INDEX ON :Address(city);
CREATE INDEX ON :Address(postal_code);
CREATE INDEX ON :Address(address_type);
CREATE INDEX ON :Address(is_registered_agent);

// ============================================================
// INDEXES — Property
// ============================================================

CREATE INDEX ON :Property(property_type);
CREATE INDEX ON :Property(country_iso);
CREATE INDEX ON :Property(city);
CREATE INDEX ON :Property(title_number);
CREATE INDEX ON :Property(tenure);

// ============================================================
// INDEXES — FinancialAccount
// ============================================================

CREATE INDEX ON :FinancialAccount(account_type);
CREATE INDEX ON :FinancialAccount(institution);
CREATE INDEX ON :FinancialAccount(country_iso);
CREATE INDEX ON :FinancialAccount(currency);
CREATE INDEX ON :FinancialAccount(status);

// ============================================================
// INDEXES — Vessel (corporate-ownership-specific)
// ============================================================

// Note: base Vessel indexes exist in memgraph-init.cypher
CREATE INDEX ON :Vessel(beneficial_owner);
CREATE INDEX ON :Vessel(registered_owner);
CREATE INDEX ON :Vessel(operator);
CREATE INDEX ON :Vessel(group_beneficial_owner);

// ============================================================
// INDEXES — VesselOwnership
// ============================================================

CREATE INDEX ON :VesselOwnership(ownership_type);
CREATE INDEX ON :VesselOwnership(start_date);
CREATE INDEX ON :VesselOwnership(end_date);
CREATE INDEX ON :VesselOwnership(source);
CREATE INDEX ON :VesselOwnership(is_current);

// ============================================================
// INDEXES — PropertyOwnership
// ============================================================

CREATE INDEX ON :PropertyOwnership(ownership_type);
CREATE INDEX ON :PropertyOwnership(start_date);
CREATE INDEX ON :PropertyOwnership(end_date);
CREATE INDEX ON :PropertyOwnership(source);
CREATE INDEX ON :PropertyOwnership(is_current);
CREATE INDEX ON :PropertyOwnership(price_paid);

// ============================================================
// INDEXES — RegistrationRecord
// ============================================================

CREATE INDEX ON :RegistrationRecord(registry);
CREATE INDEX ON :RegistrationRecord(registration_date);
CREATE INDEX ON :RegistrationRecord(status);
CREATE INDEX ON :RegistrationRecord(jurisdiction);
CREATE INDEX ON :RegistrationRecord(record_type);
CREATE INDEX ON :RegistrationRecord(source);
