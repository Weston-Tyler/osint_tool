// ============================================================
// Memgraph Native Kafka Streams Setup
// Run after: CALL mg.load_all(); (to load transform modules)
// ============================================================

// AIS Positions — high-volume stream, batched for performance
CREATE KAFKA STREAM ais_positions_stream
  TOPICS mda.ais.positions.raw
  TRANSFORM memgraph_transform.ais_position
  BOOTSTRAP_SERVERS "redpanda:9092"
  BATCH_INTERVAL 100
  BATCH_SIZE 1000;

// Sanctions Updates — low-volume, process immediately
CREATE KAFKA STREAM sanctions_stream
  TOPICS mda.sanctions.updates
  TRANSFORM memgraph_transform.sanctions_update
  BOOTSTRAP_SERVERS "redpanda:9092"
  BATCH_INTERVAL 500
  BATCH_SIZE 100;

// UAS Detections — medium-volume
CREATE KAFKA STREAM uas_detections_stream
  TOPICS mda.uas.detections.raw
  TRANSFORM memgraph_transform.uas_detection
  BOOTSTRAP_SERVERS "redpanda:9092"
  BATCH_INTERVAL 200
  BATCH_SIZE 50;

// Start all streams
START STREAM ais_positions_stream;
START STREAM sanctions_stream;
START STREAM uas_detections_stream;
