
## How to run:

**Compile**: mvn clean package

**Step1**:
Run the RestAPI + Main ETL jar
`java -jar target/iot-anomaly-detection-1.0-SNAPSHOT.jar`

**Step2**:
Run the test data generator

Generate data for single customer
`java -cp target/iot-anomaly-detection-1.0-SNAPSHOT.jar iot.TestDataGenerator`

Generate data for 5 customers
`java -cp target/iot-anomaly-detection-1.0-SNAPSHOT.jar iot.TestDataGenerator 5`



## REST API — design spec

**Endpoint**: `POST /measurements`
**Input payload**: `{CustomerID, DeviceID, TS, reading}`

**Processing steps, in order:**

1. **APIIngestTS assignment** — captured at the earliest possible point upon receiving the request, before any other processing (validation, tier lookup, etc.). This is the API's own clock, establishing the single trustworthy timestamp the rest of the pipeline will rely on.
2. **Per-tenant resilience controls** — rate limiting and circuit breaking applied keyed on `CustomerID`, so tenant isolation begins at the edge.
3. **Schema validation** — confirms required fields (`CustomerID`, `DeviceID`, `TS`, `reading`) are present and correctly typed. Malformed requests rejected immediately.
4. **Input sanity checks** — reject empty/null readings, readings outside a plausible physical range, and oversized/spam payloads. Failing requests are rejected at the edge; nothing invalid reaches Kafka or the ETL.
5. **Tier lookup** — consult the externally-maintained tenant tier catalog (read-only) using `CustomerID` to determine target Kafka topic.
6. **UUID generation** — a unique message ID assigned to this accepted request.
7. **Durable persistence** — `{UUID, CustomerID, DeviceID, TS, APIIngestTS, reading}` written to a short-term durable store for replayability.
8. **Kafka publish**:
   - **Key**: `DeviceID@CustomerID`
   - **Value**: `{UUID, TS, APIIngestTS, reading}`
   - **Topic**: tier-selected (`measurements-heavy/medium/light`)

## Main ETL — design spec (per tier, 3 identical jobs)

**Source**
Kafka consumer on the tier's topic. Checkpointing enabled in exactly-once mode; offsets committed via checkpoint for consistent replay on failure.

**Watermark strategy**
Assigned at the source, based on `APIIngestTS` — the single trustworthy clock value established at the API. Device `TS` is never used for watermarking.

**keyBy**
`DeviceID@CustomerID`.

**Dedup stage** (two sequential/combined checks, drop on match with either)
- `MapState<UUID, Boolean>` (TTL-bound) — catches infrastructure-level redelivery (Kafka producer/consumer retries).
- `MapState<TS, Boolean>` (TTL-bound) — catches device-level duplicate sends (same device, same internally-claimed `TS`, assumed duplicate regardless of clock correctness).

**Validation/filtering stage**
Stuck-sensor pattern detection (e.g., suspiciously repeated identical values over time) — statistical checks beyond what the API's basic sanity bounds can catch, since this requires historical context per device.

**Event-time windowing**
1-minute window with a grace period for late arrivals; window assignment driven by `APIIngestTS`, not device `TS`. "Late" is defined relative to system ingestion time, consistent with not trusting device clocks for ordering.

**Anomaly detection**
2-pass mean/stdDev computation per window per `DeviceID@CustomerID`, operating on managed window state (`ListState`/accumulator) so it's included in checkpoint snapshots and survives crash/restart consistently.

**Checkpointing**
Exactly-once mode, RocksDB state backend, checkpoint storage on durable external storage. Restart strategy: failure-rate-based with exponential backoff.

**Sink**
Anomalies published to an output Kafka topic, keyed for idempotent downstream consumption (still open for a dedicated design pass).





