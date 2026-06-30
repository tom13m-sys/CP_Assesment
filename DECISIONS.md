# Design Decisions

This document summarizes the reasoning behind the IoT Temperature Anomaly Detection
system as it evolved from a single-laptop demo (~2,000 devices, synthetic data, no
fault tolerance) toward a design intended to scale toward a real multi-tenant
production system (~200,000 devices, many tenants, real ingestion, crash recovery).

## 1. What changed from the original ETL

| Original demo | Current design |
|---|---|
| Synthetic in-process data generator | REST API ingestion, decoupled from the ETL via a queue (Kafka stand-in locally) |
| Processing-time windows | Event-time windows, watermarked on `apiIngestTs` |
| No deduplication | Two dedup checks: message UUID (infra redelivery) + device `ts` (device-level duplicate sends) |
| No checkpointing | Checkpointing enabled, exactly-once mode |
| Single flat pipeline | Tiered pipeline concept (Heavy/Medium/Light) for tenant load isolation — *designed but not built into the runnable code* |
| `DeviceID` assumed unique | Composite key `DeviceID@CustomerID`, since device IDs are not globally unique across tenants |
| No input validation | Schema validation + sanity-range checks at the API, before data reaches Kafka/the ETL |
| `print()` sink only | Same — kept deliberately, see Section 3 |

## 2. Trade-offs weighed

**Device `TS` for windowing vs. dedup only.** I initially considered using the
device-reported timestamp to drive event-time windowing, since that's the more
"correct" event-time semantically. I backed away from this once I assumed device
clocks may not be reliable — a skewed device clock could distort watermark
progression for the whole keyed stream. I chose to trust `apiIngestTs` (assigned
once, at a single point I control) for ordering/windowing, and demoted device `TS`
to a narrower, lower-trust role: duplicate-send detection only, where it only needs
to be locally consistent per device, not globally accurate.

**Tiered pipelines vs. per-tenant infrastructure.** For tenant isolation at
~200,000 devices / many tenants, per-tenant Kafka partitions or per-tenant
pipelines don't scale operationally past a few thousand tenants. I chose a
small, fixed number of load-based tiers (Heavy/Medium/Light) as a middle ground —
cheaper to operate than per-tenant isolation, better than a single shared
pipeline. I explicitly accepted that this only solves *cross-tier* isolation,
not *within-tier* fairness, as a deliberate scope trade-off rather than an
oversight.

**Two dedup mechanisms instead of one.** UUID dedup and TS dedup catch different
failure modes (infrastructure redelivery vs. device-side duplicate sends). We
chose to keep both rather than simplify to one, since collapsing them would
silently drop one class of duplicate protection.

**Exactly-once checkpointing vs. simplicity.** I enabled exactly-once
checkpointing from the start rather than deferring it, since retrofitting
correct state management (managed `MapState`/`ListState` instead of local
buffers) after the fact is more disruptive than building on it from day one,
even though it added complexity early.

## 3. What I deliberately did not do, and why

- **No real Kafka in the runnable code.** The local demo uses an in-memory queue
  as a stand-in. This was a scope decision to keep the example runnable without
  external infrastructure; it does **not** provide real replay guarantees, which
  the full design depends on. Explicitly flagged as a known gap in the code's
  comments.

- **No tier routing or multiple ETL jobs in the runnable code.** I designed the
  3-tier concept but built a single pipeline locally. Adding tier routing was
  judged to add complexity without adding learning value to a local demo running
  on one device/customer at a time.

- **No per-tenant rate limiting / circuit breaking in the REST API code.** We
  discussed this as a requirement for real tenant isolation at the edge, but the
  local API is a minimal example focused on the data shape and validation logic,
  not production resilience controls.

- **No durable persistence/replay store in the REST API.** The full design calls
  for persisting `{UUID, payload}` for a retention window before publishing, to
  support replay independent of Kafka retention. Omitted locally as out of scope
  for a runnable demo.

- **No error handling or metrics/alerting design.** Explicitly tabled earlier in
  the discussion as a separate topic, not yet addressed.

- **No idempotent/transactional sink.** Anomalies are printed to stdout. We
  named this early as a structural ceiling — exactly-once end-to-end is not
  achievable with this sink regardless of upstream correctness — and chose not
  to design a transactional sink (e.g., a Kafka producer with two-phase commit)
  since it wasn't necessary to demonstrate the rest of the design.

- **No real JSON library in the REST API.** A minimal regex-based extractor was
  used instead, to avoid adding a dependency for a demo-scale API. Flagged in
  code comments as a simplification.

- **No schema registry / formal schema evolution strategy.** Identified as a
  future need (payload shape will change over time) but not designed, since it
  wasn't blocking the current scope.

## 4. What I'd do next, with another week (For local demo purposes only)


1. **Add support for generic devices** - Currently the ETL only supports a concrete temperature
   device. The ETL needs to be able to accept data and metadata info from
   a wider range of device types.

2. **Add better data filteration**
   Currently the REST API performs a filter out of invalid data such as out 
   of range readings, empty, null readings.
   Add REST API coarse filter (For exmaple payload smaller than 10 chars or 
   larger than 100 can be ignored)
   The REST API should only be doing a coarse filter. Business logic belongs
   in the main ETL.
   (The data filteration needs to be added before the dedup step)

3. **Add Backpressure handling**
3. **Add improved REST API call governance** - Introduce a black list management where corrupted or garbaged devices should
   be ignored (automatic by thresholds or adhoc management)

4. **Validate the dedup TTL and event-time bounds (5s out-of-orderness, 10s
   grace) against real device/network behavior** rather than the placeholder
   values used for local testing — these were chosen for fast local iteration,
   not based on real-world latency data.

5. **Design and implement an idempotent sink** for anomalies (e.g., a Kafka
   producer with a deterministic key, or an upsert-based store), closing the
   exactly-once gap that stdout currently leaves open.

6. **Build out the 3-tier deployment** for real — same job code, three configs,
   three topics — and add the tenant→tier lookup against an external catalog
   (assumed to exist, per earlier scope decision) instead of a hardcoded
   single-tier pipeline.
   Create a batch job that update the tier catalog and determines customer-tier mapping.

   ** Idealy, a real production system should have a dynamic multi-tier management
      system. The customers will automatically be shifted (depending on their thoughput)
      across different tiers and the number of tiers should be increased or reduced
      depending on the overall activity.

7. **Revisit the REST API's resilience controls** (per-tenant rate limiting,
   circuit breaking) and durable persistence for replay, both named in the
   design but not built, since they're necessary for the tenant isolation
   requirement to hold end-to-end rather than just at the ETL layer.


8. **Add the error-handling and alerting design** I explicitly
   deferred — dead-letter handling for malformed/unparseable messages,
   consumer lag monitoring, checkpoint failure alerting, and the operational
   health signals an on-call engineer would need.
