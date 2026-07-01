package iot.etl;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import iot.data.*;
import iot.gateway.MeasurementApi;

/**
 * IoT temperature anomaly detection ETL.
 *
 * Reflects the agreed design:
 *  - Source: standing in for the per-tier Kafka topic. Locally, an in-memory queue fed
 *    by the REST API (see MeasurementApi / LocalDemo) instead of a real Kafka consumer.
 *  - keyBy: DeviceID@CustomerID (device IDs are not globally unique; this also co-locates
 *    each device's readings for ordering and isolates per-device state).
 *  - Dedup: two checks, both keyed state with TTL —
 *      (a) UUID seen before -> infra-level redelivery (Kafka/producer retries)
 *      (b) TS seen before for this device -> device-level duplicate send
 *    A message dropped by either check never reaches windowing.
 *  - Event time: watermarks driven by apiIngestTs (the API's own clock), NOT the
 *    device-reported ts. Device ts is only ever used for dedup, never for ordering.
 *  - Windowing: 1-minute tumbling event-time window with a grace period for late data.
 *  - Anomaly detection: per-window 2-pass mean/stdDev; readings beyond 3 sigma are flagged.
 *
 * Checkpointing is enabled so this is replay/crash-recovery safe; for this local example
 * the source itself does not implement real replay (see comments on LocalQueueSource).
 */
public class AnomalyDetectionJob {

    private static final double THRESHOLD_SIGMA = 3.0;
    private static final Duration DEDUP_TTL = Duration.ofMinutes(10);

    // -------------------------------------------------------------------------
    // Local source: reads from the in-memory queue the REST API publishes to.
    // Stands in for a Kafka consumer; not replayable (a real Kafka source is
    // what provides the replay guarantee the full design relies on).
    // -------------------------------------------------------------------------
    public static class LocalQueueSource implements SourceFunction<Measurement> {
        // private final BlockingQueue<Measurement> queue;
        static final BlockingQueue<Measurement> QUEUE = new LinkedBlockingQueue<>();
        private volatile boolean running = true;

        // public LocalQueueSource(BlockingQueue<Measurement> queue) {
        //     // this.queue = queue;
        // }

        @Override
        public void run(SourceContext<Measurement> ctx) throws Exception {
            while (running) {
                // Measurement m = this.queue.poll(200, java.util.concurrent.TimeUnit.MILLISECONDS);
                Measurement m = QUEUE.poll(200, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (m != null) {
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(m);
                    }
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // -------------------------------------------------------------------------
    // Dedup: drops a measurement if its UUID or its (device, ts) pair was seen
    // before. Both checks use TTL'd keyed state so memory stays bounded.
    // -------------------------------------------------------------------------
    public static class DedupFunction extends KeyedProcessFunction<String, Measurement, Measurement> {

        private transient MapState<String, Boolean> seenUuids;
        private transient MapState<Long, Boolean> seenTimestamps;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.minutes(DEDUP_TTL.toMinutes()))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) // OnReadAndWrite would reset the TTL time for every contains check
                    .build();

            MapStateDescriptor<String, Boolean> uuidDesc =
                    new MapStateDescriptor<>("seen-uuids", Types.STRING, Types.BOOLEAN);
            uuidDesc.enableTimeToLive(ttlConfig);
            seenUuids = getRuntimeContext().getMapState(uuidDesc);

            MapStateDescriptor<Long, Boolean> tsDesc =
                    new MapStateDescriptor<>("seen-timestamps", Types.LONG, Types.BOOLEAN);
            tsDesc.enableTimeToLive(ttlConfig);
            seenTimestamps = getRuntimeContext().getMapState(tsDesc);
        }

        @Override
        public void processElement(Measurement m, Context ctx, Collector<Measurement> out) throws Exception {
            if (seenUuids.contains(m.uuid)) {
                return; // infra-level redelivery
            }
            if (seenTimestamps.contains(m.ts)) {
                return; // device-level duplicate send
            }
            seenUuids.put(m.uuid, true);
            seenTimestamps.put(m.ts, true);
            out.collect(m);
        }
    }

    // -------------------------------------------------------------------------
    // Per-window anomaly detection: two-pass mean/stdDev, flag readings beyond
    // 3 sigma. Window state (the buffered readings) is managed by Flink and is
    // included in checkpoints.
    // -------------------------------------------------------------------------
    public static class AnomalyWindowFunction
            extends ProcessWindowFunction<Measurement, String, String, TimeWindow> {

        @Override
        public void process(String deviceKey, Context context,
                             Iterable<Measurement> readings, Collector<String> out) {

            List<Measurement> buffer = new ArrayList<>();
            double sum = 0.0;
            double sumSq = 0.0;

            for (Measurement m : readings) {
                buffer.add(m);
                sum += m.reading;
                sumSq += m.reading * m.reading;
            }
            if (buffer.isEmpty()) return;

            double mean = sum / buffer.size();
            double stdDev = Math.sqrt(Math.max(0.0, sumSq / buffer.size() - mean * mean));
            if (stdDev == 0) return; // no deviation possible

            for (Measurement m : buffer) {
                double deviation = Math.abs(m.reading - mean) / stdDev;
                if (deviation > THRESHOLD_SIGMA) {
                    out.collect(String.format(
                            "Device %s, measurement %.1f C, time %d", deviceKey, m.reading, m.ts));
                }
            }
        }
    }

    /**
     * Builds and returns the job's main pipeline (source -> dedup -> window -> anomalies).
     * Kept separate from main() so it can be reused by a real Kafka source later without
     * touching the rest of the topology.
     */
    public static DataStream<String> buildPipeline(StreamExecutionEnvironment env,
                                                     SourceFunction<Measurement> source) {

        DataStream<Measurement> measurements = env
                .addSource(source)
                .name("Measurement-Source")
                .assignTimestampsAndWatermarks(
                    // forMonotonousTimestamps - for ordered arrivals, out of order events are dropped - lowest latency
                    // forBoundedOutOfOrderness - events can arrive out of order, but the disorder is bounded
                    // forGeneratePunctuatedWatermarks - custom logic for watermarks
                        WatermarkStrategy.<Measurement>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // allow 5 seconds for out of order messages (Network or API issues)
                                .withTimestampAssigner(
                                        (SerializableTimestampAssigner<Measurement>)
                                            (m, recordTs) -> m.apiIngestTs).withIdleness(Duration.ofSeconds(15))); // after 15 seconds of no activity the window should be closed and processed

        DataStream<Measurement> deduped = measurements
                .keyBy(Measurement::deviceKey)
                .process(new DedupFunction())
                .name("Dedup");

        return deduped
                .keyBy(Measurement::deviceKey)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.minutes(1)))
                // .window(SlidingEventTimeWindows.of(
                //     org.apache.flink.streaming.api.windowing.time.Time.minutes(1),
                //     org.apache.flink.streaming.api.windowing.time.Time.seconds(30)))
                .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)) // Allow 10 seconds for late arriving events to be included in the window
                .process(new AnomalyWindowFunction())
                .name("Anomaly-Detector");
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // single-threaded for easy local review of output ordering
        env.enableCheckpointing(10_000); // exactly-once by default


        // DataStream<String> anomalies = buildPipeline(env, new LocalQueueSource(queue));
        DataStream<String> anomalies = buildPipeline(env, new LocalQueueSource());
        anomalies.print().name("Anomaly-Sink");

        // Local stand-in for the per-tier Kafka topic: a shared in-memory queue,
        // fed directly by the REST API in this same process.
        // AnomalyDetectionJob.queue = new LinkedBlockingQueue<>();
        // MeasurementApi api = new MeasurementApi(queue);
        MeasurementApi api = new MeasurementApi(LocalQueueSource.QUEUE);
        api.start(8080);


        System.out.println("=== ETL started. POST to http://localhost:8080/measurements ===");
        env.execute("IoT Temperature Anomaly Detection");
    }

}