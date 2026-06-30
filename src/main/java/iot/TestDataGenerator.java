package iot;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Standalone test data generator for the IoT Anomaly Detection ETL.
 *
 * Run separately from the ETL/REST API (a different process, after the API is up):
 *
 *   java -cp target/iot-anomaly-detection-1.0-SNAPSHOT.jar iot.TestDataGenerator
 *   java -cp target/iot-anomaly-detection-1.0-SNAPSHOT.jar iot.TestDataGenerator 5
 *
 * Behavior, per customer:
 *   - Every 5 seconds: sends 10 "normal" readings clustered around a per-device baseline.
 *   - Every 30 seconds: sends one additional wild outlier reading, on top of the normal
 *     batch, so a clear anomaly appears in that window.
 *
 * Each customer gets its own device and its own baseline temperature, so this can scale
 * from a single customer to many independent device streams just by raising the
 * customer count argument — no code changes needed.
 */
public class TestDataGenerator {

    private static final String API_URL = "http://localhost:8080/measurements";
    private static final int NORMAL_BATCH_SIZE = 100;
    private static final int NORMAL_INTERVAL_SECONDS = 5;
    private static final int ANOMALY_INTERVAL_SECONDS = 60;

    public static void main(String[] args) throws Exception {
        int customerCount = args.length > 0 ? Integer.parseInt(args[0]) : 1;

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(customerCount * 2);

        for (int i = 0; i < customerCount; i++) {
            String customerId = "cust" + i;
            String deviceId = "dev" + i;
            double baseline = 18.0 + new Random().nextInt(8); // 18-25 C baseline, varies per customer

            CustomerSimulator simulator = new CustomerSimulator(customerId, deviceId, baseline);

            // Every 5s: send a batch of normal readings.
            scheduler.scheduleAtFixedRate(
                    simulator::sendNormalBatch, 0, NORMAL_INTERVAL_SECONDS, TimeUnit.SECONDS);

            // Every 30s: send one anomalous reading on top.
            scheduler.scheduleAtFixedRate(
                    simulator::sendAnomaly, ANOMALY_INTERVAL_SECONDS, ANOMALY_INTERVAL_SECONDS, TimeUnit.SECONDS);
        }

        System.out.println("Test data generator running for " + customerCount + " customer(s).");
        System.out.println("Sending " + NORMAL_BATCH_SIZE + " normal readings every "
                + NORMAL_INTERVAL_SECONDS + "s, one anomaly every " + ANOMALY_INTERVAL_SECONDS + "s per customer.");
        System.out.println("Press Ctrl+C to stop.");

        // Keep the main thread alive; scheduled tasks run on the pool.
        Thread.currentThread().join();
    }

    /** Generates and sends readings for a single customer/device pair. */
    private static class CustomerSimulator {
        private final String customerId;
        private final String deviceId;
        private final double baseline;
        private final Random random = new Random();
        private long tsCounter = System.currentTimeMillis();

        CustomerSimulator(String customerId, String deviceId, double baseline) {
            this.customerId = customerId;
            this.deviceId = deviceId;
            this.baseline = baseline;
        }

        void sendNormalBatch() {
            for (int i = 0; i < NORMAL_BATCH_SIZE; i++) {
                double reading = baseline + random.nextGaussian() * 0.5; // tight cluster, stdDev ~0.5
                send(reading, false);
            }
        }

        void sendAnomaly() {
            double spike = (random.nextBoolean() ? 1 : -1) * (20 + random.nextInt(30))*2; // far outside normal range
            send(baseline + spike, true);
        }

        private void send(double reading, boolean isAnomaly) {
            long ts = tsCounter++;
            String json = String.format(
                    "{\"customerId\":\"%s\",\"deviceId\":\"%s\",\"ts\":%d,\"reading\":%.1f}",
                    customerId, deviceId, ts, reading);

            String tag = isAnomaly ? "[ANOMALY]" : "[normal] ";
            if (isAnomaly)
                System.out.printf("%s sent %s/%s ts=%d reading=%.1f C%n",
                        tag, customerId, deviceId, ts, reading);

            try {
                postJson(json);
            } catch (IOException e) {
                System.err.println("Failed to send reading for " + customerId + "/" + deviceId + ": " + e.getMessage());
            }
        }

        private void postJson(String json) throws IOException {
            URL url = new URL(API_URL);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setConnectTimeout(2000);
            conn.setReadTimeout(2000);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(json.getBytes());
            }
            conn.getResponseCode(); // triggers the request; ignore the response body
            conn.disconnect();
        }
    }
}