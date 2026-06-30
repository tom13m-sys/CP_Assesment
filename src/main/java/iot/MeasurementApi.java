package iot;


import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimal REST API standing in for the ingestion tier.
 *
 * POST /measurements   body: {"customerId":"...", "deviceId":"...", "ts":123, "reading":21.5}
 *
 * Responsibilities (kept intentionally simple for local review):
 *   1. Assign apiIngestTs at the earliest possible point on receipt.
 *   2. Validate schema (required fields present and well-formed).
 *   3. Apply basic sanity bounds (reject empty/out-of-range readings).
 *   4. Generate a UUID per accepted request.
 *   5. Publish the resulting Measurement onto the shared queue (local stand-in for Kafka).
 *
 * NOTE: per-tenant rate limiting/circuit breaking, durable persistence for replay,
 * and tier-based topic routing are part of the full design but intentionally
 * omitted here to keep this runnable example small and focused on the data flow
 * that the ETL actually depends on.
 */
public class MeasurementApi {

    private static final double MIN_VALID_READING = -100.0;
    private static final double MAX_VALID_READING = 200.0;

    // Very small hand-rolled JSON field extractor — avoids pulling in a JSON library
    // for this minimal example. Real implementation would use a proper parser.
    private static final Pattern FIELD = Pattern.compile("\"(\\w+)\"\\s*:\\s*\"?([^,\"}]+)\"?");

    private final BlockingQueue<Measurement> outputQueue;

    public MeasurementApi(BlockingQueue<Measurement> outputQueue) {
        this.outputQueue = outputQueue;
    }

    public HttpServer start(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/measurements", this::handle);
        server.start();
        System.out.println("REST API listening on port " + port);
        return server;
    }

    private void handle(HttpExchange exchange) throws IOException {
        // Step 1: capture ingestion time at the earliest possible point.
        long apiIngestTs = System.currentTimeMillis();

        if (!"POST".equals(exchange.getRequestMethod())) {
            respond(exchange, 405, "Only POST is supported");
            return;
        }

        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);

        
        // TODO: check body.length() and abort call if exceeds a pre-determined threshold

        Map<String, String> fields = parseJson(body);

        // Step 2: schema validation — required fields present and well-formed.
        String customerId = fields.get("customerId");
        String deviceId = fields.get("deviceId");
        String tsStr = fields.get("ts");
        String readingStr = fields.get("reading");

        if (isBlank(customerId) || isBlank(deviceId) || isBlank(tsStr) || isBlank(readingStr)) {
            respond(exchange, 400, "Missing required field(s)");
            return;
        }

        long ts;
        double reading;
        try {
            ts = Long.parseLong(tsStr.trim());
            reading = Double.parseDouble(readingStr.trim());
        } catch (NumberFormatException e) {
            respond(exchange, 400, "ts and reading must be numeric");
            return;
        }

        // Step 3: basic sanity bounds — reject garbage/out-of-range readings.
        if (Double.isNaN(reading) || reading < MIN_VALID_READING || reading > MAX_VALID_READING) {
            respond(exchange, 400, "Reading out of valid range");
            return;
        }

        // Step 4: UUID generation for this accepted request.
        String uuid = UUID.randomUUID().toString();

        Measurement measurement = new Measurement(uuid, customerId, deviceId, ts, apiIngestTs, reading);

        // Step 5: publish (local stand-in for: write to durable store, then publish to Kafka
        // keyed by DeviceID@CustomerID, topic chosen by tenant tier).
        outputQueue.offer(measurement);

        respond(exchange, 202, "Accepted: " + uuid + "Queue.size:" + outputQueue.size() + "\n");
    }

    private Map<String, String> parseJson(String body) {
        Map<String, String> fields = new java.util.HashMap<>();
        Matcher m = FIELD.matcher(body);
        while (m.find()) {
            fields.put(m.group(1), m.group(2));
        }
        return fields;
    }

    private boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private void respond(HttpExchange exchange, int status, String message) throws IOException {
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}