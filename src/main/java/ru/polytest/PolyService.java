package ru.polytest;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.net.ssl.HttpsURLConnection;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class PolyService {
    private static final Logger log = LoggerFactory.getLogger(PolyService.class);
    private static final String GAMMA_API_BASE_URL = "https://gamma-api.polymarket.com/";
    private static final String CLOB_API_BASE_URL = "https://clob.polymarket.com/";
    private static final double DEFAULT_VOLUME_NUM_MIN = 50_000d;
    private static final double DEFAULT_LIQUIDITY_NUM_MIN = 10_000d;

    private final Gson gson = new Gson();

    public List<Market> getAllMarkets() {
        return getAllMarkets(DEFAULT_VOLUME_NUM_MIN, DEFAULT_LIQUIDITY_NUM_MIN);
    }

    public List<Market> getAllMarkets(double volumeNumMin, double liquidityNumMin) {
        log.info("> getAllMarkets ----------");

        List<Market> allMarkets = new ArrayList<>();
        String afterCursor = null;
        int pageNumber = 1;

        while (true) {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("limit", "1000");
            params.put("closed", "false");
            params.put("volume_num_min", formatNumber(volumeNumMin));
            params.put("liquidity_num_min", formatNumber(liquidityNumMin));
            if (afterCursor != null && !afterCursor.isBlank()) {
                params.put("after_cursor", afterCursor);
            }

            try {
                String respJson = executeRequest(GAMMA_API_BASE_URL, "markets/keyset", params, "GET");
                MarketsPageResponse page = gson.fromJson(respJson, MarketsPageResponse.class);

                if (page == null) {
                    log.warn("Polymarket returned empty page object for page {}", pageNumber);
                    break;
                }

                if (page.markets != null && !page.markets.isEmpty()) {
                    for (Market market : page.markets) {
                        if (isTradableMarket(market)) {
                            allMarkets.add(market);
                        }
                    }
                }

                log.info(
                        "Fetched Polymarket markets page {}: {} items, tradable {}",
                        pageNumber,
                        page.markets == null ? 0 : page.markets.size(),
                        page.markets == null ? 0 : page.markets.stream().filter(this::isTradableMarket).count()
                );

                if (page.nextCursor == null || page.nextCursor.isBlank()) {
                    break;
                }

                afterCursor = page.nextCursor;
                pageNumber++;
            } catch (Throwable e) {
                throw new RuntimeException("Couldn't load markets from Polymarket", e);
            }
        }

        log.info(
                "< getAllMarkets total={}, volume_num_min={}, liquidity_num_min={}",
                allMarkets.size(),
                volumeNumMin,
                liquidityNumMin
        );
        return allMarkets;
    }

    public List<Event> getAllEvents() {
        return getAllEvents(DEFAULT_VOLUME_NUM_MIN, DEFAULT_LIQUIDITY_NUM_MIN);
    }

    public List<Event> getAllEvents(double volumeMin, double liquidityMin) {
        log.info("> getAllEvents ----------");

        List<Event> allEvents = new ArrayList<>();
        String afterCursor = null;
        int pageNumber = 1;

        while (true) {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("limit", "500");
            params.put("closed", "false");
            params.put("volume_min", formatNumber(volumeMin));
            params.put("liquidity_min", formatNumber(liquidityMin));
            if (afterCursor != null && !afterCursor.isBlank()) {
                params.put("after_cursor", afterCursor);
            }

            try {
                String respJson = executeRequest(GAMMA_API_BASE_URL, "events/keyset", params, "GET");
                EventsPageResponse page = gson.fromJson(respJson, EventsPageResponse.class);

                if (page == null) {
                    log.warn("Polymarket returned empty events page object for page {}", pageNumber);
                    break;
                }

                if (page.events != null && !page.events.isEmpty()) {
                    for (Event event : page.events) {
                        if (isTradableEvent(event)) {
                            allEvents.add(event);
                        }
                    }
                }

                log.info(
                        "Fetched Polymarket events page {}: {} items, active {}",
                        pageNumber,
                        page.events == null ? 0 : page.events.size(),
                        page.events == null ? 0 : page.events.stream().filter(this::isTradableEvent).count()
                );

                if (page.nextCursor == null || page.nextCursor.isBlank()) {
                    break;
                }

                afterCursor = page.nextCursor;
                pageNumber++;
            } catch (Throwable e) {
                throw new RuntimeException("Couldn't load events from Polymarket", e);
            }
        }

        log.info(
                "< getAllEvents total={}, volume_min={}, liquidity_min={}",
                allEvents.size(),
                volumeMin,
                liquidityMin
        );
        return allEvents;
    }

    public double getFeeRate(String tokenId) {
        if (tokenId == null || tokenId.isBlank()) {
            throw new IllegalArgumentException("tokenId must not be blank");
        }

        try {
            Map<String, String> params = new LinkedHashMap<>();
            params.put("token_id", tokenId);

            String respJson = executeRequest(CLOB_API_BASE_URL, "fee-rate", params, "GET");
            FeeRateResponse response = gson.fromJson(respJson, FeeRateResponse.class);

            if (response == null || response.baseFee == null) {
                throw new IllegalStateException("Polymarket fee-rate response does not contain base_fee");
            }

            return response.baseFee / 10_000d;
        } catch (Throwable e) {
            throw new RuntimeException("Couldn't load fee rate from Polymarket for tokenId=" + tokenId, e);
        }
    }

    /**  */
    private String executeRequest(String baseUrl, String subUrl, Map<String, String> params, String method) throws Throwable {
        String allQuery = buildQuery(params);
        // log.info("Q: {} {}", subUrl, allQuery);

        String urlStr = baseUrl + subUrl;
        if (!allQuery.isEmpty()) {
            urlStr += "?" + allQuery;
        }

        StringBuilder rv = new StringBuilder();
        URL url = new URL(urlStr);

        HttpURLConnection conn;
        if ("https".equalsIgnoreCase(url.getProtocol())) {
            conn = (HttpsURLConnection) url.openConnection();
        } else {
            conn = (HttpURLConnection) url.openConnection();
        }

        conn.setConnectTimeout(60_000);
        conn.setReadTimeout(60_000);
        conn.setUseCaches(false);
        conn.setRequestMethod(method);
        conn.setRequestProperty("Accept", "application/json");
        conn.setDoOutput(false);
        conn.setDoInput(true);

        int httpCode = conn.getResponseCode();
        InputStream stream = httpCode >= 400 ? conn.getErrorStream() : conn.getInputStream();

        if (stream == null) {
            log.warn("Empty HTTP stream from Polymarket: code={}, endpoint={}", httpCode, subUrl);
            return "";
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                rv.append(line);
            }
        }

        if (httpCode >= 400) {
            throw new IllegalStateException("Polymarket HTTP " + httpCode + " for " + subUrl + ": " + rv);
        }

        return rv.toString();
    }

    private String buildQuery(Map<String, String> params) {
        StringBuilder allQuery = new StringBuilder();

        for (Map.Entry<String, String> entry : params.entrySet()) {
            if (allQuery.length() > 0) {
                allQuery.append("&");
            }

            allQuery
                    .append(encode(entry.getKey()))
                    .append("=")
                    .append(encode(entry.getValue()));
        }

        return allQuery.toString();
    }

    private String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private String formatNumber(double value) {
        if (value == Math.rint(value)) {
            return String.valueOf((long) value);
        }
        return String.valueOf(value);
    }

    private boolean isTradableMarket(Market market) {
        return market != null
                && Boolean.TRUE.equals(market.getActive())
                && !Boolean.TRUE.equals(market.getArchived())
                && !Boolean.TRUE.equals(market.getClosed())
                && Boolean.TRUE.equals(market.getAcceptingOrders());
    }

    private boolean isTradableEvent(Event event) {
        return event != null
                && Boolean.TRUE.equals(event.getActive())
                && !Boolean.TRUE.equals(event.getArchived())
                && !Boolean.TRUE.equals(event.getClosed());
    }

    private static class MarketsPageResponse {
        private List<Market> markets;
        @SerializedName("next_cursor")
        private String nextCursor;
    }

    private static class EventsPageResponse {
        private List<Event> events;
        @SerializedName("next_cursor")
        private String nextCursor;
    }

    private static class FeeRateResponse {
        @SerializedName("base_fee")
        private Long baseFee;
    }
}
