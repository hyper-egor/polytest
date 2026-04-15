package ru.polytest;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSyntaxException;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


@Component
public class PolyHandler implements BinanceUniHandler {
    private static final Logger log = LoggerFactory.getLogger(PolyHandler.class);
    private static final Gson GSON = new Gson();

    private final Set<String> subscriptions = new HashSet<>();
    private StakanService stakanService;

    BinanceSocketer socketer = null;

    /** Запускает websocket-подключение к Polymarket. */
    public synchronized void init()
    {
        if (socketer != null && !socketer.isClosed()) {
            log.info("Socket already created, skip duplicated init.");
            return;
        }
        createSocket();
    }

    /** Сохраняет набор токенов, на которые нужно оформить подписку после подключения. */
    public synchronized void setSubscriptions(Set<String> tokenIds)
    {
        subscriptions.clear();
        if (tokenIds == null || tokenIds.isEmpty()) {
            log.info("No token ids for subscription.");
            return;
        }
        subscriptions.addAll(tokenIds);
        log.info("Prepared {} token ids for websocket subscription.", subscriptions.size());
    }

    /** Регистрирует сервис, который будет принимать распарсенные market-data события. */
    public synchronized void setStakanService(StakanService stakanService)
    {
        this.stakanService = stakanService;
    }

    long RETRY_SLEEP_INTERVAL = 2000L;

    ///
    private void createSocket()
    {
        log.info("Trying to create Socketer...");
        try {
            int tryCnt = 0;
            while (true)
            {
                tryCnt ++;
                try {
                    String _url = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
                    
                    log.info("URL = " + _url);
                    
                    socketer = new BinanceSocketer( new URI( _url ) );
                    socketer.setHandler( this );
                    socketer.connect();
                    break;
                } catch (Throwable e)
                {
                    if (tryCnt > 10)
                        RETRY_SLEEP_INTERVAL = 60_000L;
                    log.error("Couldn't create socket, sleeping " + RETRY_SLEEP_INTERVAL + " ms", e);
                    try {
                        Thread.currentThread().sleep(RETRY_SLEEP_INTERVAL);
                    } catch (Throwable t) {}
                }                
            }
            RETRY_SLEEP_INTERVAL = 2000L;
        } catch (Throwable e)
        {            
            e.printStackTrace();
        }        
    }

    private volatile boolean needReconnect = false;
    @Scheduled(fixedDelay = 15_000L)
    private void checkNeedReconnect()
    {
        if (needReconnect)
        {
            createSocket();
            needReconnect = false;
        } else {
            log.info("no need recon");
        }
    }

    public PolyHandler()
    {
    }

    boolean opened=false;
    
    /**  */
    public void onOpen( ServerHandshake handshakedata )
    {
        opened = true;
        log.info( "Poly websocket connected.");

        if (subscriptions.isEmpty()) {
            log.info("No prepared subscriptions for Polymarket websocket.");
            return;
        }

        List<String> assetIds = new ArrayList<>(subscriptions);
        MarketSubscriptionRequest request = new MarketSubscriptionRequest(assetIds, "market", true);
        String payload = GSON.toJson(request);

        log.info("Sending subscription for {} tokens.", assetIds.size());
        socketer.send(payload);
    }

  //  @Scheduled(fixedDelay = 3000L)
    private void hartBeat()
    {        
        if (!opened) return;
        socketer.send("{\"method\": \"userDataStream.ping\"}");
        log.info("hart beated F");
    }

    @Override
    public void onMessage( String message )
    {
        try {
            JsonElement root = com.google.gson.JsonParser.parseString(message);
            if (root == null || root.isJsonNull()) {
                log.info("unknown message: " + message);
                return;
            }

            if (root.isJsonObject()) {
                processEventObject(root.getAsJsonObject(), message);
                return;
            }

            if (root.isJsonArray()) {
                JsonArray events = root.getAsJsonArray();
                for (JsonElement item : events) {
                    if (item == null || !item.isJsonObject()) {
                        log.info("Skip non-object websocket array item: {}", item);
                        continue;
                    }
                    processEventObject(item.getAsJsonObject(), item.toString());
                }
                return;
            }

            log.info("unknown message type: " + message);
        } catch (JsonParseException e) {
            log.error("Couldn't parse websocket message: " + message, e);
        } catch (Exception e) {
            log.error("Couldn't map object from str: " + message, e);
        }
    }

    /** Обрабатывает одно websocket-сообщение в формате JSON-объекта. */
    private void processEventObject(JsonObject eventObject, String rawMessage)
    {
        MarketEventEnvelope envelope = GSON.fromJson(eventObject, MarketEventEnvelope.class);
        if (envelope == null || envelope.event_type == null) {
            log.info("unknown message: " + rawMessage);
            return;
        }

        if ("book".equals(envelope.event_type)) {
            handleBookMessage(rawMessage);
            return;
        }

        if ("price_change".equals(envelope.event_type)) {
            handlePriceChangeMessage(rawMessage);
            return;
        }

        // log.info("Skip unsupported event_type {}: {}", envelope.event_type, rawMessage);
    }

    @Override
    public void onClose( int code, String reason, boolean remote )
    {
        log.info( "!!!!!!!!!!!! =========== Disconnected " + code + reason + remote);
        // TODO: сообщить сервису что прошлоотключение
        opened = false;
        socketer = null;
        needReconnect = true;
        // createSocket();
    }

    @Override
    public void onError( Exception ex )
    {
        log.error( "Yo-ho-ho", ex );
    }

    /**  */
    private void handleBookMessage(String message)
    {
        BookMessage bookMessage = GSON.fromJson(message, BookMessage.class);
        if (bookMessage == null || bookMessage.asset_id == null) {
            log.warn("Skip invalid book message: {}", message);
            return;
        }
        if (stakanService == null) {
            log.warn("StakanService is not registered yet, skip book event.");
            return;
        }

        stakanService.handleBookEvent(
                bookMessage.asset_id,
                bookMessage.market,
                toLevelUpdates(bookMessage.bids),
                toLevelUpdates(bookMessage.asks),
                bookMessage.hash,
                bookMessage.timestamp
        );
    }

    /**  */
    private void handlePriceChangeMessage(String message)
    {
        PriceChangeMessage priceChangeMessage = GSON.fromJson(message, PriceChangeMessage.class);
        if (priceChangeMessage == null || priceChangeMessage.price_changes == null || priceChangeMessage.price_changes.isEmpty()) {
            log.warn("Skip invalid price_change message: {}", message);
            return;
        }
        if (stakanService == null) {
            log.warn("StakanService is not registered yet, skip price_change event.");
            return;
        }

        for (PriceChangeEntry priceChange : priceChangeMessage.price_changes) {
            if (priceChange == null || priceChange.asset_id == null || priceChange.side == null
                    || priceChange.price == null || priceChange.size == null) {
                log.warn("Skip broken price_change entry in message: {}", message);
                continue;
            }

            stakanService.handlePriceChangeEvent(
                    priceChange.asset_id,
                    priceChangeMessage.market,
                    priceChange.side,
                    priceChange.price,
                    priceChange.size,
                    priceChange.hash,
                    priceChangeMessage.timestamp
            );
        }
    }

    /**  */
    private List<Book.LevelUpdate> toLevelUpdates(List<PriceLevel> levels)
    {
        List<Book.LevelUpdate> updates = new ArrayList<>();
        if (levels == null || levels.isEmpty()) {
            return updates;
        }

        for (PriceLevel level : levels) {
            if (level == null || level.price == null || level.size == null) {
                continue;
            }
            updates.add(new Book.LevelUpdate(level.price, level.size));
        }
        return updates;
    }

    /** DTO для сообщения подписки на market channel в Polymarket. */
    private static class MarketSubscriptionRequest
    {
        private final List<String> assets_ids;
        private final String type;
        private final boolean initial_dump;

        private MarketSubscriptionRequest(List<String> assetsIds, String type, boolean initialDump)
        {
            this.assets_ids = assetsIds;
            this.type = type;
            this.initial_dump = initialDump;
        }
    }

    private static class MarketEventEnvelope
    {
        private String event_type;
    }

    private static class BookMessage
    {
        private String market;
        private String asset_id;
        private List<PriceLevel> bids;
        private List<PriceLevel> asks;
        private String hash;
        private String timestamp;
        private String event_type;
    }

    private static class PriceChangeMessage
    {
        private String market;
        private List<PriceChangeEntry> price_changes;
        private String timestamp;
        private String event_type;
    }

    private static class PriceChangeEntry
    {
        private String asset_id;
        private String price;
        private String size;
        private String side;
        private String hash;
        private String best_bid;
        private String best_ask;
    }

    private static class PriceLevel
    {
        private String price;
        private String size;
    }

}
