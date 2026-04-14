package ru.polytest;

import com.google.gson.Gson;
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
        MarketSubscriptionRequest request = new MarketSubscriptionRequest(assetIds, "market");
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
            if (message.indexOf("@depth") >= 0)
            { // Прилетело обновление стакана
                try {
                    // do smthng
                } catch (Exception e)
                {
                    log.error("Couldn't parse " + message, e);
                }
            } else if (message.indexOf("@kline_") >= 0)
            {
                try {
                    // do smthng
                    //log.info( message );
                } catch (Exception e)
                {
                    log.error("Couldn't parse " + message, e);
                }
            } else
            {
                log.info("unknown message: " + message);
            }
        } catch (Exception e) {
            log.error("Couldn't map object from str: " + message, e);
        }
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

    /** DTO для сообщения подписки на market channel в Polymarket. */
    private static class MarketSubscriptionRequest
    {
        private final List<String> assets_ids;
        private final String type;

        private MarketSubscriptionRequest(List<String> assetsIds, String type)
        {
            this.assets_ids = assetsIds;
            this.type = type;
        }
    }

}
