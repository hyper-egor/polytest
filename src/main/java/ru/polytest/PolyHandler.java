package ru.polytest;

import com.google.gson.JsonParser;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;


@Component
public class PolyHandler implements BinanceUniHandler {
    private static final Logger log = LoggerFactory.getLogger(BinanceFutHandler.class);

    @Autowired
    StakanService stakanService;


    private Set<String> subscriptions = new HashSet<>();
    
    @Autowired
    BinanceService binanceConnector;
    
    private String listenKey = null;
    private long keyUpdateTime = 0;

    JsonParser parser = new JsonParser();
    
    BinanceSocketer socketer = null;

    //@PostConstruct
    public void init()
    {
        createSocket();
    }

    long RETRY_SLEEP_INTERVAL = 2000L;

    ///
    private void createSocket()
    {
        log.info("Trying to create BinanceSocketer for Futures...");
        try {
            int tryCnt = 0;
            while (true)
            {
                tryCnt ++;
                try {
                    String streams = "";
                    
                    for (int i=0; i < instruments.size(); i++)
                    {
                        String symbol = instruments.get(i).toLowerCase();
                        if (streams.length() > 0) streams = streams + "/";
                        streams = streams +
                            symbol + "@depth5@100ms";
                                //symbol + "@kline_1m/" +
                            // symbol + "@trade";
                    }
                    listenKey = binanceConnector.getListenKey( true );
                    if (listenKey == null) throw new Exception("Couldn't recv listenKey");
                    
                    String _url = (!StakanService.DEMO ? "wss://fstream.binance.com/" : "wss://stream.binancefuture.com/")
                            + "stream?streams=" + streams + "/" + listenKey;
                    
                    log.info("URL = " + _url);
                    
                    socketer = new BinanceSocketer( new URI( _url ) );
                    socketer.setHandler( this );
                    socketer.connect();
                    keyUpdateTime = System.currentTimeMillis();
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

    @Scheduled(fixedDelay = 300_000L)       // раз в 5 минут
    private void checkKeyValidity()
    {
        if (listenKey != null)
        {
            if (System.currentTimeMillis() - keyUpdateTime > 3000_000) // Если прошло более 50 минут - обновим
            {
                log.info("refreshing listen key");
                binanceConnector.keepAliveListenKey(true, listenKey);
                keyUpdateTime = System.currentTimeMillis();
            }
        }
    }

    /**  */
    public PolyHandler()
    {
    }

    boolean opened=false;
    
    /**  */
    public void onOpen( ServerHandshake handshakedata )
    {
        opened = true;
        log.info( "Binance fut Connected.");
        for (String s : subscriptions)
        {
            log.info( "Subscription : " + s);
            log.error(" =================== BINANCE FUTURES EXTRA SUBSCRIPTIONS NOT MADE ======================= TODO:.....");
            // this.send("{\"op\": \"subscribe\", \"args\": \"" + s + "\"}");
        }
    }

  //  @Scheduled(fixedDelay = 3000L)
    private void hartBeat()
    {        
        if (!opened) return;
        socketer.send("{\"method\": \"userDataStream.ping\"}");
        log.info("hart beated F");
    }

    long lastDebugTime = System.currentTimeMillis();

    @Override
    public void onMessage( String message )
    {
        if (System.currentTimeMillis() - lastDebugTime > 2000) {
           // log.info("==== Binance Fut update : " + message);
            lastDebugTime = System.currentTimeMillis();
        }
        try {
            if (message.indexOf("@depth") >= 0)
            { // Прилетело обновление стакана
                try {
                  //  log.info("UPDATE" + message);
                    BstakanUpdate update = new BstakanUpdate( parser, message , true);
                    stakanService.updateBinanceStakan( update, true );
                } catch (Exception e)
                {
                    log.error("Couldn't parse " + message, e);
                }
            } else if (message.indexOf("@kline_") >= 0)
            {
                try {
                    StreamCandle candle = new StreamCandle(parser, message);                    
                    stakanService.updateBinanceCandles( candle, true );
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
        stakanService.clearBinanceStakan( true );
        needReconnect = true;
        // createSocket();
    }

    @Override
    public void onError( Exception ex )
    {
        log.error( "Yo-ho-ho", ex );
    }

}
