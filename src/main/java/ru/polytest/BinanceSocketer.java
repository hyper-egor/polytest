package ru.polytest;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BinanceSocketer extends WebSocketClient {
    private static final Logger log = LoggerFactory.getLogger(BinanceSocketer.class);
    
    private BinanceUniHandler handler;
    private String url;

    public BinanceSocketer(URI serverUri) {
        super(serverUri);
        url = serverUri.toString();
    }

    public void setHandler(BinanceUniHandler handler) {
        this.handler = handler;
    }
    
    /**  */
    @Override
    public void onOpen( ServerHandshake handshakedata )
    {        
        log.info( "Connected. " + url);
        handler.onOpen(handshakedata);
    }

    @Override
    public void onMessage( String message )
    {
        handler.onMessage( message );
    }
    
    @Override
    public void onMessage( ByteBuffer bb )
    {        
        try {
            GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream( bb.array() ));
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gis, "UTF-8"));
            String line;
            StringBuffer outStr = new StringBuffer();
            while ((line = bufferedReader.readLine()) != null) {
                outStr.append(line);
            }
            handler.onMessage( outStr.toString() );
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void onClose( int code, String reason, boolean remote ) {
        handler.onClose(code, reason, remote);
    }

    @Override
    public void onError( Exception ex )
    {
        handler.onError( ex );
    }
    
}
