package ru.polytest;

import org.java_websocket.handshake.ServerHandshake;


public interface BinanceUniHandler {
    
    public void onOpen( ServerHandshake handshakedata );    

    public void onMessage( String message );
    
    public void onClose( int code, String reason, boolean remote );
    

    public void onError( Exception ex );
    
}
