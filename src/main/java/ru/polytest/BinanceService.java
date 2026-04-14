package ru.polytest;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import ru.bitok.binance_json.*;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.HttpsURLConnection;
import javax.xml.bind.DatatypeConverter;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * */
@Service
public class BinanceService {
    private static final Logger log = LoggerFactory.getLogger(BinanceService.class);
    
    private JsonParser parser = new JsonParser();

    @Autowired
    private BinanceTradeHandler tradeHandler;

    // egor
    public static String API_KEY = "iaGegb6axOY9NoJ7NUVXNsnSMFcD5OwJdaiOb99Zeu2UoJGBXeDuN1ybOuXbkyhg";
    public static String SKEY = "EEhH8KFidYeiTZQC1wPeex8iFwetncHk0HmctDFTltfr6dNerhvDeBu0K6aT52Vn";

    // mayya
   //  public static String API_KEY = "bX9XaIIhERnXEqmRuHnAdqvTb450pzJhks5aCSn29NMfm3QJSltq35sOrmwVEqEH";
   //  public static String SKEY = "bwqr6vOzl2VfOWzViCfoi8beqx81bWCUOd295QQGaPo17j6RQMyJloq0M4l1dwvE";
  
    /** 'futures' by default */
    public BinOrder sendOrder(BinOrder bo, int digitsAfterDotPrice, int digitsAfterDotLots)
    {
        return sendOrder( bo,  digitsAfterDotPrice,  digitsAfterDotLots, true);
    }
    
    /** 'futures' by default */
    public BinOrder changeOrder(BinOrder bo, int digitsAfterDotPrice, int digitsAfterDotLots, double newPrice)
    {
        return changeOrder( bo,  digitsAfterDotPrice,  digitsAfterDotLots, newPrice, true);
    }
    
    /**  */
    public BinOrder changeOrder(BinOrder bo, int digitsAfterDotPrice, int digitsAfterDotLots, double newPrice, boolean futures)
    {
        log.info("> changeOrder " + bo + " newPrice: " + String.format("%."+ digitsAfterDotPrice +"f", newPrice).replace(',', '.'));
        String orderRespJson = null;
        try {
            Map<String, String> params = new HashMap<>();
            params.put("symbol", bo.getSymbol());
            params.put("side", bo.getSide());
            
            params.put("orderId", "" + bo.getOrderId());
            
            if (bo.getQuantity() != null)
                params.put("quantity", String.format("%."+ digitsAfterDotLots +"f", bo.getQuantity()).replace(',', '.') );
            
            params.put("price", String.format("%."+ digitsAfterDotPrice +"f", newPrice).replace(',', '.') );
            
            if (futures) {
                orderRespJson = postBinance(getFuturesBaseUsrl(), "fapi/v1/order", params, true /* sign */, "PUT", true);      // FUTURES!
            } else {
                orderRespJson = postBinance("https://api.binance.com/", "api/v3/order", params, true /* sign */, "PUT", true);      // FUTURES!
            }
            
            /*
            // v1 format
            {
                "orderId":3148635748,
                "symbol":"BTCUSDT",
                "status":"FILLED",
                "clientOrderId":"cwA8SKmdmImyKyrSn8lVyq",
                "price":"0",
                "avgPrice":"8690.42000",            // реальная цена сделки
                "origQty":"0.001",
                "executedQty":"0.001",
                "cumQty":"0.001",                   // это не размер позиции!
                "cumQuote":"8.69042",
                "timeInForce":"GTC",
                "type":"MARKET",
                "reduceOnly":false,
                "side":"BUY",
                "positionSide":"BOTH",
                "stopPrice":"0",
                "workingType":"CONTRACT_PRICE",
                "origType":"MARKET",
                "updateTime":1588265961468
             } */
            
            /* 
            // v.3 format
            {
    "symbol": "YFIIUSDT",
    "orderId": 831743341,
    "orderListId": -1,
    "clientOrderId": "Pwcf1qXtPknMy0GKndlIU3",
    "transactTime": 1653496348963,
    "price": "0.00000000",
    "origQty": "0.13140000",
    "executedQty": "0.13140000",
    "cummulativeQuoteQty": "100.23600000",
    "status": "FILLED",
    "timeInForce": "GTC",
    "type": "MARKET",
    "side": "BUY",
            "fills": [
    {
      "price": "4000.00000000",
      "qty": "1.00000000",
      "commission": "4.00000000",
      "commissionAsset": "USDT",
      "tradeId": 56
    },
}
            */
            
            
            JsonObject json = parser.parse( orderRespJson ).getAsJsonObject();
            if (json.get("status") != null )
                bo.setStatus( json.get("status").getAsString() );
            if (json.get("executedQty") != null)
                bo.setExecutedQty( json.get("executedQty").getAsDouble() );
            if (json.get("cummulativeQuoteQty") != null)
                bo.setCummulativeQuoteQty( json.get("cummulativeQuoteQty").getAsDouble() );
            if (json.get("code") != null)
                bo.setDelErrCode( json.get("code").getAsInt() );
            bo.setPrice(newPrice);
            log.info("Resp: " + orderRespJson);
        } catch (Throwable e)
        {
            log.error("Couldn't parse order resp from [" + orderRespJson + "]", e);
            return null;
        }        
        return bo;
    }

    public boolean useWebsocket = true;
    
    /**  */
    public BinOrder sendOrder(BinOrder bo, int digitsAfterDotPrice, int digitsAfterDotLots, boolean futures)
    {
        log.info("> sendOrder " + bo);
        String orderRespJson = null;
        try {
            Map<String, String> params = new HashMap<>();
            params.put("symbol", bo.getSymbol());
            params.put("side", bo.getSide());
            params.put("type", bo.getType());
            if (bo.getTimeInForce() != null)
                params.put("timeInForce", bo.getTimeInForce());
            if (bo.getPositionSide() != null)
                params.put("positionSide", bo.getPositionSide());
            
            if (bo.getNewClientOrderId() != null)
                params.put("newClientOrderId", bo.getNewClientOrderId());
            
            if (bo.getQuantity() != null)
                params.put("quantity", String.format("%."+ digitsAfterDotLots +"f", bo.getQuantity()).replace(',', '.') );
            if (BinOrder.TYPE_LIMIT.equals(bo.getType()) || 
                    BinOrder.STOP_LOSS.equals(bo.getType()) ||
                    BinOrder.TAKE_PROFIT.equals(bo.getType()) )
            {
                params.put("price", String.format("%." + digitsAfterDotPrice + "f", bo.getPrice()).replace(',', '.'));
                if (BinOrder.STOP_LOSS.equals(bo.getType()) ||
                    BinOrder.TAKE_PROFIT.equals(bo.getType()) )
                {
                    params.put("triggerprice", String.format("%." + digitsAfterDotPrice + "f", bo.getPrice()).replace(',', '.'));
                }
            }

            if (bo.getReduceOnly() != null) {
                params.put("reduceOnly", bo.getReduceOnly().toString());
            }
            
            if (bo.getStopPrice() != null)
            {
                params.put("stopPrice", String.format("%."+ digitsAfterDotPrice +"f", bo.getStopPrice()).replace(',', '.') );
            }
            
            params.put("newOrderRespType", bo.getNewOrderRespType());            

            if (useWebsocket && tradeHandler.isReady()
                    // && BinOrder.TYPE_MARKET.equals(bo.getType()) // WS работает только для входа по рынку
            )
            {
                return tradeHandler.sendOrder(params,bo);
            }
            log.info("NO WS ORDER PLACEMENT");

            //orderRespJson = postBinance(getFuturesBaseUsrl(), "fapi/v1/order", params, true /* sign */, "POST", true);      // FUTURES!
            
            if (futures) {
                orderRespJson = postBinance(getFuturesBaseUsrl(), "fapi/v1/order", params, true /* sign */, "POST", true);      // FUTURES!
            } else {
                orderRespJson = postBinance("https://api.binance.com/", "api/v3/order", params, true /* sign */, "POST", true);      // FUTURES!
            }
            
            /*
            // v1 format
            {
                "orderId":3148635748,
                "symbol":"BTCUSDT",
                "status":"FILLED",
                "clientOrderId":"cwA8SKmdmImyKyrSn8lVyq",
                "price":"0",
                "avgPrice":"8690.42000",            // реальная цена сделки
                "origQty":"0.001",
                "executedQty":"0.001",
                "cumQty":"0.001",                   // это не размер позиции!
                "cumQuote":"8.69042",
                "timeInForce":"GTC",
                "type":"MARKET",
                "reduceOnly":false,
                "side":"BUY",
                "positionSide":"BOTH",
                "stopPrice":"0",
                "workingType":"CONTRACT_PRICE",
                "origType":"MARKET",
                "updateTime":1588265961468
             } */
            
            /* 
            // v.3 format
            {
    "symbol": "YFIIUSDT",
    "orderId": 831743341,
    "orderListId": -1,
    "clientOrderId": "Pwcf1qXtPknMy0GKndlIU3",
    "transactTime": 1653496348963,
    "price": "0.00000000",
    "origQty": "0.13140000",
    "executedQty": "0.13140000",
    "cummulativeQuoteQty": "100.23600000",
    "status": "FILLED",
    "timeInForce": "GTC",
    "type": "MARKET",
    "side": "BUY",
            "fills": [
    {
      "price": "4000.00000000",
      "qty": "1.00000000",
      "commission": "4.00000000",
      "commissionAsset": "USDT",
      "tradeId": 56
    },
}
            */
            
            
            JsonObject json = parser.parse( orderRespJson ).getAsJsonObject();            
            
            if (json.get("code") != null)
                bo.setCreateErrCode(json.get("code").getAsInt() );
            
            if (json.get("status") != null)
                bo.setStatus( json.get("status").getAsString() );
            
            if (json.get("executedQty") != null)
                bo.setExecutedQty( json.get("executedQty").getAsDouble() );
            if (json.get("cummulativeQuoteQty") != null)
                bo.setCummulativeQuoteQty( json.get("cummulativeQuoteQty").getAsDouble() );
            
            if (json.get("orderId") != null)
                bo.setOrderId( json.get("orderId").getAsString() );
            
            if (json.get("avgPrice") != null && !"0.00".equals(json.get("avgPrice").getAsString())
                    && !BinOrder.TYPE_LIMIT.equals(bo.getType()))
                bo.setPrice( json.get("avgPrice").getAsDouble() );
            // попробуем расчитать среднюю итоговую цену по блоку ответа fills'
            JsonElement fillsObj = json.get("fills");
            if (fillsObj != null) {
                JsonArray fillsArray = fillsObj.getAsJsonArray();
                double totalQty = 0.0;
                double totalPayedPrice = 0.0;
                for (int i = 0; i< fillsArray.size(); i++)
                {
                    JsonObject fillObj = fillsArray.get(i).getAsJsonObject();
                    double fPrice = fillObj.get("price").getAsDouble();
                    double fQty = fillObj.get("qty").getAsDouble();
                    totalQty += fQty;
                    totalPayedPrice += fQty * fPrice;
                    // 2 по 15р
                    // 1 по 14р
                    // 1 по 13р
                }
                if (totalQty > 0.0)
                    bo.setPrice(totalPayedPrice / totalQty);
            }
            log.info("Resp: " + orderRespJson);
        } catch (Throwable e)
        {
            log.error("Couldn't parse order resp from [" + orderRespJson + "]", e);
            return null;
        }        
        return bo;
    }
    
    /**  */
    public BinOrder delOrder(BinOrder bo)
    {
        log.info("> delOrder " + bo);
        String orderRespJson = null;
        try {
            Map<String, String> params = new HashMap<>();
            //params.put("symbol", bo.getSymbol()); // for old ver.
            //params.put("orderId", ""+bo.getOrderId()); // old version
            params.put("algoid", ""+bo.getOrderId()); // new 'algo' version
            
            //orderRespJson = postBinance(getFuturesBaseUsrl(), "fapi/v1/order", params, true /* sign */, "DELETE", true);      // FUTURES! old ver
            orderRespJson = postBinance(getFuturesBaseUsrl(), "fapi/v1/algoOrder", params, true /* sign */, "DELETE", true); // new 'algo' ver
            log.info("DEL RESP : " + orderRespJson);
/*          {
                "orderId":3148635748,
                "symbol":"BTCUSDT",
                "status":"FILLED",
                "clientOrderId":"cwA8SKmdmImyKyrSn8lVyq",
                "price":"0",
                "avgPrice":"8690.42000",            // реальная цена сделки
                "origQty":"0.001",
                "executedQty":"0.001",
                "cumQty":"0.001",                   // это не размер позиции!
                "cumQuote":"8.69042",
                "timeInForce":"GTC",
                "type":"MARKET",
                "reduceOnly":false,
                "side":"BUY",
                "positionSide":"BOTH",
                "stopPrice":"0",
                "workingType":"CONTRACT_PRICE",
                "origType":"MARKET",
                "updateTime":1588265961468
             }  */
            JsonObject json = parser.parse( orderRespJson ).getAsJsonObject();
            if (json.get("code") != null)
                bo.setDelErrCode( json.get("code").getAsInt() );
            if (json.get("status") != null)
                bo.setStatus( json.get("status").getAsString() );
            if (json.get("executedQty") != null)
                bo.setExecutedQty( json.get("executedQty").getAsDouble() );
            if (json.get("cumQty") != null)
                bo.setCummulativeQuoteQty( json.get("cumQty").getAsDouble() );            
            if (json.get("avgPrice") != null)
                bo.setPrice( json.get("avgPrice").getAsDouble() );
        } catch (Throwable e)
        {
            log.error("Couldn't parse order resp from [" + orderRespJson + "]", e);
            return null;
        }
        return bo;
    }
    
    /**  */
    private String getFuturesBaseUsrl()
    {
        return StakanService.DEMO ? "https://testnet.binancefuture.com/" : "https://fapi.binance.com/";
    }
    
    /**  */
    public String _delAllOrders(String symbol)
    {
        log.info("> delAllOrders " + symbol);
        String json = null;
        try {
            Map<String, String> params = new HashMap<>();
            params.put("symbol", symbol);            
            json = postBinance(getFuturesBaseUsrl(), "fapi/v1/allOpenOrders", params, true /* sign */, "DELETE", true);      // FUTURES!
/*          {
    "code": "200", 
    "msg": "The operation of cancel all open order is done."
}  */
            log.info("RESP: " + json);
        } catch (Throwable e)
        {
            log.error("Couldn't del all orders for " + symbol, e);
            return null;
        }
        return json;
    }
    
    /** Получить ключик для Websocket c данными по аккаунту  */
    public String getListenKey(boolean futures)
    {
        log.info("> getListenKey ---------- futures " + futures);
        String rv = null;
        try {
            Map<String, String> params = new HashMap<>();
            
            String respJson;
            if (futures)
            {
                respJson = postBinance(getFuturesBaseUsrl(), "fapi/v1/listenKey", params, true, "POST", futures);      // FUTURES!
            } else
            {
                respJson = postBinance("https://api.binance.com/", "api/v3/userDataStream", params, false, "POST", futures);      // REAL BITOK
            }            
/*          {
                "listenKey": "pqia91ma19a5s61cv6a81va65sdf19v8a65a1a5s61cv6a81va65sdf19v8a65a1"
            }                   */
            
            log.info( respJson );
            JsonObject jo = parser.parse(respJson).getAsJsonObject();
            rv = jo.get("listenKey").getAsString();
        } catch (Throwable e)
        {
            log.error("Couldn't parse getListenKey reply ", e);
        }
        return rv;
    }
    
    /** Получить ключик для Websocket c данными по аккаунту  */
    public void keepAliveListenKey(boolean futures, String key)
    {
        log.info("> getListenKey ---------- ");
        String rv = null;
        try {
            Map<String, String> params = new HashMap<>();
            String respJson;
            if (futures)
            {
                respJson = postBinance(getFuturesBaseUsrl(), "fapi/v1/listenKey", params, true, "PUT", futures);      // FUTURES!
            } else
            {
                respJson = postBinance("https://api.binance.com/", "api/v1/listenKey", params, true, "PUT", futures);      // REAL BITOK
            }            
            log.info( "keepAlive Key rv: " + respJson );            
        } catch (Throwable e)
        {
            log.error("Zzzz....", e);
        }
    }

    /**  */
    public Bstakan getStakan(boolean futures, String symbol)
    {
        log.info("> getStakan ---------- ");
        try {
            Map<String, String> params = new HashMap<>();
            params.put("limit", "10");
            params.put("symbol", symbol.toUpperCase());
            
            String stakanJson;
            if (futures)
            {
                stakanJson = postBinance(getFuturesBaseUsrl(), "fapi/v1/depth", params, false, "GET", futures);      // FUTURES!
            } else
            {
                stakanJson = postBinance("https://api.binance.com/", "api/v3/depth", params, false, "GET", futures);      // REAL BITOK
            }

            Bstakan stak = null;

            log.info("Get stakan = " + stakanJson);

            try {
                stak = new Bstakan(parser, stakanJson, symbol);
            } catch (Exception e)
            {
                log.info("Exceptio parsing json " + stakanJson);
                e.printStackTrace();
            }
            
            //p ("Got stakan last Update:  " + stak.lastUpdateId);
            //p ("BIDS----------------");

            return stak;
        } catch (Throwable e)
        {
            log.info("Couldn't parse Stakan ", e);
            e.printStackTrace();
        }
        return null;
    }
    
    /**  */
    public Set<Position> getPositions(boolean futures) throws Throwable
    {
        Set<Position> rv = new HashSet<>();

        String json;
        if (futures) {
            json = postBinance(getFuturesBaseUsrl(), "fapi/v2/positionRisk", new HashMap<>(), true, "GET", futures);      // FUTURES!
        } else {
            json = postBinance("https://api.binance.com/", "api/v2/positionRisk", new HashMap<>(), true, "GET", futures);      // REAL BITOK
        }

        //log.info(json);
        JsonArray positionsArray = parser.parse(json).getAsJsonArray();
        for (int i = 0; i < positionsArray.size(); i++) {
            JsonObject posObj = positionsArray.get(i).getAsJsonObject();
            Position p = new Position();
            p.symbol = posObj.get("symbol").getAsString();
            p.positionAmt = posObj.get("positionAmt").getAsDouble();
            p.unRealizedProfit = posObj.get("unRealizedProfit").getAsDouble();
            p.positionSide = posObj.get("positionSide").getAsString();
            p.entryPrice = posObj.get("entryPrice").getAsDouble();
            rv.add(p);
        }
        //log.info("< getPositions " + rv.size() + " elems");

        return rv;
    }

    /**  */
    public Double getBalance(String asset, boolean futures) throws Throwable
    {
        Double rv = null;

        String json;
        if (futures) {
            json = postBinance(getFuturesBaseUsrl(), "fapi/v2/balance", new HashMap<>(), true, "GET", futures);      // FUTURES!
        } else {
            json = postBinance("https://api.binance.com/", "api/v2/balance", new HashMap<>(), true, "GET", futures);      // REAL BITOK
        }

        log.info(json);
        JsonArray positionsArray = parser.parse(json).getAsJsonArray();
        for (int i = 0; i < positionsArray.size(); i++) {
            JsonObject posObj = positionsArray.get(i).getAsJsonObject();
            String symbol = posObj.get("asset").getAsString();
            double balance = posObj.get("balance").getAsDouble();
            if (asset.equals(symbol))
                return balance;
        }
        log.info("< Balance " + rv);
        return rv;
    }
    
    
    /**  */
    public Map<String, SymbolInfo> getSymbolInfo(boolean futures)
    {
        Map<String, SymbolInfo> rv = new ConcurrentHashMap<>();
        try {   
            String json;
            if (futures) {
                json = postBinance(getFuturesBaseUsrl() , "fapi/v1/exchangeInfo", new HashMap<>(), false, "GET", futures);
            } else {
                json = postBinance("https://api.binance.com/", "api/v1/exchangeInfo", new HashMap<>(), false, "GET", futures);
            }
         //   log.info("LLLLLLLLLLLLLLLL " + json);
            JsonObject jo = parser.parse( json ).getAsJsonObject();
            JsonArray symbols = jo.get("symbols").getAsJsonArray();
            for (int i=0; i<symbols.size(); i++)
            {
                JsonObject symbInfo = symbols.get(i).getAsJsonObject();
                String symbol = symbInfo.get("symbol").getAsString();
                String status = symbInfo.get("status").getAsString();
                if (!"TRADING".equals(status) || (!symbol.endsWith("USDT") /*&& !symbol.endsWith("BUSD")*/))
                {
                    log.info("Symbol " + symbol + " not trading : skip");
                    continue;
                }
                SymbolInfo si = new SymbolInfo(symbol);                
                JsonArray filterArray = symbInfo.get("filters").getAsJsonArray();
                for (int j=0; j < filterArray.size(); j++)
                {
                    JsonObject filter = filterArray.get(j).getAsJsonObject();
                    String filterType = filter.get("filterType").getAsString();
                    if ("LOT_SIZE".equals(filterType))
                    {
                        si.minQty = filter.get("minQty").getAsDouble();
                    } else if ("PRICE_FILTER".equals(filterType))
                    {
                        si.tickSize = filter.get("tickSize").getAsDouble();                        
                    }
                }
                caltNumDigitsAfterDot( si );
                log.info( "" + si );
                rv.put(si.symbol, si);
            }
        } catch (Throwable e) {
            log.error("cant get binance symb info " + e.getMessage(), e);
        }
        return rv;
    }


    public volatile double ALL_MARKET_24H_VOL = 0.0;
    /**  
     * Возвращает список символов - вначале малые обьемы, потом большие
     */
    public List<SymbolInfo> get24hrStat(boolean futures, Map<String, SymbolInfo> sInfos, double min24hrVolume)
    {
        List<SymbolInfo> sortedSymbols = new ArrayList<>();
        try {   
            String json;
            if (futures) {
                json = postBinance(getFuturesBaseUsrl() , "fapi/v1/ticker/24hr", new HashMap<>(), false, "GET", futures);
            } else {
                json = postBinance("https://api.binance.com/", "api/v1/ticker/24hr", new HashMap<>(), false, "GET", futures);
            }
            log.info(" ====" + json);
            
            JsonArray symbols = parser.parse( json ).getAsJsonArray();
            double allmarketVol = 0;
            for (int i=0; i < symbols.size(); i++)
            {
                JsonObject symbInfo = symbols.get(i).getAsJsonObject();
                String symbol = symbInfo.get("symbol").getAsString();
                Double volume = symbInfo.get("volume").getAsDouble();
                Double weightedAvgPrice = symbInfo.get("weightedAvgPrice").getAsDouble();
                double volumeUsd = volume * weightedAvgPrice;
                allmarketVol += volumeUsd;
                SymbolInfo si = sInfos.get(symbol);
                if (si != null)
                {
                    si.volumes24h = volumeUsd;
                    log.info("Symb " + symbol + " VOLUME: " + String.format("%.2f", si.volumes24h));
                    /*
                    if (si.volumes24h < min24hrVolume)
                    {
                        log.info(" low volumes - remove from list");
                        sInfos.remove( symbol );
                    } else
                    */
                    {
                        boolean inserted = false;
                        for (int j = 0; j < sortedSymbols.size(); j++) {
                            SymbolInfo sij = sortedSymbols.get( j );
                            if (si.volumes24h < sij.volumes24h) {
                                sortedSymbols.add(j, si);
                                inserted = true;
                                break;
                            }
                        }
                        if (!inserted) {
                            sortedSymbols.add(si);
                        }
                    }
                }
            }
            ALL_MARKET_24H_VOL = allmarketVol;
        } catch (Throwable e) {
            log.error("cant get binance symb info " + e.getMessage(), e);
        }
        return sortedSymbols;
    }

    /**  */
    public Map<String, Double> get24hrStat(String jsonData)
    {
        Map<String, Double> rv = new HashMap<>();
        try {
            JsonArray symbols = parser.parse( jsonData ).getAsJsonArray();
            double all = 0;
            for (int i=0; i < symbols.size(); i++)
            {
                JsonObject symbInfo = symbols.get(i).getAsJsonObject();
                String symbol = symbInfo.get("symbol").getAsString();
                Double volume = symbInfo.get("volume").getAsDouble();
                Double weightedAvgPrice = symbInfo.get("weightedAvgPrice").getAsDouble();
                double vol24h = volume * weightedAvgPrice;
                all += vol24h;
                rv.put(symbol, vol24h);
            }
            rv.put("ALL_MARKET", all);
        } catch (Throwable e) {
            log.error("cant get binance symb info " + e.getMessage(), e);
        }
        return rv;
    }

    /**
     * Получаем возможные плечи по символам, фильтруем символы по минимально допустимому плечу
     * и по минимальному лимиту позиции (notionalCap), затем переключаем плечо на максимально
     * возможное значение, которое обеспечивает требуемый объем позиции.
     */
    public Map<String, SymbolInfo> leverageFilter(boolean futures, Map<String, SymbolInfo> sInfos,
                                                  int minLeverage,
                                                  double minRequiredPosUSDT)
    {
        // Минимальный размер позиции в USDT, который должен быть доступен на выбранном плече.
        final double MIN_REQUIRED_POSITION_USDT = minRequiredPosUSDT; // 5000

        try {
            String json;
            if (futures) {
                json = postBinance(getFuturesBaseUsrl(), "fapi/v1/leverageBracket", new HashMap<>(), true, "GET", futures);
            } else {
                json = postBinance("https://api.binance.com/", "api/v1/leverageBracket", new HashMap<>(), true, "GET", futures);
            }

            JsonArray symbols = parser.parse(json).getAsJsonArray();
            for (int i = 0; i < symbols.size(); i++)
            {
                JsonObject symbInfo = symbols.get(i).getAsJsonObject();
                String symbol = symbInfo.get("symbol").getAsString();

                SymbolInfo si = sInfos.get(symbol);
                if (si == null) {
                    continue;
                }

                JsonArray allBrackets = symbInfo.get("brackets").getAsJsonArray();
                si.leverages.clear();

                int maxLeverage = 0;
                int previousLeverage = Integer.MAX_VALUE;
                boolean orderedFromHighToLow = true;
                Leverage selectedLeverage = null;

                for (int j = 0; j < allBrackets.size(); j++)
                {
                    JsonObject bracket = allBrackets.get(j).getAsJsonObject();
                    int leverage = bracket.get("initialLeverage").getAsInt();
                    double maxTradeUSDT = bracket.get("notionalCap").getAsDouble();

                    Leverage leverageInfo = new Leverage(leverage, maxTradeUSDT);
                    si.leverages.add(leverageInfo);

                    if (leverage > maxLeverage) {
                        maxLeverage = leverage;
                    }

                    // Проверяем ожидаемый порядок из Binance: от большего плеча к меньшему.
                    if (leverage > previousLeverage) {
                        orderedFromHighToLow = false;
                    }
                    previousLeverage = leverage;

                    // Выбираем максимальное плечо, которое удовлетворяет двум условиям:
                    // 1) не ниже минимально допустимого плеча,
                    // 2) notionalCap не ниже требуемого минимального объема позиции.
                    if (leverage >= minLeverage && maxTradeUSDT >= MIN_REQUIRED_POSITION_USDT) {
                        if (selectedLeverage == null || leverage > selectedLeverage.leverage) {
                            selectedLeverage = leverageInfo;
                        }
                    }
                }

                if (!orderedFromHighToLow) {
                    log.info("Binance leverage brackets order is not descending for symbol {}", symbol);
                }

                if (maxLeverage < minLeverage) {
                    log.info("Skip {}: max leverage {} is smaller than required {}", symbol, maxLeverage, minLeverage);
                    sInfos.remove(symbol);
                    continue;
                }

                if (selectedLeverage == null) {
                    log.info("Skip {}: no leverage >= {} with notionalCap >= {}", symbol, minLeverage, MIN_REQUIRED_POSITION_USDT);
                    sInfos.remove(symbol);
                    continue;
                }

                changeLeverage(symbol, selectedLeverage.leverage, futures);

                // После выбора/переключения оставляем в symbol info только активное плечо.
                si.leverages.clear();
                si.leverages.add(selectedLeverage);

                log.info("Use {} leverage {} with max trade {}", symbol, selectedLeverage.leverage, selectedLeverage.maxUSDTtrade);
            }

            Map<Integer, List<Double>> maxTradeStatByLeverage = new HashMap<>();
            for (SymbolInfo si : sInfos.values())
            {
                if (si.leverages.isEmpty()) {
                    continue;
                }

                Leverage leverage = si.leverages.get(0);
                List<Double> stat = maxTradeStatByLeverage.get(leverage.leverage);
                if (stat == null) {
                    stat = new ArrayList<>();
                    maxTradeStatByLeverage.put(leverage.leverage, stat);
                }
                stat.add(leverage.maxUSDTtrade);
            }

            for (Integer leverage : maxTradeStatByLeverage.keySet())
            {
                List<Double> stats = maxTradeStatByLeverage.get(leverage);
                int count = 0;
                double sum = 0;
                for (Double maxTrade : stats)
                {
                    count++;
                    sum += maxTrade;
                }
                log.info(" --- leverage {} cnt: {} avg max trade: {}", leverage, count, String.format("%.1f", (sum / count)));
            }
        } catch (Throwable e) {
            log.error("cant get binance symb info " + e.getMessage(), e);
        }
        return sInfos;
    }

    /**  */
    public void changeLeverage(String symbol, int leverage, boolean futures)
    {
        log.info("> changeLeverage " + symbol + " lev: " + leverage);
        String resp = null;
        try {
            Map<String, String> params = new HashMap<>();
            params.put("symbol", symbol);
            params.put("leverage", "" + leverage);

            if (futures) {
                resp = postBinance(getFuturesBaseUsrl(), "fapi/v1/leverage", params, true /* sign */, "POST", true);      // FUTURES!
            } else {
                resp = postBinance("https://api.binance.com/", "api/v1/leverage", params, true /* sign */, "POST", true);      // FUTURES!
            }
            log.info("Resp: " + resp);
        } catch (Throwable e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Вызывает подписанный Binance API метод POST /fapi/v1/stock/contract без параметров.
     * Возвращает сырой ответ Binance в виде строки.
     */
    public String postFuturesStockContract()
    {
        log.info("> postFuturesStockContract");
        String resp = null;
        try {
            Map<String, String> params = new HashMap<>();
            resp = postBinance(getFuturesBaseUsrl(), "fapi/v1/stock/contract", params, true /* sign */, "POST", true);
            log.info("Resp: " + resp);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return resp;
    }

    /**  */
    public Map<String, SymbolInfo> filterSymbols(Map<String, SymbolInfo> sInfos, String allowedSymbols)
    {
        Map<String, SymbolInfo> rv = new HashMap<>();
        try {   
            StringTokenizer st = new StringTokenizer(allowedSymbols, ", ");
            Set<String> ss = new HashSet<>();
            while (st.hasMoreTokens())
            {
                ss.add( st.nextToken() );
            }            
            for (String key : sInfos.keySet())
            {
                if (ss.contains(key))
                    rv.put(key, sInfos.get(key));
            }
        } catch (Throwable e) {
            log.error("y " + e.getMessage(), e);
        }
        return rv;
    }
    
    /**  */
    private void caltNumDigitsAfterDot(SymbolInfo si)
    {
        double minPrice = si.tickSize;
        int digitsAfterDotPrice = 0;
        while (minPrice < 1)
        {
            digitsAfterDotPrice++;
            minPrice = minPrice * 10;
        }
        si.setDigitsAfterDotPrice(digitsAfterDotPrice);
        double minLots = si.minQty;
        while (minLots < 1)
        {
            si.digitsAfterDotLots++;
            minLots = minLots * 10;
        }
    }
    
    /**
     * LEGACY-путь обновления стакана (дифф-логика add/update/delete).
     * Оставлен как fallback для совместимости и возможного возврата на старую схему.
     */
    public void updateStakan(StakanContainer sc, BstakanUpdate update)
    {

        // 4. Drop any event where u is < lastUpdateId in the snapshot.
        //if (update.lastUpdateId /* u */ < sc.prevBinanceEventID)
          //  return;

        //sc.prevBinanceEventID = update.lastUpdateId; // 'u' тут нет ошибки. предыдушее значение должны быть равно тому которое передано как последнее

       // if (update.firstUpdateId /* U */ <= sc.prevBinanceEventID && update.lastUpdateId /* u */ >= sc.prevBinanceEventID)
       // {
     //       // good update!
   //     } else {
    //        log.info("bad update.. "+ sc.symbol);
  //          return;
//        }

        for (int i=0; i < update.bids.size(); i++)
        {
            PriceQty p = update.bids.get(i);
          
            PriceQty oldP = sc.bstakan.bidsMap.get( p.price );
            if (oldP != null)
            {
                if (p.qty > 0.0)
                {
                    oldP.qty = p.qty;
                } else
                    sc.bstakan.deleteLine(true, p);

            } else if (p.qty > 0.0)
            {
                sc.bstakan.addStakanRecord(true, p);
            }
        }
        
        for (int i=0; i < update.asks.size(); i++)
        {
            PriceQty p = update.asks.get(i);
            
            PriceQty oldP = sc.bstakan.asksMap.get( p.price );
            if (oldP != null)
            {
                if (p.qty > 0.0)
                {
                    oldP.qty = p.qty;
                } else
                    sc.bstakan.deleteLine(false, p);

            } else if (p.qty > 0.0)
            {
                sc.bstakan.addStakanRecord(false, p);
            }
        }
    }

    public long serverTime = 0;
    private long serverTimeDelta = 0;

    /**  */
    public long getServerTime(boolean futures)
    {
        try {
            String rv;
            if (futures) {
                rv = postBinance(getFuturesBaseUsrl(), "fapi/v1/time", new HashMap<>(), false, "GET", futures);
            } else {
                rv = postBinance("https://api.binance.com/", "api/v1/time", new HashMap<>(), false, "GET", futures);
            }
            JsonObject jo = parser.parse(rv).getAsJsonObject();
            serverTime = jo.get("serverTime").getAsLong();
            serverTimeDelta = serverTime - System.currentTimeMillis();
            return serverTime;
        } catch (Throwable e) {
            log.error("cant get binance tiome " + e.getMessage(), e);
        }
        return 0;
    }

    /**  */
    public synchronized List<CandleResp> getCandles(boolean futures, CandleRequest req) throws Exception
    {
        List<CandleResp> rv = new ArrayList<>();
        Map<String, String> params = new HashMap<>();
        params.put("symbol", req.symbol);
        params.put("interval", req.interval);

        if (req.startTime != 0)            
            params.put("startTime", "" + req.startTime);
        if (req.endTime != 0)
            params.put("endTime", "" + req.endTime);
        params.put("limit", "" + req.limit);
        
        try {
            String json;
            if (futures) {
                json = postBinance(getFuturesBaseUsrl(), "fapi/v1/klines", params, false, "GET", futures);
            } else {
                json = postBinance("https://api.binance.com/", "api/v3/klines", params, false, "GET", futures);
            }
            if (json == null || json.isBlank()) {
                log.warn("Пустой ответ Binance для свечей: symbol={}, interval={}, futures={}", req.symbol, req.interval, futures);
                return rv;
            }
            if (json.indexOf("Invalid symbol") > 0)
                throw new Exception("Invalid symbol");

            JsonArray candlArr = parser.parse( json ).getAsJsonArray();
            for (int i = 0; i<candlArr.size(); i++)
            {
                JsonArray candleJ = candlArr.get(i).getAsJsonArray();
                CandleResp cr = new CandleResp();
                cr.openTime = candleJ.get( 0 ).getAsLong();                
                cr.o = candleJ.get( 1 ).getAsDouble();
                cr.h = candleJ.get( 2 ).getAsDouble();
                cr.l = candleJ.get( 3 ).getAsDouble();
                cr.c = candleJ.get( 4 ).getAsDouble();
                cr.vol = candleJ.get( 5 ).getAsDouble();
                cr.q_asset_vol = candleJ.get( 7 ).getAsDouble();
                cr.num_trades = candleJ.get( 8 ).getAsInt();
                cr.taker_buy_base_asset_vol = candleJ.get( 9 ).getAsDouble();
                cr.taker_sell_base_asset_vol = cr.vol - cr.taker_buy_base_asset_vol;
                //cr.taker_buy_q_asset_vol = candleJ.get( 10 ).getAsDouble();
                rv.add(cr);
            }                        
            return rv;
        } catch (Throwable e) {
            if (e.getMessage().equals("Invalid symbol")) throw (Exception) e;
            log.error("cant get binance candles " + e.getMessage(), e);
            return rv;
        }
    }

    Map<String, Double> lastPriceCache = new HashMap<>();

    long prevReq = 0;
    /**  */
    public synchronized double getLastPrice(boolean futures, String symbol, long timeTo)
    {
        String key = symbol + timeTo;
        Double r = lastPriceCache.get(key);
        if (r != null) return r;

        long curTime = System.currentTimeMillis();
        if (curTime - prevReq < 502) {
            try {
                Thread.currentThread().sleep(503 - (curTime - prevReq));
            } catch (Exception e) {}
        }
        prevReq = System.currentTimeMillis();

        Map<String, String> params = new HashMap<>();
        params.put("symbol", symbol);
        params.put("endTime", "" + timeTo); // long ts
        params.put("limit", "1");
        try {
            String json;
            if (futures) {
                json = postBinance(getFuturesBaseUsrl(), "fapi/v1/aggTrades", params, false, "GET", futures);
            } else {
                json = postBinance("https://api.binance.com/", "api/v1/aggTrades", params, false, "GET", futures);
            }
            if (json == null || json.isBlank()) {
                log.warn("Пустой ответ Binance: symbol={}, timeTo={}", symbol, timeTo);
                lastPriceCache.put(key, Double.NaN);
                return Double.NaN;
            }
            JsonArray candlArr = parser.parse( json ).getAsJsonArray();
            double rv = Double.NaN;
            for (int i = 0; i<candlArr.size(); i++)
            {
                JsonObject aggTrade = candlArr.get(i).getAsJsonObject();
                rv = aggTrade.get("p").getAsDouble();
            }
            lastPriceCache.put(key, rv);
            return rv;
        } catch (Throwable e) {
            log.error("cant get binance aggTrade " + e.getMessage(), e);
            lastPriceCache.put(key, Double.NaN);
            return Double.NaN;
        }
    }

    /**  */
    public synchronized List<LongShortRatio> getLongShortRatio(String symbol, int periodMinutes, int limit, long startTime)
    {
        // limit MAX = 500
        Map<String, String> params = new HashMap<>();
        params.put("symbol", symbol);
        params.put("period", periodMinutes + "m");
        //params.put("startTime", "" + startTime);
        if (startTime > 0) {
            long endTime = startTime + limit * periodMinutes * 60_000L;
            params.put("endTime", "" + endTime);
        }
        params.put("limit", "" + limit);
        try {
            String json = postBinance(getFuturesBaseUsrl(), "futures/data/topLongShortPositionRatio", params, false, "GET", true);
            //log.info("~~" + json);
            JsonArray data = parser.parse( json ).getAsJsonArray();
            List<LongShortRatio> rv = new ArrayList<>();
            for (int i = 0; i < data.size(); i++)
            {
                JsonObject obj = data.get( i ).getAsJsonObject();
                LongShortRatio lsr = new LongShortRatio();
                lsr.longAccount = obj.get("longAccount").getAsDouble();
                lsr.shortAccount = obj.get("shortAccount").getAsDouble();
                lsr.timestamp = obj.get("timestamp").getAsLong();
                rv.add(lsr);
            }
            return rv;
        } catch (Throwable e) {
            log.error("Can't get binance long/short ratio for " + symbol + " " + e.getMessage(), e);
            return null;
        }
    }

    /**  */
    public synchronized Map<Long, LongShortRatio> getLongShortRatioAsMap(String symbol, int periodMinutes, int limit, long startTime)
    {
        // limit MAX = 500
        Map<String, String> params = new HashMap<>();
        params.put("symbol", symbol);
        params.put("period", periodMinutes + "m");
        //params.put("startTime", "" + startTime);
        if (startTime > 0) {
            long endTime = startTime + limit * periodMinutes * 60_000L;
            params.put("endTime", "" + endTime);
        }
        params.put("limit", "" + limit);
        try {
            String json = postBinance(getFuturesBaseUsrl(), "futures/data/topLongShortPositionRatio", params, false, "GET", true);
            //log.info("~~" + json);
            JsonArray data = parser.parse( json ).getAsJsonArray();
            Map<Long, LongShortRatio> rv = new HashMap<>();
            for (int i = 0; i < data.size(); i++)
            {
                JsonObject obj = data.get( i ).getAsJsonObject();
                LongShortRatio lsr = new LongShortRatio();
                lsr.longAccount = obj.get("longAccount").getAsDouble();
                lsr.shortAccount = obj.get("shortAccount").getAsDouble();
                lsr.timestamp = obj.get("timestamp").getAsLong();
                rv.put(lsr.timestamp, lsr);
            }
            return rv;
        } catch (Throwable e) {
            log.error("Can't get binance long/short ratio for " + symbol + " " + e.getMessage(), e);
            return null;
        }
    }
    
    /**
     */
    private String postBinance(String B_URL, String subUrl, Map<String, String> params, boolean sign, String method, boolean futures) throws Throwable
    {   
        // log.info("> postBinance " + subUrl);
        String allQuery = "";
        for (String key : params.keySet())
        {
            String val = params.get(key);
            if (allQuery.length() > 0)
                allQuery = allQuery + "&";
            allQuery = allQuery + key + "=" + val;
        }
        
        log.info("Q: " + subUrl + " " + allQuery);
        
        if (sign)
        {
            allQuery += "&timestamp=" + (System.currentTimeMillis() + serverTimeDelta);
            allQuery += "&recvWindow=60000";
            allQuery += "&signature=" + makeSign( allQuery );
        }        
        
        // ex: https://testnet.binancefuture.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000 
        String urlStr = B_URL + subUrl + "?" + allQuery;
                    
        // log.info("U: " + urlStr);
        
        String rv = "";
        URL url = new URL( urlStr );

        HttpURLConnection conn = null;
        if (url.getProtocol().toLowerCase().equals("https")) {
            HttpsURLConnection httpsCon = (HttpsURLConnection) url.openConnection();
            httpsCon.setHostnameVerifier(new DummyHostnameVerifier());
            conn = httpsCon;
        } else {
            conn = (HttpURLConnection) url.openConnection();
        }
        conn.setConnectTimeout(60_000);
        conn.setReadTimeout(60_000);
        conn.setUseCaches(false);

        conn.setRequestMethod(method);
        
        String public_key = API_KEY;
        
        conn.setRequestProperty("X-MBX-APIKEY", public_key);
        conn.setRequestProperty("Accept", "application/json");
        conn.setDoOutput(true);
        conn.setDoInput(true);
        
        String line;

        // Expected 200
        int http_code = conn.getResponseCode();

        InputStream stream;
        if (http_code >= 400) {
            stream = conn.getErrorStream();
        } else {
            stream = conn.getInputStream();
        }

        if (stream == null) {
            log.warn("Пустой HTTP stream от Binance: code={}, endpoint={}", http_code, subUrl);
            return rv;
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
            while ((line = reader.readLine()) != null) {
                rv += line;
            }
        }
        //log("< postBinance");
        return rv;
    }
    
    /**  */
    public static String makeSign(String data)
    {
        String _secret_key = SKEY;
        String hash="";
        try{
            Mac sha256_HMAC = Mac.getInstance("HmacSHA256");
            SecretKeySpec secret_key = new SecretKeySpec(_secret_key.getBytes(), "HmacSHA256");
            sha256_HMAC.init(secret_key);            
            hash = DatatypeConverter.printHexBinary( sha256_HMAC.doFinal(data.getBytes()) );
        }catch (Exception e)
        {
        }
        return hash.trim();
    }
}
