package ru.polytest;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import jakarta.annotation.PostConstruct;
import org.codehaus.jackson.map.ObjectMapper;
import org.encog.neural.networks.BasicNetwork;
import org.encog.persist.EncogDirectoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import ru.bitok.binance_json.*;
import ru.bitok.json.MarginContainer;
import ru.bitok.json.MarginRecord;
import ru.bitok.json.OrderContainer;
import ru.bitok.json.OrderResp;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author ypabl
 */
@Service
@EnableScheduling
public class StakanService {
    public static final Logger log = LoggerFactory.getLogger(StakanService.class);
    
    public static final boolean DEMO = false;
    
    private ObjectMapper mapper = new ObjectMapper();
    
    private Map<String, Stakan> stakanBySymbol = new HashMap<>();

    @Autowired
    private BinanceHandler binanceHandler;

    @Autowired
    private ByBitHelper byBitHelper;

    @Autowired
    public TGSender tgSender;

    @Autowired
    private ChartFileCreator chartFileCreator;

    @Autowired
    private BinanceFutHandler binanceFutHandler;
    @Autowired
    private BinanceFutHandler2 binanceFutHandlerTrades;
    @Autowired
    private BinanceFutHandler3 binanceFutHandlerAllDepth;

    @Autowired
    private BuyBitFutHandler buyBitFutHandler;

    @Autowired
    private BuyBitService buyBitService;
    
    @Autowired
    private BinanceKLineHandler binanceKLineHandler;
    
    @Autowired
    private BinanceService binancer;
    
    @Autowired
    private HuobiService huobiService;

    @Autowired
    private EnricherService dbEnricherService;

    @Autowired
    private TradingStateStorageService tradingStateStorageService;
    
    //@Autowired
    //private HistoryRepo historyRepo;
    
    
    // Пока без этого обойдемся
    double usedOpenCnt = 0.0;
    
    private ru.bitok.Logger csvLogger = new ru.bitok.Logger("C:\\tmp", "_bitok.csv");
    
    JsonParser parser = new JsonParser();
    
    String lastOp = "";
    
    public Map<String, StakanContainer> bstakanBySymbol = new ConcurrentHashMap<>();
    public Map<String, StakanContainer> bstakanBySymbolFutures = new ConcurrentHashMap<>();
    
    public HashMap<String, LatestVolumes> volumesBySymbol = new HashMap<>();
    public HashMap<String, LatestVolumes> volumesBySymbolFutures = new HashMap<>();
    
    /**  */
    public synchronized StakanContainer getStakanContainerBySymbol(String symbol, boolean futures)
    {
        if (futures) {
            StakanContainer rv = bstakanBySymbolFutures.get(symbol);
            if (rv == null) {
                rv = new StakanContainer(symbol);
                bstakanBySymbolFutures.put(symbol, rv);
            }
            return rv;
        } else {
            StakanContainer rv = bstakanBySymbol.get(symbol);
            if (rv == null) {
                rv = new StakanContainer(symbol);
                bstakanBySymbol.put(symbol, rv);
            }
            return rv;
        }
    }
    
    /**  */
    public synchronized void saveStakanContainer(StakanContainer sc, boolean futures)
    {
        if (!futures)
            bstakanBySymbol.put(sc.symbol, sc);
        else 
            bstakanBySymbolFutures.put(sc.symbol, sc);
    }
    
    //@Scheduled(fixedDelay = 6000L)
    private void printStakanData()
    {        
        StakanContainer sc = bstakanBySymbol.get("LUNAUSDT");
        if (sc != null)
        {
            log.info("========== " + sc.symbol + " ==========");
            Bstakan stakan = sc.bstakan;
            for (int i = stakan.asks.size() - 1; i>=0 ; i--)
            {
                PriceQty pq = stakan.asks.get(i);
                log.info("S: " + pq);
            }
            log.info("----");
            for (int i = 0; i<stakan.bids.size() ; i++)
            {
                PriceQty pq = stakan.bids.get(i);
                log.info("B: " + pq);
            }
        } else
            log.info("========== XXX");
    }
    
    volatile boolean positionsBusy = false;
    
    public final static long TIMEFRAME_SIZE = 60_000L;    // ms.
    public final static long TIMEFRAME_SIZE_BIG = 120_000L;    // ms.
    private TimeFrame binanceFrame = new TimeFrame( TIMEFRAME_SIZE );
    private FrameHistory binanceHist = new FrameHistory( 20 );
    private TimeFrame binanceFrameBig = new TimeFrame( TIMEFRAME_SIZE_BIG );
    private FrameHistory binanceHistBig = new FrameHistory( 20 );
    
    // вторая проверочная "минутка" (от другого биржи)
    private TimeFrame bitmexFrame = new TimeFrame( TIMEFRAME_SIZE );
    private FrameHistory bitmexHist = new FrameHistory( 30 );    
    private TimeFrame bitmexFrameBig = new TimeFrame( TIMEFRAME_SIZE_BIG );
    private FrameHistory bitmexHistBig = new FrameHistory( 30 );
    
    private volatile Map<String, SymbolInfo> symbolInfos = null;
    
    Map<String, Set<PinCase>> pinBarCases = new HashMap<>();
    
    public int MAX_CANDLES = 50;

    
    private volatile Map<String, Double> maxTradeAmountForSymbol = new HashMap<>();
    private void setMaxTradeQtys(String data)
    {                
       Map<String, Double> _maxTradeAmountForSymbol = new HashMap<>();
       StringTokenizer st = new StringTokenizer(data.replaceAll("=", " ").trim(), "\r\n");
       while (st.hasMoreTokens())
       {
           String line = st.nextToken();
           StringTokenizer st2 = new StringTokenizer(line, " :;,-");
           String symbol = st2.nextToken();
           Double maxQty = Double.parseDouble(st2.nextToken()) * 0.99;
           _maxTradeAmountForSymbol.put(symbol, maxQty);
       }
       for (String s : _maxTradeAmountForSymbol.keySet())
       {
           double v = _maxTradeAmountForSymbol.get(s);
           log.info("NEW MAX TRADE AMNT SET: " + s + " " + String.format("%.2f", v));
       }
       maxTradeAmountForSymbol = _maxTradeAmountForSymbol;
    }
    
    /**  */
    public synchronized String printAllSettings()
    {
        String rv = "<h3>Default params</h3>" + this.defaultParams + "<br/>";
        rv += "<h3>Custom params</h3><br/>";
        for (String s : paramsForSymbol.keySet())
        {
            BestParams p = paramsForSymbol.get(s);
            rv += p + "<br/>";
        }

        rv += "<h3>DEALS</h3><br/>";
        DealPack dp = this.getAnyDealPack();
        for (Deal d : dp.deals)
        {
            String side = "UP";
            if (!d.up)
                side = "DOWN";
            String targetSide = "UP";
            if (!d.targetUp)
                targetSide = "DOWN";
            rv += d.symbol + " side: " + side 
                    + " QTY: " + String.format("%.5f", d.getTotalQty())
                    + " price: " + String.format("%.5f", d.price)
                    + " Amount: " + String.format("%.5f", d.price * d.getTotalQty().doubleValue())
                    + " targetSide: " + targetSide
                    + " targetQTY: " + String.format("%.5f", d.targetQty)
                    + "<br/>";
        }
        try {
            System.gc();
            log.info("System.gc() done.");
        } catch (Throwable t)
        {
            t.printStackTrace();
        }
        return rv;
    }
    

    
    public volatile Map<String, BestParams> paramsForSymbol = new HashMap<>();
    private void initBestParams()
    {
        Map<String, BestParams> symbolSettings = new HashMap<>();
        paramsForSymbol = symbolSettings;
        log.info("Inited settings for delta trades: " + paramsForSymbol.size());
    }
    
    private volatile BestParams defaultParams = new BestParams("", 12, 500, 500, -1, 12,12,12, 0, 0, 0);
    
    /**  */
    private BestParams getBestParams(String symbol)
    {
        BestParams bp = paramsForSymbol.get(symbol);
        if (bp == null) {
            bp = defaultParams;
            bp.symbol = symbol;
        }
        return bp;
    }

    static SimpleDateFormat sdf2 =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static SimpleDateFormat sdf3 =  new SimpleDateFormat("MM_dd HH-mm-ss");
    static SimpleDateFormat sdf4 =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    
    private volatile Map<String, Double> vlcBySymbol = new HashMap<>();
    
    private void getVolatileData()
    {
        String statPrintTime = "2023-04-09 00:00:00";
        long startPrintTiomeL = 0;
        long curTime = System.currentTimeMillis();
        try {
            startPrintTiomeL = sdf2.parse(statPrintTime).getTime();
        } catch (Exception e) {}
        Map<String, Double> _vlcBySymbol = new HashMap<>();
        for (String instr : symbolInfos.keySet())
        {
            CandleRequest req = new CandleRequest(instr, 1100, "2h"); // for runtime prod
            log.info("Get --- hh candles for " + instr);
            List<CandleResp> hist = null;
            try {
                hist = binancer.getCandles(false, req);
                MMA slow = new MMA(1100);
                MMA fast = new MMA(10);
                double k = 0;
                for (int i=0; i < hist.size(); i++)
                {
                    CandleResp cr = hist.get(i);
                    if (curTime - cr.openTime < 2 * 60 * 60 * 1000L)
                    {
                        log.info("skiped last bar");
                        continue;
                    }
                    double delta_1 = (cr.h - Math.min(cr.o, cr.c)) / cr.c; // отклонение в процентах от цены
                    double delta_2 = (Math.max(cr.o, cr.c) - cr.l) / cr.c;
                    double vlt = delta_1 + delta_2;
                    slow.addStat(vlt);
                    fast.addStat(vlt);
                    
                    k = slow.getAvg() == 0 ? 1 : fast.getAvg() / slow.getAvg();
         //           if (cr.openTime > startPrintTiomeL)
                 //       System.out.println("VLT; " + instr + " ;  " + sdf2.format( new Date(cr.openTime)) + " ; " + String.format("%.2f", k));
                }
                if (k > 0)
                {
                    _vlcBySymbol.put(instr, k);
                }
            } catch (Exception e)
            {
                log.error("cant get candles for " + instr, e);
                continue;
            }
        }
        vlcBySymbol = _vlcBySymbol;
    }
    
    private long earliestHuobiTime = Long.MAX_VALUE;


    /**  Load spot + Futures history data for all symbols from symbolInfos */
    private Bobo loadBoboBinance(String startTime, String endTimeStr, String extraInstr, String extraInstr2)
    {
        Bobo bobo = null;
        String allSymbs = "";
        for (String s : symbolInfos.keySet())
        {
            String fname = "bobo_"+ s + ".data";
            allSymbs += "_" + s;
            Bobo b = null;
            try {
                FileInputStream fis = new FileInputStream(fname);
                ObjectInputStream ois = new ObjectInputStream(fis);
                b = (Bobo) ois.readObject();
                ois.close();
            } catch (Exception e) {
                log.info("Can't read file " + fname + ", we'll create the data.");
                e.printStackTrace(System.out);
            }
            if (b != null)
            {
                if (bobo == null)
                    bobo = b;
                else
                    bobo.add( b );
            }
        }

        //String file = "bobo" + allSymbs + ".data";

        try {
            //if (bobo == null)
            {
                // log.info("Fetching Binance...");
                long curTime = System.currentTimeMillis();

                // Загружаем данные для теста
                for (String instr : symbolInfos.keySet()) {

                    long _startTime = sdf2.parse(startTime).getTime();
                    long _endTime = endTimeStr != null ? sdf2.parse(endTimeStr).getTime() : System.currentTimeMillis();

                    Bobo _bobo = new Bobo();
                    if (bobo != null) {
                        long latestExistTime = bobo.getLatestHistTime(instr);
                        if (latestExistTime >= _endTime)
                            continue;
                        // Время начала сбора данных устанавливаем на последние существующее
                        if (latestExistTime > 0) {
                            _startTime = latestExistTime;
                            _bobo = bobo.getPartForSymbol(instr);
                        }
                    }

                    log.info("pre-fetch : get 1m candles for " + instr);
                    SymbolInfo si = symbolInfos.get(instr);

                    int max = 1980000;
                    int cnt = 0;

                    while (true) {
                        long endTime = _startTime + 1 * 60 * 999 * 1000L;
                        log.info("Start time: " + _startTime);
                        CandleRequest req = new CandleRequest(instr, "1m", _startTime, 0);
                        List<CandleResp> histF = binancer.getCandles(true, req);

                        if (si.priceExample == 0) {
                            si.priceExample = histF.get(0).o;
                        }

                        Map<Long, CandleResp> histF_map = huobiService.toMap(histF, _bobo.huobiCandleMapBySymbolF.get(instr));
                        _bobo.huobiCandleMapBySymbolF.put(instr, histF_map);

                        try {
                            // на споте символа может и не быть вовсе
                            List<CandleResp> histS = binancer.getCandles(false, req);
                            if (histS == null)
                            {
                                log.info("No candle data available for Spot.");
                                break;
                            }
                            Map<Long, CandleResp> histS_map = huobiService.toMap(histS, _bobo.huobiCandleMapBySymbolS.get(instr));
                            _bobo.huobiCandleMapBySymbolS.put(instr, histS_map);
                        } catch (Exception e) {
                            break;
                        }

                        cnt += histF.size();
                        if (cnt >= max) {
                            break;
                        }

                        _startTime = endTime;
                        if (_startTime > _endTime) {
                            break;
                        }
                    }
                    String file = "bobo_" + instr + ".data";
                    log.info("Writing data to file " + file + " ...");
                    try {
                        FileOutputStream fos = new FileOutputStream(file);
                        ObjectOutputStream oos = new ObjectOutputStream(fos);
                        oos.writeObject(_bobo);
                        oos.flush();
                        oos.close();
                        log.info("done.");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    if (bobo == null)
                        bobo = _bobo;
                    else
                        bobo.add(_bobo);
                }
            }
            /* else {
                log.info("Data read from files ");
            } */
        } catch (Throwable t)
        {
            t.printStackTrace(System.out);
        }
        return bobo;
    }

    private void addSymbol(Map<String, SymbolInfo> symbolInfos, String s)
    {
        symbolInfos.put(s, new SymbolInfo( s ));
    }

    private boolean suxxes(int suxessPercent)
    {
        if (suxessPercent == 100 || Math.random() <= (double)suxessPercent/100.0)
            return true;
        else return false;
    }

    private int brakeInt(int in, int suxessPercent)
    {
        if (suxessPercent == 100 || Math.random() <= (double)suxessPercent/100.0)
            return in;
        if (Math.random() < 0.5)
            return in + 2;
        return in - 2;
    }

    private float calcGlobalBuySellRatio(ProboyLine pl, MMATema upP, MMATema upP2, MMATema downP, MMATema downP2)
    {
        if (pl == null || upP == null || upP2 == null || downP == null || downP2 == null)
            return 0.0f;

        double upp = (upP.getSum() + upP2.getSum()) / 2.0;
        double dwn = (downP.getSum() + downP2.getSum()) / 2.0;

        // Логика совпадает с торговыми условиями: "своё" направление делим на "чужое".
        if (pl.proboy && pl.up || !pl.proboy && !pl.up) {
            if (dwn == 0.0)
                return 0.0f;
            return (float) (upp * 10.0 / dwn);
        }
        if (upp == 0.0)
            return 0.0f;
        return (float) (dwn * 10.0 / upp);
    }

    private int addEventToDbBuffer(Map<String, List<Event>> dbEventsBySymbol, Event event, int bufferedCount)
    {
        if (event == null || event.symbol == null) {
            return bufferedCount;
        }
        List<Event> list = dbEventsBySymbol.get(event.symbol);
        if (list == null) {
            list = new ArrayList<>();
            dbEventsBySymbol.put(event.symbol, list);
        }
        list.add(event);
        return bufferedCount + 1;
    }

    private void flushDbEventsBuffer(Map<String, List<Event>> dbEventsBySymbol)
    {
        if (dbEventsBySymbol == null || dbEventsBySymbol.isEmpty()) {
            return;
        }
        for (Map.Entry<String, List<Event>> entry : dbEventsBySymbol.entrySet()) {
            List<Event> list = entry.getValue();
            if (list == null || list.isEmpty()) {
                continue;
            }
            dbEnricherService.enrichEventsFast(entry.getKey(), list);
            list.clear();
        }
    }

    private boolean equals(SymbContainer sc1, SymbContainer sc2)
    {
        if ( sc1.symTradeStat.ma1 == sc2.symTradeStat.ma1
                && sc1.symTradeStat.ma2 == sc2.symTradeStat.ma2
                && sc1.symTradeStat.ma3 == sc2.symTradeStat.ma3
                && sc1.symTradeStat.l1 == sc2.symTradeStat.l1
                && sc1.symTradeStat.l2 == sc2.symTradeStat.l2
                && sc1.symTradeStat.multik == sc2.symTradeStat.multik
                && sc1.symTradeStat.tpCount == sc2.symTradeStat.tpCount
                && sc1.symTradeStat.smooth == sc2.symTradeStat.smooth
        )
            return true;

        return false;
    }

    /**  */
    private List<SymbContainer> getBest4AllParts(Map<Integer, List<MaxProfitCollection>> goodProfitCollectionsByPart)
    {
        List<SymbContainer> maxProfSymbs;

        int parts = goodProfitCollectionsByPart.keySet().size();

        List<Map<Integer, MaxProfitCollection>> startegiesExistingInAllParts = new ArrayList<>();
        Map<Integer, MaxProfitCollection> bestResultsOfAllParts = null;

        for (int partNum = 1; partNum < 2; partNum++)
        {
            // Достаем сортированный лист результатов профитов.
            List<MaxProfitCollection> symbProfCollection = goodProfitCollectionsByPart.get( partNum );

            // Внутри результатов одной части теста - перебираем результаты от самого выгодного..
            for (int partResultI = 0; partResultI < symbProfCollection.size(); partResultI ++)
            {
                MaxProfitCollection partResultCollection = symbProfCollection.get( partResultI );

                SymbContainer exampleSetting = partResultCollection.maxProfSymbs.get(0);

                Map<Integer, MaxProfitCollection> bestExistingProfitsOfAllParts = new HashMap<>();
                bestExistingProfitsOfAllParts.put(partNum, partResultCollection);

                for (int partNumOther = 1; partNumOther < parts + 1; partNumOther++)
                {
                    // пропустим проверку аналогичной стратегии в том же кусочке
                    if (partNumOther == partNum)
                        continue;

                    // Достаем сортированный лист результатов профитов.
                    List<MaxProfitCollection> symbProfCollectionOther = goodProfitCollectionsByPart.get( partNumOther );

                    // Внутри результатов одной части теста - перебираем результаты от самого выгодного..
                    MaxProfitCollection bestOtherResultCollection = null;
                    for (int partResultIOther = 0; partResultIOther < symbProfCollectionOther.size(); partResultIOther ++)
                    {
                        MaxProfitCollection partResultCollectionOther = symbProfCollectionOther.get( partResultIOther );
                        SymbContainer exampleSettingOther = partResultCollectionOther.maxProfSymbs.get(0);
                        if (equals(exampleSetting, exampleSettingOther)) {
                            if (bestOtherResultCollection == null
                                    || bestOtherResultCollection.getMeasure() < partResultCollectionOther.getMeasure())
                                bestOtherResultCollection = partResultCollectionOther;
                        }
                    }
                    if (bestOtherResultCollection != null)
                    {
                        // добавить в множество лучших результатов разных частей....
                        bestExistingProfitsOfAllParts.put(partNumOther, bestOtherResultCollection);
                    }
                }
                if (bestExistingProfitsOfAllParts.keySet().size() == parts)
                {
                    // Стратегия есть во всех частях теста! и
                    // взято уже самое лучше из каждой части в рамках проверяемых ппараметров стратегии
                    startegiesExistingInAllParts.add( bestExistingProfitsOfAllParts );
                }
            }
        }

        double maxProfit = 0;
        for (int i=0; i < startegiesExistingInAllParts.size(); i++)
        {
            Map<Integer, MaxProfitCollection> allPartsData = startegiesExistingInAllParts.get(i);
            double maxOfGroup = Double.MIN_VALUE;
            for (MaxProfitCollection mpc : allPartsData.values())
            {
                // totalProfit += mpc.profit;
                if (maxOfGroup < mpc.getMeasure())
                    maxOfGroup = mpc.getMeasure();
            }
            if (bestResultsOfAllParts == null || maxOfGroup > maxProfit)
            {
                maxProfit = maxOfGroup;
                bestResultsOfAllParts = allPartsData;
            }
        }
        List<SymbContainer> rv = new ArrayList<>();
        if (bestResultsOfAllParts != null)
        {
            double sumSL = 0;
            double sumMult = 0;
            double sumTp = 0;
            double sumProfit = 0;
            for (MaxProfitCollection mpc : bestResultsOfAllParts.values())
            {
                rv = mpc.maxProfSymbs;
                sumSL += mpc.maxProfSymbs.get(0).symTradeStat.slComissions;
                sumMult += mpc.maxProfSymbs.get(0).symTradeStat.multik;
                sumTp += mpc.maxProfSymbs.get(0).symTradeStat.tpComissions;
                sumProfit += mpc.getProfit();
            }
            log.info(" - AVG SL:   " + sumSL / bestResultsOfAllParts.size());
            log.info(" - AVG TP:   " + sumTp / bestResultsOfAllParts.size());
            log.info(" - AVG Mult: " + sumMult / bestResultsOfAllParts.size());
            log.info(" - SUM prof: " + sumProfit);
        }
        return rv;
    }

    private FileOutputStream statFile = null;
    private void addStat(String str)
    {
        try {
        //    System.out.println( str );
            statFile.write(str.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Map<String, DeltaProcessor> deltaProcessorMap = new HashMap<>();

    private void initCandlesEthBtc(boolean loadCandles)
    {
        final int candleCnt = 300;

        DeltaProcessor deltaProcessor = new ru.bitok.DeltaProcessor(150, 55, 0,
                2,
                0, 33, 0, 0, 0, 0,
                "ETHUSDT",
                105, 555, 0, 0.4, true, false, 1, 4);
        deltaProcessorMap.put(deltaProcessor.baseCcy, deltaProcessor);

        deltaProcessor = new ru.bitok.DeltaProcessor(150, 55, 0,
                2,
                0, 30, 0, 0, 0, 0,
                "JASMYUSDT",
                105, 555, 0, 0.4, true, false, 1, 4);
        deltaProcessorMap.put(deltaProcessor.baseCcy, deltaProcessor);

        deltaProcessor = new ru.bitok.DeltaProcessor(150, 55, 0,
                2,
                0, 40, 0, 0, 0, 0,
                "ETCUSDT",
                105, 555, 0, 0.4, true, false, 1, 4);
        deltaProcessorMap.put(deltaProcessor.baseCcy, deltaProcessor);

        if (loadCandles) {
            for (DeltaProcessor dp : deltaProcessorMap.values())
            {
                /*
                CandleRequest req = new CandleRequest("BTCUSDT", candleCnt, "1m");
                List<CandleResp> btcF = binancer.getCandles(true, req);
                List<CandleResp> btcS = binancer.getCandles(false, req);*/
                CandleRequest req = new CandleRequest(dp.baseCcy, candleCnt, "1m");
                try {
                    List<CandleResp> ethF = binancer.getCandles(true, req);
                    List<CandleResp> ethS = binancer.getCandles(false, req);
                    dp.init(null, null, ethF, ethS);
                } catch (Exception e) {}
            }
        }
    }


    /**  */
    private void getHistory()
    {
        symbolInfos = new HashMap<>();

         //      initCandlesEthBtc(false);
//       addSymbol(symbolInfos, "ETHUSDT");

        addSymbol(symbolInfos, "BTCUSDT");
        addSymbol(symbolInfos, "ETHUSDT");
        addSymbol(symbolInfos, "LTCUSDT");
        addSymbol(symbolInfos, "XRPUSDT");

        addSymbol(symbolInfos, "JASMYUSDT");
        addSymbol(symbolInfos, "PEOPLEUSDT");
        addSymbol(symbolInfos, "BNBUSDT");
        addSymbol(symbolInfos, "BCHUSDT");
        addSymbol(symbolInfos, "SOLUSDT");

        addSymbol(symbolInfos, "DOGEUSDT");
        addSymbol(symbolInfos, "DOTUSDT");
        addSymbol(symbolInfos, "STXUSDT");

/*
        addSymbol(symbolInfos, "AVAXUSDT");     // -
        addSymbol(symbolInfos, "MATICUSDT");      // -
        addSymbol(symbolInfos, "LINKUSDT");  // -
        addSymbol(symbolInfos, "ADAUSDT");  // -

        addSymbol(symbolInfos, "BTCUSDT");
        addSymbol(symbolInfos, "ETHUSDT");
        addSymbol(symbolInfos, "ARBUSDT");      //
        addSymbol(symbolInfos, "ETCUSDT");
        addSymbol(symbolInfos, "BCHUSDT");
        addSymbol(symbolInfos, "LTCUSDT");      //
        addSymbol(symbolInfos, "SOLUSDT");      //
        addSymbol(symbolInfos, "DOGEUSDT");     //
        addSymbol(symbolInfos, "FILUSDT");      //

        addSymbol(symbolInfos, "NEARUSDT");     //
        addSymbol(symbolInfos, "JASMYUSDT");    //
        addSymbol(symbolInfos, "NEOUSDT");      //
        addSymbol(symbolInfos, "DOTUSDT");      //
        addSymbol(symbolInfos, "STXUSDT");      //
        addSymbol(symbolInfos, "ONTUSDT");      //
        addSymbol(symbolInfos, "ARUSDT");       //
        addSymbol(symbolInfos, "FTMUSDT");
        addSymbol(symbolInfos, "SEIUSDT");      //
        addSymbol(symbolInfos, "WLDUSDT");      //
        addSymbol(symbolInfos, "ORDIUSDT");     //
        addSymbol(symbolInfos, "BNBUSDT");      //
*/

        for (String symb : deltaProcessorMap.keySet())
        {
            addSymbol(symbolInfos, symb);
        }
   //     deltaProcessorMap.clear();

        try {

            String startTime =  "2024-07-01 00:00:00";
            String endTimeStr = "2024-08-01 00:00:00";
            String oneSymb = null;
            for (String s : symbolInfos.keySet())
            {
            //    if (!s.startsWith("BTC"))
                {
                    oneSymb = s;
                    break;
                }
            }

            Bobo bobo = loadBoboBinance(startTime, endTimeStr, "-", "-");

            long earlietsCandleTime = Long.MAX_VALUE;
            long latestCandleTime = Long.MIN_VALUE;
            for (Long t : bobo.huobiCandleMapBySymbolS.get( oneSymb ).keySet())
            {
                if (earlietsCandleTime > t) earlietsCandleTime = t;
                if (latestCandleTime < t) latestCandleTime = t;
            }
            
            long step = 1 * 60 * 1000L;

            {
                Double tradeAmount = 1_000.0;

                TradeStat symTradeStat_max = null;


                Map<Long, CandleResp> symbF = null;
                Map<Long, CandleResp> symbS = null;

                String startTime2 = "2024-06-15 00:00:00";
                String endTime2   = "2024-06-01 00:00:00";
              //  earlietsCandleTime = sdf2.parse( startTime2 ).getTime();
             //   latestCandleTime   = sdf2.parse( endTime2 ).getTime();

                log.info("        == FROM " + new Date(earlietsCandleTime) + " - to - " + new Date(latestCandleTime));
                int errCnt = 0;

                boolean printStat = false;

                List<TradeStat> allGoods = new ArrayList<>();


                final int PART_CNT = 1;
                long PART_SIZE = ((long)((latestCandleTime - earlietsCandleTime) / PART_CNT / 60_000)) * 60_000L;

                Map<Integer, List<MaxProfitCollection>> goodProfitCollectionsByPart = new HashMap<>();
                final double criticalLossPercent = 40; // потери до закрытия сделки
                final double MAX_PROSADKA_PERCENT = 20; // максимальная просадка зафиксированного баланса от максимума
                final boolean printBalanceStat = false;

                for (int parnTum = 1; parnTum <= PART_CNT; parnTum++)
                {
                    long _start = earlietsCandleTime + PART_SIZE * (parnTum - 1);
                    long _end   = _start + PART_SIZE;
                    log.info(" ============= Running part " + parnTum + " of " + PART_CNT + " / "
                            + new Date(_start )+ " -> " + new Date(_end));

                    List<MaxProfitCollection> maxProfSymbs = new ArrayList<>();
                    double maxProf = 0;

                        for (int ma4 = 160; ma4 <= 160; ma4 += 30)

                for (int ma1 = 200; ma1 <= 200; ma1 += 10)
                            for (int ma2 = 140; ma2 <= 140; ma2 += 10) // bs1
                            for (int ma3 = 0; ma3 <= 0; ma3 += 10) // bs2
                            for (int smooth = 1; smooth <= 1; smooth += 1)


                for (int tp = 40; tp <= 40; tp += 10) // 45 95
                for (int sl = 40; sl <= 40; sl += 10)
                for (int multik = 10; multik <= 10; multik += 10) //
                for (int martin = 0; martin <= 0; martin += 1) // множитель мартингейл = 1 + martin / 10.0
                for (int l1 = 8; l1 <= 8; l1+= 1) // 16 27
                for (int l2 = 0; l2 <= 0; l2+= 1)
                        for (int l3 = 0; l3 <= 0; l3 += 1)
                        for (int l4 = 0; l4 <= 0; l4 += 1)
                            for (int l5 = 0; l5 <= 0; l5 += 1)
                                for (int l6 = 0; l6 <= 0; l6 += 1)
                for (int tpCnt = 1; tpCnt <= 1; tpCnt += 1)
                for (int minProfitToClose = 100; minProfitToClose <= 100; minProfitToClose += 1) // 35 !
                                     for (int nlComms = 0; nlComms <= 0; nlComms += 1)  // 20 ?
                                     for (int startTpPercent = 50; startTpPercent <= 50; startTpPercent += 10)
                                     for (int maxPortions = 1; maxPortions <= 1; maxPortions += 1)
                                     for (int x = 10; x <= 10; x += 1)
                                     for (int x2 = 0; x2 <= 0; x2 +=1)  // 0..7
                                     for (int suxxes = 100; suxxes >= 100; suxxes -= 3)
                                     for (int maxParallelDeals = 6; maxParallelDeals <= 12; maxParallelDeals += 1)
                                            for (int sb = -50; sb <= -50; sb += 10)                  //  4 -5
                                                //  !!! сделать большим при начальных тестах!   25 оптимально с марта
                                            for (int closeAllProfit = 555; closeAllProfit <= 555; closeAllProfit += 5) // 0.1 %
                                            for (int closeAllLoss = 555; closeAllLoss <= 555; closeAllLoss += 5) // 0.1 %
                                            for (int criticalLossToClosePart = 55; criticalLossToClosePart <= 55; criticalLossToClosePart += 1)

                {
                    log.info("part " + parnTum + " ma1 " + ma1 + " tp " + tp + " sl " + sl + " smooth" + smooth +
                             " mult " + multik + " l1 " + l1 + " l2 " + l2 + " l3 " + l3 +  " x=" + x + " sb=" + sb + " martin=" + martin +
                             " maxParallelDeals " + maxParallelDeals + " criticalLossToClosePart " + criticalLossToClosePart +
                    " minProfitToClose " + minProfitToClose + " closeAllProfit " + closeAllProfit + " maxPortions " + maxPortions);

                    List<SymbContainer> symbs = new ArrayList<>();
                    SymbContainer scBTC = null;
                 //   boolean onlyBuy = true;
                    for (String symb : symbolInfos.keySet())
                    {
                        {
                            DeltaProcessor deltaProcessor = deltaProcessorMap.get(symb);
                            if (deltaProcessor == null) {
                                deltaProcessor = new DeltaProcessor(ma1, ma2, ma3, smooth, l1, l2, l3, l4, l5, l6, symb, tp, sl, nlComms, multik/100.0, true,
                                        true, tpCnt);
                                deltaProcessor.quants = maxPortions;
                              //  deltaProcessorMap.put(symb, deltaProcessor);
                            }
                            SymbContainer sc = new SymbContainer(symb);

                            sc.symbF = bobo.huobiCandleMapBySymbolF.get(symb);
                            sc.symbS = bobo.huobiCandleMapBySymbolS.get(symb);

                            //sc.mmaVolBuy = new MMA( deltaProcessor.ma2 );
                            //sc.mmaVolSell = new MMA( deltaProcessor.ma2 );
                            sc.rsiVolBuySmooth = new RSI( deltaProcessor.ma2 );

                            sc.mfast = new MMA(deltaProcessor.ma1);
                            sc.mslow = new MMA(deltaProcessor.ma2);

                            sc.evo1 = new EVO(deltaProcessor.ma1, deltaProcessor.smooth);
                            sc.evo2 = new EVO(deltaProcessor.ma1, deltaProcessor.smooth);
                            sc.evo3 = new EVO(deltaProcessor.ma1, deltaProcessor.smooth);
                            sc.evo1c = new EVO(deltaProcessor.ma2, deltaProcessor.smooth);
                            sc.evo2c = new EVO(deltaProcessor.ma2, deltaProcessor.smooth);
                            sc.evo3c = new EVO(deltaProcessor.ma2, deltaProcessor.smooth);

                            sc.rsiDelta = new RSI( deltaProcessor.ma1 );
                            sc.rsiVolBuy = new RSI( deltaProcessor.ma1 );
                            //sc.rsiVolBuyS = new RSI( deltaProcessor.ma1 );
                            sc.rsiVolSell = new RSI( deltaProcessor.ma1 );
                            //sc.rsi = new RSI( deltaProcessor.ma1 );

                            sc.symTradeStat = new TradeStat( symb, tradeAmount, deltaProcessor.ma1, deltaProcessor.ma2, deltaProcessor.ma3, ma4,
                                    //(int)(deltaProcessor.TP_COMMS - deltaProcessor.TP_COMMS * x2 / 10.0)
                                    tp
                                    ,
                                    deltaProcessor.SL_COMMS,
                                    tpCnt, 1,
                                   deltaProcessor.MULT * 10.0 * x,
                                    //multik,
                                    sb, 1, deltaProcessor.smooth, deltaProcessor.NO_LOSS_COMMS, deltaProcessor.quants, x, x2,
                                    deltaProcessor.rsiLevel, deltaProcessor.rsiLevel2,
                                    deltaProcessor.l3, deltaProcessor.l4, deltaProcessor.l5, l6,
                                    startTpPercent,startTpPercent, true, suxxes, maxParallelDeals, minProfitToClose, closeAllProfit,
                                    criticalLossToClosePart, martin, "");
/*
                            if (onlyBuy)
                            {
                                sc.symTradeStat.onlyBuy = true;
                            } else
                                sc.symTradeStat.onlySell = true;
                            onlyBuy = !onlyBuy;
*/
                            symbs.add( sc );

/*
                            if (symb.startsWith("BTCUSDT")) {
                                scBTC = sc;
                        //        symbFb = sc.symbF;
                          //      symbSb = sc.symbS;
                            } else*/
                            {
                                symbF = sc.symbF;
                                symbS = sc.symbS;
                            }

                            // для дальнейшей печати
                            symTradeStat_max = sc.symTradeStat;
                        }
                    }


                    errCnt = 0;

                    int i = 0;
                    boolean criticalLoss = false;
                    boolean criticalProsadka = false;
                    double maxBalance = TradeStat.tradeAmntGlobalByKey.get("");
                    if (printBalanceStat)
                        prepareStatFile();
                    double maxTotalLoss = Double.MIN_VALUE;
                    double maxProsadka = Double.MIN_VALUE;
                    double minTotalProfit = 0.0;
                    double maxTotalProfit = 0.0;
                    for (long candleOpenTime = _start; candleOpenTime <= _end; candleOpenTime += step)
                    {
                        double totalMaxlLossB = 0.0;
                        double totalMaxlLossS = 0.0;
                        double totalMaxlProfB = 0.0;
                        double totalMaxlProfS = 0.0;
                        double totalMaxPNL = 0.0;
                        double totalMinPNL = 0.0;

                        double totalProfit = 0.0;
                        boolean btcWas = false;
                        List<SymbContainer> pretendToOpen = new ArrayList<>();
                        List<SymbContainer> pretendToClose = new ArrayList<>();
                        for (SymbContainer sc : symbs)
                        {
                            if (sc.symTradeStat.didBuy != null)
                            {
                                if (sc.symTradeStat.didBuy)
                                    upCnt++;
                                else
                                    downCnt ++;
                            }
                        }
                        for (SymbContainer sc : symbs)
                        {

                        //    if (sc.symb.startsWith("BTCUSDT"))
                          //      continue;

                            CandleResp crF = sc.symbF.get(candleOpenTime);
                            CandleResp crS = sc.symbS.get(candleOpenTime);

                         //   CandleResp crF_btc = scBTC.symbF.get(candleOpenTime);
                           // CandleResp crS_btc = scBTC.symbS.get(candleOpenTime);

                            if (crF == null || crS == null
                             //       || crF_btc == null || crS_btc == null
                            ) {
                                errCnt++;
                                continue;
                            }
/*
                            if (!btcWas) {
                                double deltaH_btc = crS_btc.h - crF_btc.h;
                                double deltaL_btc = crS_btc.l - crF_btc.l;
                                double deltaAVG_btc = deltaH_btc + deltaL_btc;
                                scBTC.rsiDelta.addStat( deltaAVG_btc );
                                scBTC.rsiVolBuy.addStat((crF_btc.taker_buy_base_asset_vol - crF_btc.taker_sell_base_asset_vol) * crF_btc.vol );
                                if (scBTC.zohan > 0 && scBTC.rsiDelta.getAvg() < 50) {
                                    scBTC.zohan = 0;
                                }
                                else if (scBTC.zohan < 0 && scBTC.rsiDelta.getAvg() > 50) {
                                    scBTC.zohan = 0;
                                }
                                if (scBTC.zohanV > 0 && scBTC.rsiVolBuy.getAvg() < 50) {
                                    scBTC.zohanV = 0;
                                }
                                else if (scBTC.zohanV < 0 && scBTC.rsiVolBuy.getAvg() > 50) {
                                    scBTC.zohanV = 0;
                                }
                                scBTC.zohan = scBTC.zohan + scBTC.rsiDelta.getAvg() - 50.0;
                                scBTC.zohanV = scBTC.zohanV + scBTC.rsiVolBuy.getAvg() - 50.0;
                                btcWas = true;
                            }*/

                            double deltaH = crS.h - crF.h;
                            double deltaL = crS.l - crF.l;
                            double deltaAVG = deltaH + deltaL;

                            sc.evo1.addStat(crF.o, crF.h, crF.l, crF.c, crF.taker_buy_base_asset_vol);
                            sc.evo2.addStat(crF.o, crF.h, crF.l, crF.c, crF.taker_sell_base_asset_vol);
                            sc.evo3.addStat(crF.o, crF.h, crF.l, crF.c, crF.vol);

                    //        sc.rsiDelta.addStat( deltaAVG );

                     //       sc.mfast.addStat( deltaAVG );
                       //     sc.mslow.addStat( deltaAVG );
                          //  sc.mmaVolBuy.addStat(crF.taker_buy_base_asset_vol);
                          //  sc.mmaVolSell.addStat(crF.taker_sell_base_asset_vol);
                            sc.rsiVolBuy.addStat( crF.taker_buy_base_asset_vol );
                        //    sc.rsiVolSell.addStat( crF.taker_sell_base_asset_vol );
                        //    sc.rsiVolSell.addStat( crF.vol );
                         //    sc.rsiVolBuy.addStat(crF.taker_buy_base_asset_vol - crF.taker_sell_base_asset_vol);
                        //    sc.rsiVolBuy.addStat((crF.taker_buy_base_asset_vol - crF.taker_sell_base_asset_vol) * crF.vol );

                            i++;
                            if (i < 180) continue;

                            sc.symTradeStat.traceMinMax(crF.l, crF.h, crF.c);
                           // scBTC.symTradeStat.traceMinMax(crF_btc.l, crF_btc.h, crF_btc.c);
                            // это максимальное отклонение - без учета того, что за минуту могло дернуться в обе
                            // стороны и на самом деле отклонение реальное меньше.
                            if (sc.symTradeStat.didBuy != null) {
                                if (sc.symTradeStat.didBuy) {
                                    totalMaxlLossB += sc.symTradeStat.lossPercent;
                                    totalMaxlProfB += sc.symTradeStat.profitPercent;
                                } else {
                                    totalMaxlLossS += sc.symTradeStat.lossPercent;
                                    totalMaxlProfS += sc.symTradeStat.profitPercent;
                                }
                            }
                            // BTC
                            /*
                            if (scBTC.symTradeStat.didBuy != null) {
                                if (scBTC.symTradeStat.didBuy) {
                                    totalMaxlLossB += scBTC.symTradeStat.lossPercent;
                                    totalMaxlProfB += scBTC.symTradeStat.profitPercent;
                                } else {
                                    totalMaxlLossS += scBTC.symTradeStat.lossPercent;
                                    totalMaxlProfS += scBTC.symTradeStat.profitPercent;
                                }
                            }*/


                            if (sc.zohan > 0 && sc.rsiDelta.getAvg() < 50) {
                                sc.zohan = 0;
                            }
                            else if (sc.zohan < 0 && sc.rsiDelta.getAvg() > 50) {
                                sc.zohan = 0;
                            }
                            if (sc.zohanV > 0 && sc.rsiVolBuy.getAvg() < 50) {
                                sc.zohanV = 0;
                            }
                            else if (sc.zohanV < 0 && sc.rsiVolBuy.getAvg() > 50) {
                                sc.zohanV = 0;
                            }
                            if (sc.zohanVS > 0 && sc.rsiVolSell.getAvg() < 50) {
                                sc.zohanVS = 0;
                            }
                            else if (sc.zohanVS < 0 && sc.rsiVolSell.getAvg() > 50) {
                                sc.zohanVS = 0;
                            }

                            sc.zohan = sc.zohan + sc.rsiDelta.getAvg() - 50.0;
                            sc.zohanV = sc.zohanV + sc.rsiVolBuy.getAvg() - 50.0;
                            sc.zohanVS = sc.zohanVS + sc.rsiVolSell.getAvg() - 50.0;


                            if (
                                    sc.evo3.getAvg() < sc.evo2.getAvg()
                                            &&
                                            sc.zohanV > l1
                            ) {
                                if ( sc.symTradeStat.didBuy != null
                                        || TradeStat.totalDeals < maxParallelDeals)
                                {
                                    sc.symTradeStat.buy(crF.c);
                                    sc.zohan = 0;
                                    sc.zohanV = 0;
                                    sc.zohanVS = 0;
                                }
                            } else
                            if (
                                    sc.evo3.getAvg() < sc.evo1.getAvg()
                                            &&
                                            sc.evo3.getPrevAvg() >= sc.evo1.getPrevAvg()
                            ) {
                                if ( sc.symTradeStat.didBuy != null
                                        || TradeStat.totalDeals < maxParallelDeals)
                                {
                             //       sc.symTradeStat.sell(crF.c);
                                //    sc.zohan = 0;
                                //    sc.zohanV = 0;
                                //    sc.zohanVS = 0;
                                }
                            }

                            ///  Single trade (продаем половинку от купленного)....
                            /*
                            if (
                                    sc.evo3.getAvg() > sc.evo2.getAvg()
                                            &&
                                            sc.evo3.getPrevAvg() <= sc.evo2.getPrevAvg()
                            ) {
                                if ( sc.symTradeStat.didBuy != null
                                        || TradeStat.totalDeals < maxParallelDeals)
                                {
                                    sc.symTradeStat.buy(crF.c);
                                }
                            } else
                            if (
                                    sc.evo3.getAvg() < sc.evo1.getAvg()
                                            &&
                                            sc.evo3.getPrevAvg() >= sc.evo1.getPrevAvg()
                            ) {
                                if ( sc.symTradeStat.didBuy != null
                                        || TradeStat.totalDeals < maxParallelDeals)
                                {
                                    sc.symTradeStat.sell(crF.c);
                                }
                            }*/

                            ////////////////////////// CLOSE //////////////////////
                            /*
                            if (
                                    sc.mfast.getAvg() < sc.mslow.getAvg()
                                            &&
                                            sc.mfast.getPrevAvg() >= sc.mslow.getPrevAvg()
                            ) {
                                sc.symTradeStat.buy(crF.c, true, false);
                            } else
                            if (
                                    sc.mfast.getAvg() > sc.mslow.getAvg()
                                            &&
                                            sc.mfast.getPrevAvg() <= sc.mslow.getPrevAvg()
                            ) {
                                sc.symTradeStat.sell(crF.c, true, false);
                            }
*/
                            if (sc.symTradeStat.didBuy != null)
                            {
                                // метод getCurreProfitPercent скорее всего содержит ошибку расчета!
                                // исправить
                                totalProfit += sc.symTradeStat.getCurreProfitPercent(crF.c);
                                sc.lastPrice = crF.c;
                                if (sc.symTradeStat.currentProfit > (double) minProfitToClose) {
                                    pretendToClose.add(sc);
                                }
                            }
                    } // -------------------- end of 'symbs'

                        double extremum1 = totalMaxlProfB - totalMaxlLossS;
                        double extremum2 = totalMaxlProfS - totalMaxlLossB;
                        totalMaxPNL = Math.max(extremum1, extremum2);
                        totalMinPNL = Math.min(extremum1, extremum2);

                        if (TradeStat.tradeAmntGlobalByKey.get("") > maxBalance)
                            maxBalance = TradeStat.tradeAmntGlobalByKey.get("");
                        double prosadka = 100.0 * (maxBalance - TradeStat.tradeAmntGlobalByKey.get("")) / maxBalance;
                        if (prosadka > MAX_PROSADKA_PERCENT)
                        {
                            criticalProsadka = true;
                            log.info("Critical prosadka: " + prosadka);
                            break;
                        }

                        if (maxTotalProfit < totalMaxPNL)
                            maxTotalProfit = totalMaxPNL;
                        if (minTotalProfit > totalMinPNL)
                            minTotalProfit = totalMinPNL;

                        if (printBalanceStat)
                        {
                            if (candleOpenTime % 3600_000 == 0) {
                                addStat("['" + sdf2.format(new Date(candleOpenTime)) + "', "
                                        + TradeStat.tradeAmntGlobalByKey.get("") +
                                        ", " + minTotalProfit + ", " + maxTotalProfit + "],");
                                minTotalProfit = 0;
                                maxTotalProfit = 0;
                            }
                        }
                        /*
                        while (totalProfit < -criticalLossToClosePart)
                        {
                            double biggestLoss = 0;
                            SymbContainer sc2Clear = null;
                            for (SymbContainer sc : symbs)
                            {
                                if (sc.symTradeStat.didBuy != null) {
                                    PriceCty pc = sc.symTradeStat.allDealPrices.getOldesRecord();
                                    double lossPer = 0;
                                    if (sc.symTradeStat.didBuy)
                                    {
                                        lossPer = (pc.price - sc.lastPrice) / sc.lastPrice;
                                    } else
                                    {
                                        lossPer = (sc.lastPrice - pc.price) / sc.lastPrice;
                                    }
                                    if (lossPer > biggestLoss)
                                    {
                                        biggestLoss = lossPer;
                                        sc2Clear = sc;
                                    }
                                }
                            }
                            if (sc2Clear != null)
                            {
                                if (sc2Clear.symTradeStat.didBuy) {
                                    sc2Clear.symTradeStat.sell(sc2Clear.lastPrice);
                                } else {
                                    sc2Clear.symTradeStat.buy(sc2Clear.lastPrice);
                                }
                                totalProfit += biggestLoss * (double)sc2Clear.symTradeStat.multik * 100.0; // ?
                            } else {
                                log.info("Strange situation! ");
                                break;
                            }
                        } */

                        if (-totalMinPNL >= criticalLossPercent)
                        {
                            criticalLoss = true;
                            log.info("Critical loss: " + totalMinPNL);
                            break;
                        }
                        if (totalMaxPNL >= closeAllProfit / 10.0)
                        {
                            TradeStat.tradeAmntGlobalByKey.put("", TradeStat.tradeAmntGlobalByKey.get("") * (1.0 + totalMaxPNL/100.0));
                            for (SymbContainer sc : symbs)
                            {
                                if (sc.symTradeStat.didBuy != null)
                                    sc.symTradeStat.tradeCnt++;
                                sc.symTradeStat.allDealPrices.reset();
                                sc.symTradeStat.didBuy = null;
                                sc.symTradeStat.currentProfit = 0.0;
                                sc.symTradeStat.tradeAmnt = sc.symTradeStat.tradeAmntGlobalByKey.get("");
                                sc.symTradeStat.gridBuy = null;
                            }
                            TradeStat.totalDeals = 0;
                        }
                        if (-totalMinPNL >= closeAllLoss / 10.0)
                        {
                            TradeStat.tradeAmntGlobalByKey.put("", TradeStat.tradeAmntGlobalByKey.get("") * (1.0 + totalMinPNL/100.0));
                            for (SymbContainer sc : symbs)
                            {
                                if (sc.symTradeStat.didBuy != null)
                                    sc.symTradeStat.tradeCnt++;
                                sc.symTradeStat.allDealPrices.reset();
                                sc.symTradeStat.didBuy = null;
                                sc.symTradeStat.currentProfit = 0.0;
                                sc.symTradeStat.tradeAmnt = sc.symTradeStat.tradeAmntGlobalByKey.get("");
                                sc.symTradeStat.gridBuy = null;
                            }
                            TradeStat.totalDeals = 0;
                        }

                        /* else
                        while (!pretendToClose.isEmpty() && !pretendToOpen.isEmpty())
                        {
                            SymbContainer scClose = pretendToClose.remove( 0 );
                            SymbContainer scOpen = pretendToOpen.remove( 0 );
                            if (scOpen.pretendToBuy)
                                scOpen.symTradeStat.buy(scOpen.lastPrice);
                            else
                                scOpen.symTradeStat.sell(scOpen.lastPrice);
                            scClose.symTradeStat.close(scClose.symTradeStat.didBuy, scClose.lastPrice);
                        } */
                        if (maxTotalLoss < -totalMinPNL)
                            maxTotalLoss = -totalMinPNL;
                        if (prosadka > maxProsadka)
                            maxProsadka = prosadka;
                    } // -------------------- end of FOR candles...


                    if (printBalanceStat)
                        closeStatFile();

                    if (criticalLoss || criticalProsadka)
                    {
                        continue;
                    }

                    double sumProfit = 0;
                    double maxLoss = 0;
                    int trCnt = 0;

                    for (SymbContainer sc : symbs)
                    {
                        // New logic:
                        //sumProfit += sc.symTradeStat.getAccumulatedProfit();
                        sumProfit = TradeStat.tradeAmntGlobalByKey.get("");

                        maxLoss = sc.symTradeStat.getMaxLossPercent();
                        trCnt += sc.symTradeStat.tradeCnt;
                    }
                 //   if (maxLoss < 29.0 && trCnt > 35)
                    if (maxProfSymbs == null || maxProf < sumProfit)
                    {
                        //maxProfSymbs = symbs;
                        maxProf = sumProfit;
                    }

                    MaxProfitCollection newResult = new MaxProfitCollection(symbs, sumProfit,
                            maxTotalLoss, maxProsadka, trCnt);
                    int insertIndex;
                    for (insertIndex = 0; insertIndex < maxProfSymbs.size(); insertIndex++)
                    {
                        MaxProfitCollection mpc = maxProfSymbs.get( insertIndex );
                        if (mpc.getMeasure() < newResult.getMeasure())
                            break;
                    }
                    maxProfSymbs.add(insertIndex, newResult);

                    log.info(" -- good variant -- " + sumProfit + " (max " + maxProf + ")");
                    log.info(" upCnt " + TradeStat.upCnt + " downCnt " + TradeStat.downCnt);
                    for (SymbContainer sc : symbs)
                    {
                        log.info(" ~~ " + sc.symb + " " + sc.symTradeStat);
                    }

                }   // ========= END of TIMELINE TEST ===========
                    goodProfitCollectionsByPart.put(parnTum, maxProfSymbs);

                }   // ========= END OF---  FOR --- n PART's ----

                List<SymbContainer> maxProfSymbs = getBest4AllParts( goodProfitCollectionsByPart );

                log.info("Error cnt: " + errCnt);
                log.info(" ====== calculation finished =====");
           //     log.info(" MAX TOTAL PROFIT: " + maxProf);
                log.info("------- All symbs scenarios: -------------");
                for (SymbContainer sc : maxProfSymbs)
                {
                    log.info(" -- " + sc.symb + " " + sc.symTradeStat);
                }
                log.info(" ============== GRAF: ===================");


                symTradeStat_max.ma1 = 50;
                symTradeStat_max.ma2 = 100;

                MMA _mfast = new MMA(  symTradeStat_max.ma1 );
                MMA _mslow = new MMA(  symTradeStat_max.ma2 );
                RSI mfast = new RSI(50);
                RSI mslow = new RSI(150);

                //EVO evoTakeBuyF = new EVO(symTradeStat_max.ma1, 3);
                //EVO evoTakeSellF = new EVO(symTradeStat_max.ma1, 3);
                EVO evo1 = new EVO(150, 2);
                EVO evo2 = new EVO(150, 2);
                EVO evo3 = new EVO(150, 2);
                EVO evo1L = new EVO(150, 2);
                EVO evo2L = new EVO(150, 2);


                RSI rsiDE = new RSI(100);

                RSI rsiTakeBuyS = new RSI( 60 );
                RSI rsiTakeSellS = new RSI( 60 );

                RSI rsiBS = new RSI( 45 );
                RSI rsi = new RSI( 45 );
                RSI rsiDelta = new RSI( 45 );



                MMA maBuyVolS = new MMA ( symTradeStat_max.ma1 );
                MMA maSellVolS = new MMA ( symTradeStat_max.ma1 );
                MMA maBuyVolF = new MMA ( symTradeStat_max.ma1 );
                MMA maSellVolF = new MMA ( symTradeStat_max.ma1 );

                MMA smaBuyVolS = new MMA ( 15 );
                MMA smaSellVolS = new MMA ( 15 );
                MMA smaBuyVolF = new MMA ( 15 );
                MMA smaSellVolF = new MMA ( 15 );

                MMA bsMa1 = new MMA(20);
                MMA bsMa2 = new MMA(60);
                MMA bsMa3 = new MMA(200);

                    long endTime = sdf2.parse(startTime2).getTime();

                    for (long candleOpenTime = earlietsCandleTime; candleOpenTime <= latestCandleTime; candleOpenTime += step)
                    {
                    //    if (endTime > candleOpenTime)
                    //        continue;

                        CandleResp crF = symbF.get( candleOpenTime );
                        CandleResp crS = symbS.get( candleOpenTime );

                  //      CandleResp h_crF = symbFb.get( candleOpenTime );
                    //    CandleResp h_crS = symbSb.get( candleOpenTime );

                        if (crF == null || crS == null //|| h_crF == null || h_crS == null
                            ) {
                            continue;
                        }

                        double deltaH = crS.h - crF.h;
                        double deltaL = crS.l - crF.l;
                        double deltaAVG = (deltaH + deltaL);

                   //     double h_deltaH = h_crS.h - h_crF.h;
                //        double h_deltaL = h_crS.l - h_crF.l;
                  //      double h_deltaAVG = (h_deltaH + h_deltaL);

                    //    double h2_deltaH = h2_crS.h - h2_crF.h;
                    //    double h2_deltaL = h2_crS.l - h2_crF.l;
                    //    double h2_deltaAVG = (h2_deltaH + h2_deltaL);

                   //     double h3_deltaH = h3_crS.h - h3_crF.h;
                  //      double h3_deltaL = h3_crS.l - h3_crF.l;
                   //     double h3_deltaAVG = (h3_deltaH + h3_deltaL);

                        //double hh_deltaH = hh_crS.h - hh_crF.h;
                        //double hh_deltaL = hh_crS.l - hh_crF.l;
                        //double hh_deltaAVG = (hh_deltaH + hh_deltaL);

                        _mfast.addStat( deltaAVG  );
                        _mslow.addStat( deltaAVG  );
                        mfast.addStat( deltaAVG );
                        mslow.addStat( deltaAVG );

                        //evoTakeSellF.addStat(crF.o, crF.h, crF.l, crF.c, crF.taker_sell_base_asset_vol);
                        //evoTakeBuyF.addStat(crF.o, crF.h, crF.l, crF.c, crF.taker_buy_base_asset_vol);

                        evo1.addStat(crF.o, crF.h, crF.l, crF.c, crF.taker_buy_base_asset_vol);
                        evo2.addStat(crF.o, crF.h, crF.l, crF.c, crF.taker_sell_base_asset_vol);
                        evo1L.addStat(crF.o, crF.h, crF.l, crF.c, crF.taker_buy_base_asset_vol);
                        evo2L.addStat(crF.o, crF.h, crF.l, crF.c, crF.taker_sell_base_asset_vol);
                        evo3.addStat(crF.o, crF.h, crF.l, crF.c, crF.vol);

                        rsiDE.addStat( evo1.getAvg() - evo2.getAvg());

                        rsiTakeBuyS.addStat( crS.taker_buy_base_asset_vol);
                        rsiTakeSellS.addStat( crS.taker_sell_base_asset_vol);

                        bsMa1.addStat( crF.taker_buy_base_asset_vol - crF.taker_sell_base_asset_vol );

                        rsiBS.addStat(bsMa1.getAvg() );
                        rsiDelta.addStat( deltaAVG );
                        rsi.addStat( deltaAVG
                                / crF.vol);

                        maBuyVolS.addStat( crS.taker_buy_base_asset_vol );
                        maSellVolS.addStat( crS.taker_sell_base_asset_vol );
                        maBuyVolF.addStat( crF.taker_buy_base_asset_vol );
                        maSellVolF.addStat( crF.taker_sell_base_asset_vol );

                        smaBuyVolS.addStat( maBuyVolS.getAvg() );
                        smaSellVolS.addStat( maSellVolS.getAvg() );
                        smaBuyVolF.addStat( maBuyVolF.getAvg() );
                        smaSellVolF.addStat( maSellVolF.getAvg() );

                        int l1 = symTradeStat_max.l1;

                        int sig = 0;
                        /*
                        if (
                                //rsi.getAvg() < rsi.getPrevAvg()
                                        rsi.getPrevAvg() > 100 - l1
                                        && rsi2.getPrevAvg() > 100 - l1
                        )
                        {
                            sig = 50;
                        } else if (
                        //rsi.getAvg() > rsi.getPrevAvg()
                                        rsi.getPrevAvg() < l1
                                        && rsi2.getPrevAvg() < l1
                        )
                        {
                            sig = -50;
                        }*/



                        if (printStat)
                        //    if (cnt % 10 == 0)
                            System.out.println("['" + sdf2.format(new Date(crF.openTime))
                                + "', " + String.format("%.6f", crF.l).replace(',', '.')
                                + ", " + String.format("%.6f", crF.o).replace(',', '.')
                                + ", " + String.format("%.6f", crF.c).replace(',', '.')
                                + ", " + String.format("%.6f", crF.h).replace(',', '.')

                                    //+ ", " + String.format("%.6f", rsiDelta.getAvg()).replace(',', '.')
                                    //+ ", " + String.format("%.6f", rsiBS.getAvg()).replace(',', '.')
                                    + ", " + String.format("%.6f", evo1.getAvg()).replace(',', '.')
                                    + ", " + String.format("%.6f", evo2.getAvg()).replace(',', '.')
                                    + ", " + String.format("%.6f", evo3.getAvg()).replace(',', '.')

                                //    + ", " + String.format("%.6f", rsiTakeBuyS.getAvg()).replace(',', '.')
                                    //    + ", " + String.format("%.6f", rsiTakeSellS.getAvg()).replace(',', '.')
                             //       + ", " + String.format("%.6f", msslow.getAvg() * 50 + 50).replace(',', '.')
                                //    + ", " + String.format("%.6f", mslow2.getAvg() * 20).replace(',', '.')
                                //    + ", " + String.format("%.6f", msslow2.getAvg() * 20).replace(',', '.')
                                   // + ", " + String.format("%.6f", eFast.getAvg()/10.0).replace(',', '.')
                                 //   + ", " + String.format("%.6f", eSlow.getAvg()/10.0).replace(',', '.')

                              //  + ", " + (sig + 50)
                                    + "],");
                    }
            }
        } catch (Throwable t) {
            log.info("EEEEEEEEEEEE " + t.getMessage());
            t.printStackTrace( System.out );
        }
    }


    private void prepareStatFile(String symbol)
    {
        try {
            if (symbol == null)
                statFile = new FileOutputStream("stat-" + sdf3.format(new Date()) + ".html");
            else
                statFile = new FileOutputStream("stat-" + symbol + ".html");
            String header = "<html>" +
                    "  <head>" +
                    "    <script type=\"text/javascript\" src=\"https://www.gstatic.com/charts/loader.js\"></script>" +
                    "    <script type=\"text/javascript\">" +
                    "      google.charts.load('current', {'packages':['corechart']});" +
                    "      google.charts.setOnLoadCallback(drawChart);" +
                    "  function drawChart() {" +
                    "    var data = google.visualization.arrayToDataTable([";
            addStat(header );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void prepareStatFile()
    {
        prepareStatFile(null);
    }

    private void closeStatFile()
    {
        try {
            String footer = "  ], true);" +
                    "    var options = {" +
                    "title: 'Trade stat'," +
                    "hAxis: { title: 'X-axis' }," +
                    "vAxis: { title: 'Y-axis' }," +
                    "        series: {" +
                    "0: {" +
                    "       type: 'candlesticks'," +
                    "targetAxisIndex: 0," +
                    "color: '#aaaaaa'" +
                    "}," +
                    "1: {" +
                    "       type: 'line'," +
                    "targetAxisIndex: 1," +
                    "color: '#ff0000'" +
                    "}," +

                    "2: {" +
                    "       type: 'line'," +
                    "targetAxisIndex: 1," +
                    "color: '#00ff00'" +
                    "}," +
                    "3: {" +
                    "       type: 'line'," +
                    "targetAxisIndex: 1," +
                    "color: '#00ff00'" +
                    "}," +
                    "4: {" +
                    "       type: 'line'," +
                    "targetAxisIndex: 1," +
                    "color: '#aaffaa'" +
                    "}," +
                    "        }," +
                    "        legend:'none'," +
                    "      crosshair: { trigger: 'both' }" +
                    "    };" +
                    "var chart = new google.visualization.ComboChart(document.getElementById('chart_div'));" +
                    "    chart.draw(data, options);" +
                    "  }" +
                    "    </script>" +
                    "  </head>" +
                    "  <body>" +
                    "    <div id=\"chart_div\" style=\"width: 80000px; height: 1500px;\"></div>" +
                    "  </body>" +
                    "</html>";
            addStat(footer);
            statFile.flush();
            statFile.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private long nextVLCinitTime = Long.MAX_VALUE;

    private boolean PLUS_PNL_MODE = true;

    /**  */
    private boolean newDealAvailable(String symb, boolean up, Map<String, SymbContainer> deals,
                                     int profPercentToClose,
                                     int maxDeals,
                                     TotalLossInfo totalLossInfo,
                                     int maxDeltaBS, int proboyOtskDelta, double PNL, boolean megaSignal, boolean proboy
                                , int ONE_DEAL_TPSL_CNT)
    {
        int cnt = 0, bCnt = 0, sCnt = 0;
        int proboyCnt = 0, otskokCnt = 0;
        Set<SymbContainer> scs2free = new HashSet<>();
        int allTPSLCnt = 0;
        int MAX_TP_SL_CNT = maxDeals * ONE_DEAL_TPSL_CNT;
        double _sumUpProfitComissins = 0;
        double _sumDownProfitComissins = 0;
        for (SymbContainer sc : deals.values())
        {
            if (sc.symTradeStat.allDealPrices.stat.size() > 0) {

                // Уже открыта сделка с этой монетой
                if (sc.symTradeStat.symbol.equals(symb))
                    return true;

                cnt++;
                allTPSLCnt += sc.symTradeStat.tpLevels.size();


                // if (sc.symTradeStat.getRealizedProfitPercent() > profPercentToClose)
                if (sc.symTradeStat.profitPercent > profPercentToClose / 10.0)
                {
                    scs2free.add(sc);
                }
                if (sc.symTradeStat.didBuy) {
                    bCnt++;
                    _sumUpProfitComissins += sc.symTradeStat.currentProfit;
                } else {
                    sCnt++;
                    _sumDownProfitComissins += sc.symTradeStat.currentProfit;
                }
                if (sc.symTradeStat.proboy)
                    proboyCnt ++;
                else
                    otskokCnt ++;
            }
        }

        if (PLUS_PNL_MODE) {
            if (up && _sumUpProfitComissins < 0)
                return false;
            if (!up && _sumDownProfitComissins < 0)
                return false;
        } else {

            if (up && bCnt - sCnt > maxDeltaBS) {
                for (SymbContainer sc : scs2free)
                {
                    if (sc.symTradeStat.didBuy) {
                        // Закроем сделку, чтобы дать возможность открыть новую сделку
                        double allQty = 0;
                        sc.symTradeStat.tradeCnt++;
                        allQty += sc.symTradeStat.allDealPrices.getTotalQty();
                        sc.symTradeStat.allDealPrices.reset();
                        sc.symTradeStat.didBuy = null;
                        sc.symTradeStat.currentProfit = 0.0;
                        double curTradeAmnt = TradeStat.tradeAmntGlobalByKey.get(sc.symTradeStat.groupId);
                        double newTradeAmnt = curTradeAmnt * (1.0 + Math.abs(sc.symTradeStat.profitPercent) * 0.9 / 100.0)
                                - allQty * TradeStat.comission * TradeStat.comissionZapasDef;
                        //  log.info("Close " + sc2free.symb + " prof " + sc2free.symTradeStat.profitPercent + "% curBal: " + curTradeAmnt + " newBal: " + newTradeAmnt);
                        TradeStat.tradeAmntGlobalByKey.put(sc.symTradeStat.groupId, newTradeAmnt);
                        for (SymbContainer _sc : deals.values()) {
                            _sc.symTradeStat.tradeAmnt = TradeStat.tradeAmntGlobalByKey.get(sc.symTradeStat.groupId);
                        }
                        totalLossInfo.removePNLInfo(sc);
                        return true;
                    }
                }
                return false;
            }
            if (!up && sCnt - bCnt > maxDeltaBS) {
                for (SymbContainer sc : scs2free) {
                    if (!sc.symTradeStat.didBuy) {
                        // Закроем сделку, чтобы дать возможность открыть новую сделку
                        double allQty = 0;
                        sc.symTradeStat.tradeCnt++;
                        allQty += sc.symTradeStat.allDealPrices.getTotalQty();
                        sc.symTradeStat.allDealPrices.reset();
                        sc.symTradeStat.didBuy = null;
                        sc.symTradeStat.currentProfit = 0.0;
                        double curTradeAmnt = TradeStat.tradeAmntGlobalByKey.get(sc.symTradeStat.groupId);
                        double newTradeAmnt = curTradeAmnt * (1.0 + Math.abs(sc.symTradeStat.profitPercent) * 0.9 / 100.0)
                                - allQty * TradeStat.comission * TradeStat.comissionZapasDef;
                        //  log.info("Close " + sc2free.symb + " prof " + sc2free.symTradeStat.profitPercent + "% curBal: " + curTradeAmnt + " newBal: " + newTradeAmnt);
                        TradeStat.tradeAmntGlobalByKey.put(sc.symTradeStat.groupId, newTradeAmnt);
                        for (SymbContainer _sc : deals.values()) {
                            _sc.symTradeStat.tradeAmnt = TradeStat.tradeAmntGlobalByKey.get(sc.symTradeStat.groupId);
                        }
                        totalLossInfo.removePNLInfo(sc);
                        return true;
                    }
                }
                return false;
            }

            if (proboy) {
                if (proboyCnt - otskokCnt > proboyOtskDelta)
                    return false;
            } else
                if (otskokCnt - proboyCnt > proboyOtskDelta)
                    return false;
        }

        if (cnt < maxDeals)
            return true;
        SymbContainer sc2free = null;
        if (!scs2free.isEmpty())
            sc2free = scs2free.iterator().next();
        if (sc2free != null && megaSignal)
        { // Закроем сделку, чтобы дать возможность открыть новую сделку
            double allQty = 0;
            sc2free.symTradeStat.tradeCnt++;
            allQty += sc2free.symTradeStat.allDealPrices.getTotalQty();
            sc2free.symTradeStat.allDealPrices.reset();
            sc2free.symTradeStat.didBuy = null;
            sc2free.symTradeStat.currentProfit = 0.0;
            double curTradeAmnt = TradeStat.tradeAmntGlobalByKey.get(sc2free.symTradeStat.groupId);
            double newTradeAmnt = curTradeAmnt * (1.0 + Math.abs(sc2free.symTradeStat.profitPercent) * 0.9 / 100.0)
                    - allQty * TradeStat.comission * TradeStat.comissionZapasDef;
            //  log.info("Close " + sc2free.symb + " prof " + sc2free.symTradeStat.profitPercent + "% curBal: " + curTradeAmnt + " newBal: " + newTradeAmnt);
            TradeStat.tradeAmntGlobalByKey.put(sc2free.symTradeStat.groupId, newTradeAmnt);
            for (SymbContainer _sc : deals.values()) {
                _sc.symTradeStat.tradeAmnt = TradeStat.tradeAmntGlobalByKey.get(sc2free.symTradeStat.groupId);
            }
            totalLossInfo.removePNLInfo(sc2free);
            return true;
        }
        return false;
    }

    private void refreshTotalLossInfo(TotalLossInfo totalLossInfo, SymbContainer sc, ProboyLine pl)
    {
        sc.symTradeStat.traceMinMax(pl.min, pl.max, pl.price);
        totalLossInfo.addOrReplacePL(sc);
    }

    /** return true  - if some new data was added */
    private boolean checkAndExtendklinesWithLongShortRatio(String symbol, Bobo2 bobo, String fname, int minutes)
    {
        // delisted symbols
        if ("BNXUSDT".equals(symbol))
            return false;

        boolean extended = false;

        // TEST
        log.info(" ============================ ");
        {
            CandleResp prev = null;
            boolean bug = false;
            for (int i=0; i< bobo.candles.size()-1; i++)
            {
                CandleResp cri = bobo.candles.get(i);
                String lsrDate = "----";
               // if (cri.lsRatio != null)
                 //   lsrDate = (new Date(cri.lsRatio.timestamp)).toString();
                //log.info(symbol + " candle " + new Date(cri.openTime) + " lsr " + lsrDate);
                if (prev != null && cri.openTime <= prev.openTime) {
                    bug = true;
                    bobo.candles.remove(i);
                    extended = true;
                    i--;
                    continue;
                }
                prev = cri;
            }
            if (bug)
                log.info(symbol + " ---> BUG");
        }

        if (true) return false;


        CandleResp cr = bobo.candles.get(bobo.candles.size() - 1);
        if (cr.lsRatio != null) {
/*
            for (int i=0; i< bobo.candles.size()-1; i++)
            {
                CandleResp cri = bobo.candles.get(i);
                if (cri.lsRatio != null)
                    log.info(symbol + " lsr " + new Date(cri.lsRatio.timestamp));
            }*/
            log.info("LSR FILLED EARLIER....");
            //return false;
        }

        long curTime = System.currentTimeMillis();

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(curTime);
        calendar.add(Calendar.MONTH, -1);
        long startStatFetchTime = calendar.getTimeInMillis() + 10 * 60_000L;
        log.info("Start LSR time: " + new Date(startStatFetchTime));

        Map<Long, LongShortRatio> lsrMap = null;

        long firstAppearLSRTime = 0;

        for (int i = 0; i < bobo.candles.size(); i++)
        {
            cr = bobo.candles.get( i );
            if (cr.openTime < startStatFetchTime)
                continue;   // биржа отдает не далее чем за 30 дней...
            if (cr.lsRatio != null)
                continue;   // данные уже есть, запрашивать не надо
            // Если мы тут, значит данных нет - надо запросить

            LongShortRatio lsr = null;

            if (lsrMap != null)
            {
                lsr = lsrMap.get( cr.openTime );
                if (lsr == null)
                    lsrMap = null;
            }
            if (lsrMap == null)
            {   // Множества LSR не было либо оно исчерпалось
                int requestLimit = Math.min(500, bobo.candles.size() - i);
                log.info("No LSR for candle " + new Date(cr.openTime));
                lsrMap = binancer.getLongShortRatioAsMap(symbol, minutes, requestLimit, cr.openTime - minutes * 2 * 60_000);
                if (lsrMap == null || lsrMap.size() < 1)
                {
                    log.info("API didn't return LSR for " + symbol + " -> quit LSR extend routine..");
                    return extended;
                }
                lsr = lsrMap.get( cr.openTime );
                if (lsr == null)
                    log.info("empty");
            }

            if (lsr != null) {
                // Время равно! НО! у KLINE это время начала бара, а в LSR это время конца периода.
                // Т.е. в данном баре мы храним LSR прошлого бара
                if (firstAppearLSRTime == 0)
                    firstAppearLSRTime = lsr.timestamp;
                cr.lsRatio = lsr;
                extended = true;
            }
        }
        log.info("-- LSR " + symbol + " starts from " + new Date(firstAppearLSRTime));
        return extended;
    }

    /**  Load spot + Futures history data for all symbols from symbolInfos */
    private Bobo2 loadCandlesBinance(String startTime, long _endTime, /*String endTimeStr,*/ String symbol, boolean useFile, int minutes)
    {
        String fname = "bobo2_"+ symbol + ".kryo";
        Bobo2 bobo = null;
        if (useFile) {
            Kryo kryo = getKryo();
            try (Input input = new Input(new FileInputStream(fname))) {
                bobo = (Bobo2)kryo.readClassAndObject(input);
            } catch (Exception e) {
                e.printStackTrace();
                log.info("Can't read file " + fname + ", we'll create the data.");
            }
        }

        try {
            // log.info("Fetching Binance...");
            if (bobo == null)
                bobo = new Bobo2();
            //log.info("pre-fetch : get 15m candles for " + symbol);
            long _startTime = sdf2.parse(startTime).getTime();
            final long oneBarTime = minutes * 60 * 1000L;

            if (bobo.candles.size() > 0)
                _startTime = bobo.candles.get(bobo.candles.size()-1).openTime + oneBarTime;


            _endTime = ((int)(_endTime /oneBarTime)) * oneBarTime;

            // long _endTime = endTimeStr != null ? sdf2.parse(endTimeStr).getTime() : System.currentTimeMillis();

            boolean added = false;
            while (_startTime < _endTime) {
                long endTime = _startTime + oneBarTime * 1499;
                log.info("Start time: " + _startTime);
                CandleRequest req = new CandleRequest(symbol, minutes + "m", _startTime, 0);
                req.limit = 1500;
                List<CandleResp> histF = binancer.getCandles(true, req);
                bobo.add(histF);
                added = true;
                _startTime = endTime;
            }

            if (added)
                checkAndExtendklinesWithLongShortRatio(symbol, bobo, fname, minutes);

            //log.info("Writing data to file " + fname + " ...");
            if (added && useFile) {
                try {
                    writeKryoAtomically(fname, bobo);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Throwable t)
        {
            t.printStackTrace(System.out);
        }
        return bobo;
    }

    private void addTestFile(String file, Map<String, Boolean> fileMap, List<String> filse)
    {
        fileMap.put(file, false);
        filse.add(file);
    }

    /**  */
    private int predict(ProboyTrade pt, BasicNetwork network)
    {
        if (pt == null)
            return -1;
        return (int) (100.0 * network.compute(pt.getMLInputs()).getData(0));
    }

    ChaseCache chaseCache = null;
    String chaseCacheFname = "_chaseCache.kryo";
    private ChaseCache initChaseCache()
    {
        // TODO: попробовать загрузить с диска
        Kryo kryo = getKryo();
        try (Input input = new Input(new FileInputStream(chaseCacheFname))) {
            chaseCache = (ChaseCache)kryo.readClassAndObject(input);
            log.info("loaded " + chaseCacheFname);
            return chaseCache;
        } catch (Exception e) {
            // e.printStackTrace();
            log.info("Didn't load " + chaseCacheFname);
        }
        chaseCache = new ChaseCache();
        return chaseCache;
    }
    /**  */
    private synchronized void saveChaseCache(boolean force)
    {
        if (chaseCache != null && chaseCache.changed
                && (force || System.currentTimeMillis() - chaseCache.lastSaved > 60_000))
        {
            try {
                writeKryoAtomically(chaseCacheFname, chaseCache);
            } catch (Exception e) {
                e.printStackTrace();
            }
            chaseCache.lastSaved = System.currentTimeMillis();
            chaseCache.changed = false;
        }
    }

    /**  */
    private synchronized void refreshChasers(PriceChaser chaserUp, PriceChaser chaserDown, ProboyLine pl)
    {
        if (chaseCache == null) chaseCache = initChaseCache();

        Set<String> chased = chaserUp.getAllSymbols();
        chased.addAll(chaserDown.getAllSymbols());
        for (String symbol : chased) {
            if (chaseCache.imposibleSymbols.contains(symbol))
                continue;
            String key = chaseCache.createKey(symbol, pl.time);
            Double lastPrice = chaseCache.getPrice(key);
            if (lastPrice != null)
            {
                if (lastPrice != Double.NaN) {
                    chaserUp.priceUpdate(symbol, lastPrice, pl.time);
                    chaserDown.priceUpdate(symbol, lastPrice, pl.time);
                }
                continue;
            }
            CandleRequest cr = new CandleRequest(symbol, "1s", pl.time - 1000, pl.time);
            cr.limit = 1;
            Double price = Double.NaN;
            List<CandleResp> candles = null;
            try {
                candles = binancer.getCandles(false, cr);
            } catch (Exception e) {
                if (e.getMessage().equals("Invalid symbol"))
                    chaseCache.saveImposible(symbol);
                continue;
            }
            if (candles != null && candles.size() > 0)
            {
                CandleResp c = candles.get(0);
                price = c.c;
            }
            if (price != Double.NaN) {
                chaserUp.priceUpdate(symbol, price, pl.time);
                chaserDown.priceUpdate(symbol, price, pl.time);
            }
            chaseCache.savePrice(key, price);
        }
        saveChaseCache(false);
    }

    private int calcProportionalMult(int minMult, int maxMult, int minSrc, int maxSrc, int v)
    {
        if (v > maxSrc)
            v = maxSrc;
        double increase = 0;
        if (v > minSrc && maxMult > minMult)
            increase = (v - minSrc) / (maxSrc - minSrc);

        int rv = minMult  +(int)(((double)(maxMult-minMult)) * increase);
        return rv;
    }

    private final ThreadLocal<Kryo> kryoThreadLocal = ThreadLocal.withInitial(this::createKryo);

    /** Создает и настраивает Kryo один раз на поток. */
    private Kryo createKryo()
    {
        Kryo kryo = new Kryo();
        // Отключаем обязательную регистрацию, чтобы не падать при расширении моделей.
        kryo.setRegistrationRequired(false);
        return kryo;
    }

    /** Возвращает потоковый инстанс Kryo. */
    private Kryo getKryo()
    {
        return kryoThreadLocal.get();
    }

    /** Пишет .kryo во временный файл и атомарно подменяет целевой файл. */
    private void writeKryoAtomically(String fileName, Object data) throws IOException
    {
        Path target = Paths.get(fileName);
        Path parent = target.toAbsolutePath().getParent();
        if (parent == null) {
            parent = Paths.get(".");
        }
        String tmpName = target.getFileName().toString() + ".tmp." + Thread.currentThread().getId() + "." + System.nanoTime();
        Path tempFile = parent.resolve(tmpName);

        Kryo kryo = getKryo();
        try (Output output = new Output(new FileOutputStream(tempFile.toFile()))) {
            kryo.writeClassAndObject(output, data);
        }

        try {
            Files.move(tempFile, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tempFile, target, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            Files.deleteIfExists(tempFile);
            throw e;
        }
    }

    /**  */
    private Map<String, ProboyDataContainer> loadDataSet(Map<String, SymbolInfo> symbolInfos,
                                                         Map<String, Boolean> invertByFile, List<String> files) throws Exception
    {
        Map<String, ProboyDataContainer> dataSet = new HashMap<>();
        final String PREFIX_LOG = "StakanService :";
        long _latestSignalTime = 0;
        for (String fName : files ) {
            String kryoFname = fName.substring(0, fName.indexOf(".")) + ".kryo";
            Kryo kryo = getKryo();
            try (Input input = new Input(new FileInputStream(kryoFname))) {
                ProboyDataContainer pdc = (ProboyDataContainer)kryo.readClassAndObject(input);
                dataSet.put(fName, pdc);
                log.info("loaded " + kryoFname);
                continue;
            } catch (Exception e) {
                e.printStackTrace();
            }
            Boolean invert = invertByFile.get(fName);
            Set<Long> eventIds = new HashSet<>();
            try (BufferedReader br = new BufferedReader(new FileReader(fName))) {
                String line;
                List<ProboyLine> data = new ArrayList<>();
                Map<String, Integer> coins = new HashMap<>();
                ProboyLine prevProboyLine = null;
                Map<String, Double> vol24hBySymbol = null;
                while ((line = br.readLine()) != null) {
                    // process the line.
                    ProboyLine pl = new ProboyLine();
                    try {
                        int ind = line.indexOf("====[");
                        if (ind > 0) {
                            String symbolVolumeDatajson = line.substring(ind + 4);
                            vol24hBySymbol = binancer.get24hrStat(symbolVolumeDatajson);
                            continue;
                        }

                        // 2025-03-22 13:09:47.603 ru.bitok.StakanService :   -$- Liquidation SELL (go down) ORCAUSDT  7 USD   price 3.099312 avgPrice 3.132000
                        ind = line.indexOf("-$- Liquidation");
                        if (ind > 0) { // TODO: !!!  сохраним в обычный ProboyLine - флагом того что это ликвидация будет power = -1
                            pl.power = -1;
                            pl.time = sdf4.parse(line.substring(0, 23)).getTime();
                            if (line.indexOf(" BUY ") > 0)
                                pl.up = true;
                            else pl.up = false;
                            ind = line.indexOf(") ");
                            line = line.substring(ind + 2);
                            ind = line.indexOf("  ");
                            pl.symbol = line.substring(0, ind);
                            int ind2 = line.indexOf(" USD ");
                            pl.levelLiq = Integer.parseInt(line.substring(ind + 2, ind2));
                            ind = line.indexOf("avgPrice ");
                            pl.price = Double.parseDouble(line.substring(ind + 9));
                            data.add(pl);
                            continue;
                        }

                        //    -+- Buy interest: 14706 Sell interest: 3608 LL: 74074 DeepBuy i: 450430 DeepSell i: 673175 allMBuy i: 307064383 allMSell i: 239026588 spd: 1090973 allMSpd: 1761047
                        //    -++- HANAUSDT eId 52314 Price: 0.026090 Buy interest: 5890 Sell interest: 2925 id 52620 AggB 4777 AggS 467
                        if (line.indexOf("-+-") > 0)
                        {   // Это доп. инфа, которая всегда идет сразу за сигналом о пробое или отскоке
                            ind = line.indexOf(" Buy interest: ");
                            if (ind > 0) {
                                if (prevProboyLine != null) {
                                    int indSi = line.indexOf(" Sell interest: ");
                                    int indLL = line.indexOf(" LL: ");
                                    prevProboyLine.buyInterest = Integer.parseInt(line.substring(ind + 15, indSi));
                                    if (indLL > 0) {
                                        prevProboyLine.sellInterest = Integer.parseInt(line.substring(indSi + 16, indLL));
                                        ind = line.indexOf(" DeepBuy i: ");
                                        if (ind > 0)
                                        {
                                            prevProboyLine.levelLiq = Integer.parseInt(line.substring(indLL + 5, ind));
                                            int ind2 = line.indexOf(" DeepSell i: ");
                                            prevProboyLine.buyInterestDeep = Integer.parseInt(line.substring(ind + 12, ind2));
                                            ind = ind2;
                                            ind2 = line.indexOf(" allMBuy i: ");
                                            prevProboyLine.sellInterestDeep = Integer.parseInt(line.substring(ind + 13, ind2));
                                            ind = ind2;
                                            ind2 = line.indexOf(" allMSell i: ");
                                            prevProboyLine.buyIntAllmarket = Integer.parseInt(line.substring(ind + 12, ind2));
                                            ind = ind2;
                                            ind2 = line.indexOf(" spd: ");
                                            prevProboyLine.sellIntallMarket = Integer.parseInt(line.substring(ind + 13, ind2));
                                            ind = ind2;
                                            ind2 = line.indexOf(" allMSpd: ");
                                            prevProboyLine.speed = Integer.parseInt(line.substring(ind + 6, ind2));
                                            prevProboyLine.speedAllMarket = Integer.parseInt(line.substring(ind2 + 10));
                                        } else {
                                            prevProboyLine.levelLiq = Integer.parseInt(line.substring(indLL + 5));
                                        }
                                    } else {
                                        prevProboyLine.sellInterest = Integer.parseInt(line.substring(indSi + 16));
                                    }
                                    prevProboyLine = null;
                                    continue;
                                } else {
                                    // Прошла инфа о ликвидностях, но нет ProboyLine к которой можно это привязать
                                    //  - это результат пробоя (без сигнала айсберга) старого отскока
                                }
                            }
                        }
                        //    -++- HANAUSDT eId 52314 Price: 0.026090 Buy interest: 5890 Sell interest: 2925 id 52620 AggB 4777 AggS 467
                        ind = line.indexOf("-++- ");
                        if (ind > 0)
                        {   // Это доп. инфа о старом сигнале, которая идет через 15 сек после сигнала
                            int ind2 = line.indexOf(" eId ");
                            ProboyLine pline = new ProboyLine();
                            pline.time = sdf4.parse(line.substring(0, 23)).getTime();
                            pline.symbol = line.substring(ind + 5, ind2);
                            pline.postSignal = true;
                            ind = ind2;
                            ind2 = line.indexOf(" Price: ");
                            pline.id = Long.parseLong(line.substring(ind+5, ind2));
                            if (!eventIds.remove(pline.id))
                            { // этого ID не было ранее - т.е. это дубль какой то - скипаем..
                                continue;
                            }
                            ind = ind2;
                            ind2 = line.indexOf(" Buy interest: ");
                            pline.price = Double.parseDouble(line.substring(ind+8, ind2));
                            pline.min = pline.max = pline.price;
                            ind = ind2;
                            ind2 = line.indexOf(" Sell interest: ");
                            pline.buyInterest = Integer.parseInt(line.substring(ind+15,ind2));
                            ind = ind2;
                            ind2 = line.indexOf(" id ");
                            pline.sellInterest = Integer.parseInt(line.substring(ind+16, ind2));
                            ind = line.indexOf(" AggB ");
                            ind2 = line.indexOf(" AggS ");
                            pline.agressBuy = Integer.parseInt(line.substring(ind + 6, ind2));
                            ind = ind2;
                            ind2 = line.indexOf(" PM ");
                            if (ind2 < 0)
                                pline.agressSell = Integer.parseInt(line.substring(ind + 6));
                            else {
                                pline.agressSell = Integer.parseInt(line.substring(ind + 6, ind2));
                                pline.priceMove = Double.parseDouble(line.substring(ind2 + 4));
                            }
                            //pline.agressSell = Integer.parseInt(line.substring(ind2 + 6));
                            data.add(pline);
                            //log.info("PL " + pline.id + " " + pline.symbol+ " " + pline.price + " " + pline.buyInterest + " "
                            //      + pline.sellInterest + " " + pline.agressBuy + " " + pline.agressSell + " pm " + pline.priceMove);
                            continue;
                        }

                        prevProboyLine = pl;
                        ind = line.indexOf("coins.put(");
                        if (ind > 0)
                        {
                            int ind2 = line.indexOf(",");
                            String coinName = line.substring(ind + 11, ind2-1);
                            int num = Integer.parseInt(line.substring(ind2+2,line.indexOf(")")));
                            coins.put(coinName, num);
                        }

                        if (line.indexOf(":   bb") > 0) {
                            pl.bybit = true;
                            continue;                           ///  !!!!!!!!! ByBit отключен !!!
                        }

                        // 2024-11-28 11:30:12.245 ru.bitok.StakanService :   P.X -> APTUSDT DOWN LEVEL 12.671900  (level x 30 0ms

                        int proboySignInd = line.indexOf("P ->");
                        int otskokSignId = proboySignInd > 0 ? -1 : line.indexOf("O <-");

                        int adda = 4;
                        if (proboySignInd < 0 && otskokSignId < 0) {
                            proboySignInd = line.indexOf("P.X ->");
                            if (proboySignInd < 0) {
                                proboySignInd = line.indexOf("P.z ->");
                                if (proboySignInd > 0)
                                    pl.futures = false;
                            }

                            if (proboySignInd > 0)
                                otskokSignId = -1;
                            else {
                                otskokSignId = line.indexOf("O.X ->");
                                if (otskokSignId < 0 )
                                {
                                    otskokSignId = line.indexOf("O.z ->");
                                    if (otskokSignId > 0)
                                        pl.futures = false;
                                }
                            }

                            if (proboySignInd > 0 || otskokSignId > 0)
                            {
                                pl.iceberg = true;
                                adda += 2;
                                //     continue;
                            }
                        } else
                        { // Это пробой НЕ Айсберга!
                            // continue;
                        }

                        int symbId = 0;
                        if (proboySignInd > 0) {
                            pl.proboy = true;
                            symbId = proboySignInd + adda;
                        } else if (otskokSignId > 0) {
                            pl.proboy = false;
                            symbId = otskokSignId + adda;
                        } else if (line.indexOf("(level x") > 0)
                        {
                            pl.proboy = null;
                            symbId = line.indexOf(PREFIX_LOG) + PREFIX_LOG.length();
                        } else
                            continue;
                        if (pl.proboy == null && pl.bybit)
                            continue;

                        pl.time = sdf4.parse(line.substring(0, 23)).getTime();
                        _latestSignalTime = pl.time;
                        line = line.substring(symbId).trim();
                        ind = line.indexOf(" ");
                        pl.symbol = line.substring(0, ind).intern();
                        //if (pl.symbol.startsWith("USDC")) continue;
                        pl.volume24h = vol24hBySymbol.get(pl.symbol);
                        pl.volumeAll24h = vol24hBySymbol.get("ALL_MARKET");
                        if (pl.symbol.indexOf("usdt") > 0)
                        {
                            pl.futures = false;
                            pl.symbol = pl.symbol.toUpperCase();
                        }
                        if (line.indexOf(" UP ") > 0)
                            pl.up = true;
                        else if (line.indexOf(" DOWN ") > 0)
                            pl.up = false;
                        ind = line.indexOf(" LEVEL ");
                        line = line.substring(ind + 7).trim();
                        ind = line.indexOf("(level x ");
                        line = line.replace(',', '.');
                        pl.price = Double.parseDouble(line.substring(0, ind).trim());
                        if (pl.proboy != null) {
                            line = line.substring(ind + 9).trim();
                            ind = line.indexOf(" ");
                            pl.power = Integer.parseInt(line.substring(0, ind));
                            int msInd = line.indexOf("ms");
                            pl.ms = Long.parseLong(line.substring(ind + 1, msInd));
                            int llInd = line.indexOf(" LL ");
                            int idInd = line.indexOf(" id ");
                            if (llInd > 0)
                            {
                                if (idInd < 0)
                                    pl.levelLiq = Integer.parseInt(line.substring(llInd + 4));
                                else
                                    pl.levelLiq = Integer.parseInt(line.substring(llInd + 4, idInd));
                            }
                            if (idInd > 0)
                            {
                                // ...111ms id 100674 AggB 24936 AggS 33475
                                int ind2 = line.indexOf(" AggB ");
                                pl.id = Long.parseLong(line.substring(idInd + 4, ind2));
                                eventIds.add(pl.id);
                                ind = ind2;
                                ind2 = line.indexOf(" AggS ");
                                pl.agressBuy = Integer.parseInt(line.substring(ind + 6,ind2));
                                ind = ind2;
                                ind2 = line.indexOf(" PM ");
                                if (ind2 < 0)
                                    pl.agressSell = Integer.parseInt(line.substring(ind + 6));
                                else {
                                    pl.agressSell = Integer.parseInt(line.substring(ind + 6, ind2));
                                    pl.priceMove = Double.parseDouble(line.substring(ind2 + 4));
                                }
                                // log.info("PL " + pl.agressSell + " pm " + pl.priceMove);
                            }
                            ///// ----------------------------------------------- done

                            // invert:
                            if (invert && pl.proboy != null) {
                                pl.up = !pl.up;
                                pl.proboy = !pl.proboy;
                            }
                            pl.min = pl.max = pl.price;
                        } else {
                            int minInd = line.indexOf("min");
                            int maxInd = line.indexOf("max");
                            if (minInd > 0)
                            {
                                pl.min = Double.parseDouble(line.substring(minInd + 3,
                                        maxInd).trim());
                                pl.max = Double.parseDouble(line.substring(maxInd + 3).trim());
                            } else
                                pl.min = pl.max = pl.price;
                        }
                        ////////////////////////////////
                        //Integer rating = topTradedCoins.get(pl.symbol);
                        Integer rating = coins.get(pl.symbol);
                        if (rating == null)
                            continue;
                        pl.ratinng = rating;
                        //if (rating > 190)
                        addSymbol(symbolInfos, pl.symbol);

                        //if (pl.futures)
                        data.add(pl);
                    } catch (Exception e) {
                        // e.printStackTrace();
                        continue;
                    }
                }
                ProboyDataContainer pdc = new ProboyDataContainer(coins, data);
                dataSet.put( fName, pdc);
                log.info("Loaded " + fName + " " + data.size() + " lines");
                try {
                    writeKryoAtomically(kryoFname, pdc);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } // try Reader
        }
        return dataSet;
    }

    /**  */
    private boolean isLevelBig(ProboyLine pl, MMATema levels, int overVolume,
                               int maxOverVolume, int statSize, int hiLevelPercent)
    {
        return overVolume > maxOverVolume
                    && pl.levelLiq > (int)(levels.getMax() * hiLevelPercent / 100.0);
    }

    private static int getRRRRbyPer(double corelKoof, int n_short)
    {
        if (corelKoof > n_short)
            return 28;
        else return 16;
    }

    /**  */
    private boolean isSymbolTrading(Map<String, SymbContainer> scCont, String symbol)
    {
        SymbContainer sc = scCont.get(symbol);
        if (sc != null && sc.symTradeStat.didBuy != null)
            return true;
        return false;
    }

    /**  */
    private void testProboys()
    {
        initTopTradedCoinsForTests();

        try {

            Map<String, Boolean> invertByFile = new HashMap<>();
   /*
            invertByFile.put("yoka10.log", false); // отсюда есть min max
            invertByFile.put("yoka11.log", false);
            invertByFile.put("yoka12.log", false);
            invertByFile.put("yoka13.log", false);
            invertByFile.put("yoka14.log", false);
            invertByFile.put("yoka15.log", false);
            invertByFile.put("yoka16.log", false);  // отсюда есть СПОТ
        //  invertByFile.put("y", false);
        //  invertByFile.put("yy", false);
            invertByFile.put("yoka17.log", false);
            invertByFile.put("yoka18.log", false);
            invertByFile.put("yoka19.log", false);
            invertByFile.put("yoka20.log", false);
*/ /*
            // ---------------------
            invertByFile.put("yoka21.log", false); // отсюда включительно - расчет среднего уровня по новому (не менее 10 мин)
            invertByFile.put("yoka24.log", false);
            invertByFile.put("yoka25.log", false);

         //   invertByFile.put("zoki23.log", false);  // это последий zoki с power >= 20. после него power c 15
            invertByFile.put("yoka26.log", false);   // всего пол дня       - отсюда нет СПОТа
            invertByFile.put("yoka27.log", false);  // всего пол дня
          //  invertByFile.put("zoki24.log", false);
          //  invertByFile.put("zoki25.log", false); // это последний zoki с 10 минутами. после него 20 минутные ММА

            invertByFile.put("yoka28.log", false);
            invertByFile.put("yoka29.log", false);

            invertByFile.put("yoka30.log", false);
            invertByFile.put("yoka31.log", false);
            // отсюда начался 11.2024
            invertByFile.put("yoka32.log", false);
            // invertByFile.put("y32.log", false); // TMP - одновременный с zoki26.log
        //    invertByFile.put("zoki26.log", false); // Одновременный с y32

            invertByFile.put("yoka33.log", false);      // sync zoki27
     //       invertByFile.put("zoki27.log", false);      // sync yoka33
     //       invertByFile.put("zoki28.log", false);      // sync yoka33
            //      invertByFile.put("zoki29.log", false);      // sync yoka33
            invertByFile.put("zoki30.log", false);      //

            invertByFile.put("out.log", false);         // отсюда есть Bybit
            invertByFile.put("zoki33.log", false);      //
            invertByFile.put("zoki34.log", false);      //
            invertByFile.put("yoka35.log", false);      //

            invertByFile.put("yoka36.log", false);      //
            invertByFile.put("yoka37.log", false);      //
            invertByFile.put("yoka38.log", false);      //
            invertByFile.put("zoki35.log", false);      //

*/
            // странный блок!
            /*
            invertByFile.put("yoka39.log", false);      // ---- Отсюда есть - iceberg -   11.27         14 часов
            invertByFile.put("zoki36.log", false);      // всего 3 часа
            invertByFile.put("zoki37.log", false);      // сутки
            invertByFile.put("zoki38.log", false);      // 20 часов
            invertByFile.put("zoki39.log", false);      // сутки до 01.12 12:00

            invertByFile.put("zoki40.log", false);      //
            invertByFile.put("zoki41.log", false);      //
            invertByFile.put("zoki42.log", false);      //
            invertByFile.put("zoki43.log", false);      //


            invertByFile.put("zoki44.log", false);      //  с 10 по 16 число - 6 дней
            invertByFile.put("zoki45.log", false);      //
            invertByFile.put("zoki46.log", false);      //
            invertByFile.put("zoki47.log", false);      //    12 часов

            invertByFile.put("zoki48.log", false);      // блок с 12.16 по 06.01 ~~~ 21 день
            invertByFile.put("zoki49.log", false);      //
            invertByFile.put("zoki50.log", false);      //
            invertByFile.put("zoki51.log", false);      //
*/  /*
            invertByFile.put("zoki52.log", false);      // блок примерно с 6 по 20 ~~~ 14 дней
            invertByFile.put("zoki53.log", false);      //
            invertByFile.put("zoki54.log", false);      //
            invertByFile.put("zoki55.log", false);      //

            invertByFile.put("zoki56.log", false);      //
            invertByFile.put("zoki57.log", false);      // 17 часов
            invertByFile.put("zoki58.log", false);

            invertByFile.put("zoki59.log", false); // отсюда есть ближайшая ликвидность
            invertByFile.put("zoki60.log", false);*/
            ///////////////////////////////////////////////////////////////////////////////////////////////////////////

            List<String> files = new ArrayList<>();
          // 61 -> 100 = 5.5 months
            /*
            addTestFile("zoki61.log", invertByFile, files); // отсюда есть Level Liquidity в USD     26.01.2025 15:15
            addTestFile("zoki62.log", invertByFile, files);

            addTestFile("zoki63.log", invertByFile, files);
            addTestFile("zoki64.log", invertByFile, files);
            addTestFile("zoki65.log", invertByFile, files);
            addTestFile("zoki66.log", invertByFile, files);
            addTestFile("zoki67.log", invertByFile, files);
            addTestFile("zoki68.log", invertByFile, files); //
            addTestFile("zoki69.log", invertByFile, files); //
            addTestFile("zoki70.log", invertByFile, files); //
            addTestFile("zoki71.log", invertByFile, files);  //  эти два файла с большим гепом!
            addTestFile("zoki72.log", invertByFile, files);  //
            addTestFile("zoki73.log", invertByFile, files);  //

            addTestFile("zoki74.log", invertByFile, files);  // отсюда есть  - LSR -
            addTestFile("zoki75-1.log", invertByFile, files);//  7 часов
            addTestFile("zoki75.log", invertByFile, files);  //

            addTestFile("zoki76.log", invertByFile, files);  //
            addTestFile("zoki77.log", invertByFile, files);  // 21 час
            addTestFile("zoki78.log", invertByFile, files);  //

            addTestFile("zoki79.log", invertByFile, files);  // 9.5 часов
            addTestFile("zoki80.log", invertByFile, files);  //
            addTestFile("zoki81.log", invertByFile, files);  //
            addTestFile("zoki82.log", invertByFile, files);  // отсюда есть ликвидации (22.03)

            addTestFile("zoki83.log", invertByFile, files);  //
            addTestFile("zoki84.log", invertByFile, files);  //
            addTestFile("zoki85.log", invertByFile, files);  //

            addTestFile("zoki86.log", invertByFile, files);  //
            addTestFile("zoki87.log", invertByFile, files);  //
            addTestFile("zoki88.log", invertByFile, files);  //

            addTestFile("zoki89.log", invertByFile, files);  //
            addTestFile("zoki90.log", invertByFile, files);  //
            addTestFile("zoki91.log", invertByFile, files);  //

            addTestFile("zoki92.log", invertByFile, files);  //
            addTestFile("zoki93.log", invertByFile, files);  //
            addTestFile("zoki94.log", invertByFile, files);  //
            addTestFile("zoki95.log", invertByFile, files);  //

            addTestFile("zoki96.log", invertByFile, files);  //
            addTestFile("zoki97.log", invertByFile, files);  //
            addTestFile("zoki98.log", invertByFile, files);  //
            addTestFile("zoki99.log", invertByFile, files);  //
            addTestFile("zoki100.log", invertByFile, files);  // SHORT
*/
  ////   101 -> 111 = 5 months

            //addTestFile("zoki101.log", invertByFile, files);  // 7 days
            //addTestFile("zoki102.log", invertByFile, files);  // 10 days

            // addTestFile("zoki103.log", invertByFile, files);  //  1 month

            //addTestFile("zoki104.log", invertByFile, files);  // 7 days

            //addTestFile("zoki105.log", invertByFile, files);  // 6 days

            //addTestFile("zoki106.log", invertByFile, files);  // 23 days

            //addTestFile("zoki107.log", invertByFile, files);  // 1 month

            //addTestFile("zoki108.log", invertByFile, files);  // 1 day
            //addTestFile("zoki109.log", invertByFile, files);  // 2 days
            // addTestFile("zoki110.log", invertByFile, files);  // 7 days
            // addTestFile("zoki111.log", invertByFile, files);  // 16 days

          //////////// 112 -> ... = 5 months

            //addTestFile("zoki112.log", invertByFile, files);  //  После этого - обрыв на 11+ часов.    2.5 days
            addTestFile("zoki113.log", invertByFile, files);  // 6 days
            /*
            addTestFile("zoki114.log", invertByFile, files);  //  отсюда включительно есть PriceMove    1 day
            addTestFile("zoki115.log", invertByFile, files);  // 1 day
            addTestFile("zoki116.log", invertByFile, files);  // 1 day
            */
            addTestFile("zoki117.log", invertByFile, files);  // 5 days
            addTestFile("zoki118.log", invertByFile, files);  // 2 days
            addTestFile("zoki119.log", invertByFile, files);  // 4 days
            addTestFile("zoki120.log", invertByFile, files);  // 2 days
            addTestFile("zoki121.log", invertByFile, files);  // 4 days
            /*
            addTestFile("zoki122.log", invertByFile, files);  // 1 day
            addTestFile("zoki123.log", invertByFile, files);  // 1 day
            addTestFile("zoki124.log", invertByFile, files);  // 1.5 days  -> BIG DELAY
            */
            addTestFile("zoki125.log", invertByFile, files);  // 7 days
            addTestFile("zoki126.log", invertByFile, files);  // 2 days
            addTestFile("zoki127.log", invertByFile, files);  // 4 days
            addTestFile("zoki128.log", invertByFile, files);  // 3 days
            addTestFile("zoki129.log", invertByFile, files);  // New Year 29..13   - 15 days
            addTestFile("zoki130.log", invertByFile, files);  // DEEP      7 days
/*
            addTestFile("zoki131.log", invertByFile, files);  // 2 days
            addTestFile("zoki132.log", invertByFile, files);  // 4 days

            addTestFile("zoki133.log", invertByFile, files);  // 2 days    -> BIG DELAY

            addTestFile("zoki134.log", invertByFile, files);  // 2 days
            addTestFile("zoki135.log", invertByFile, files);  // 2 days
            */
            //addTestFile("zoki136.log", invertByFile, files);  // ..30days

            //addTestFile("zoki137.log", invertByFile, files);  // 10 days  - all coins
/*
            addTestFile("zoki138.log", invertByFile, files);  // ~1 day      -> BIG DELAY
            addTestFile("zoki139.log", invertByFile, files);  // 2 days
*/
            addTestFile("zoki140.log", invertByFile, files);  // 12+ day
/*
            addTestFile("zoki141.log", invertByFile, files);  //  1 day DELAY before
            addTestFile("zoki142.log", invertByFile, files);  //  3 days
            */
            addTestFile("zoki143.log", invertByFile, files);  //

            Map<String, SymbolInfo> symbolInfos = new HashMap<>();
            Map<String, ProboyDataContainer> dataSet = loadDataSet(symbolInfos, invertByFile, files);
            List<ProboyLine> _lines = dataSet.get(files.get(files.size()-1)).dataSet;
            final long latestSignalTime = _lines.get(_lines.size()-1).time;

            Map<Double, String> descByProfit = new ConcurrentHashMap<>();
            Map<Double, String> descBySortino = new ConcurrentHashMap<>();
            Map<String, Double> profByDesc = new ConcurrentHashMap<>();
            Map<String, List<CandleResp>> klinesBySymbol = new ConcurrentHashMap<>();
            Counters counters = new Counters();

            List<TestParams> testParams = new ArrayList<>();
            final boolean printDetails = true;
            final boolean PRINT_FILES = false;
            PLUS_PNL_MODE = false;       // Если TRUE, то сделка открывается в прибыльную сторону.
                                        // Если FALSE то сделка открывается по допустимой дельте BUY_SELL_DELTA...

            Map<String, StringDouble> bestSymbProfits = new HashMap<>();
            final boolean PRINT_TOP_OF_SYMBOLS = false;
            final boolean GET_NN_DATA = false;

            // 1. Загрузка обученной сети
            File networkFile = new File("NN_in_data_1739206695212.trained");
            BasicNetwork network = (BasicNetwork) EncogDirectoryPersistence.loadObject(networkFile);

            final double WA_SLOZH_PERCENT_EXTRA_KOOF = 1; // во сколько раз обзий баланс д.б. больше торгуемой суммы если НЕ слож. процент
            final boolean SLOZH_PERCENT = true;
            final boolean MAKE_CSV = false;          // готовим CSV для аналитики на таймлайне
            final boolean FILL_DB = false;           // пишем события в БД сразу, без привязки к свечам
            final int DB_EVENTS_BATCH = 1000;
            final String CSV_SYMBOL = "BTCUSDT";
            final String CSV_TIMEFRAME = "15m";
            final double CRITICAL_LOSS = 30;    // 20
            final double PROSADKA = 1.5;       // 1.2 Во сколько раз максимально можно просесть за 1 интервал

            final long CHUNK_LENGTH = 1 * 1 * 24 * 60 * 60_000L; // 7 дней

            final Map<String, Double> tradeAmntWASlozhPercentGlobal = new ConcurrentHashMap<>();
            final Map<String, Double> maxLossbyDesc = new ConcurrentHashMap<>();

            final boolean LEARN_MODE = false;
            final boolean USE_ML = false;   // торговать или нет по обученной модели
            List<LearnData> allLearnData = new ArrayList<>();
            AiSignalService aiService = new AiSignalService();

                for (int porog = 0; porog <= 0; porog += 1) // 20 default
                for (int martin = 0; martin <= 0; martin += 1)

                for (int PWL = 5; PWL <= 5; PWL += 5)       // sec delay intraSymbol
                for (int PWL2 = 0; PWL2 <= 0; PWL2 += 1)    // AVR2 (close) 4
                for (int PWL3 = 90; PWL3 <= 90; PWL3 += 30)     // max nn 80
                for (int PWL4 = 20; PWL4 <= 20; PWL4 += 10)       // 15
                                                for (int fastN = 20; fastN <= 20; fastN += 10)
                                                    for (int slowN = fastN+10; slowN <= fastN+10; slowN += 10) // 4 - 7
                //for (int numBarsUnder = 28; numBarsUnder <= 28; numBarsUnder ++)
                //for (int sb = 0; sb <= 0; sb += 10)
                for (int POWER_LIMIT = 16; POWER_LIMIT <= 24; POWER_LIMIT += 4)             // 18 +
                    for (int POWER_LIMIT_SHORT = 20; POWER_LIMIT_SHORT <= 50; POWER_LIMIT_SHORT += 10)
                    for (int POWER_LIMIT_S = 10; POWER_LIMIT_S <= 10; POWER_LIMIT_S += 2)   // close - 28
                        for (int POWER_LIMIT_S2 = 20; POWER_LIMIT_S2 <= 50; POWER_LIMIT_S2 += 10)
                for (int n = 15; n <= 15; n += 5)     // сколько минут назад проверяем объем всего рынка 15
                for (int n_short = 60; n_short <= 100; n_short += 20)     // 17 ?
                for (int n2 = 22; n2 <= 22; n2 += 2)  // Число баров для оценки Волатильности   16
                for (int nn = 6; nn <= 6; nn += 1)  // фильтр по волатильности 4
                for (int ms = 44_500; ms <= 44_500; ms += 40_000) // Время сбора   .LevelLiq
                    for (int d = 8; d <= 8; d += 2)        //  2
                        for (int d2 = 0; d2 <= 0; d2 += 10)        // Число баров 20
                for (int x = 1; x <= 2; x += 1)         // в какой дельте комиссий собираем ЛИКВИДНОСТЬ уровеня

                for (int x2 = 1; x2 <= 1; x2 += 1)      // в какой дельте комиссий оцениваем ЦЕНОВОЙ уровень (для 'd') 3
                for (int z = 300; z <= 300; z += 100)     // 300 - zakis dec.
                for (int r = 300; r <= 300; r += 100)    // 600
                for (int r2 = 20; r2 <= 20; r2 += 10)      // 20  --30--
                    for (int rr = 7000; rr <= 7000; rr += 200)  //
                        for (int rrr = 8000; rrr <= 8000; rrr += 200)  // 1000
                            for (int rrrr = -15; rrrr <= -15; rrrr += 1)  // -4
                                for (int n3 = 30; n3 <= 30; n3 += 10)  // 18
                for (int ra = 0; ra <= 0; ra += 1)      // 0

                for (int speedFrame = 24; speedFrame <= 24; speedFrame += 5)    // окно замера средней скорости в минутах
                for (int spdKoof = 1; spdKoof <= 1; spdKoof += 5)    // Отклонение скорости в процентах

                            for (int LUSD = 10_000; LUSD <= 10_000; LUSD += 1_000)       // Level USD  80 000
                    for (int USD = 10_000; USD <= 10_000; USD += 5_000)   // Level USD - close  70 000
                for (int quants = 2; quants <= 2; quants += 1)
                for (int maxParallelDeals = 10; maxParallelDeals <= 10; maxParallelDeals += 2) // 14
                for (int tp = 100; tp <= 100; tp += 20) //        120
                for (int sl = 70; sl <= 70; sl += 20) //        90
                        for (int tpO = 100; tpO <= 100; tpO += 20) // 90
                        for (int slO = 70; slO <= 70; slO += 20) // 80
                for (int tpCnt = 9; tpCnt <= 9; tpCnt += 4)     // 1
                    for (int tpCntB = 9; tpCntB <= 9; tpCntB += 4)     // 1-3
                for (int slCnt = tpCnt; slCnt <= tpCnt; slCnt += 1)
                for (int nl = 0; nl <= 0; nl += 20) // 10
                for (int topCoins = 4_400_000; topCoins <= 8_400_000; topCoins += 1_000_000 )
                for (int tpCnt2start = tpCnt<2?1 : 2 ; tpCnt2start <= (tpCnt < 2?1 : 2); tpCnt2start += 1) // 2
                    for (int tpCnt2startB = tpCntB<2?1 : 2 ; tpCnt2startB <= (tpCntB < 2?1 : 2); tpCnt2startB += 1) // 2
                for (int tpCnt2startS = 2; tpCnt2startS <= 2; tpCnt2startS += 1)
                for (int mult = 30; mult <= 30; mult += 5 ) // 250
                    for (int maxMult = mult; maxMult <= mult; maxMult += 100)
                for (int profPercentToClose = 3000; profPercentToClose <= 3000; profPercentToClose += 3 ) // 4 при mult=100!
                for (int buySellDelta = maxParallelDeals; buySellDelta <= maxParallelDeals; buySellDelta += 2) // 10
                for (int CAP = 260; CAP <= 260; CAP += 40) // 5-6 десятая доля процента  (50=5%)    180
                  for (int CAL = 30000; CAL <= 30000; CAL += 50) // 5-6 десятая доля процента  (50=5%)
                for (int proboyOtskDelta = maxParallelDeals; proboyOtskDelta <= maxParallelDeals; proboyOtskDelta += 1) // 3
                for (int stage = 1; stage <= 1; stage ++) // 0 -> файлы раздельно, 1 - все вместе
                for (int mixer = 0; mixer <= 0; mixer ++)
                for (int maxHours = 200; maxHours <= 200; maxHours += 24) // 15 !
                {
                    TestParams testParam = new TestParams();
                    testParam.maxHours = maxHours;
                    testParam.quants = quants;
                    testParam.sb = 0;
                    testParam.maxParallelDeals = maxParallelDeals;
                    testParam.mult = mult;
                    testParam.maxMult = maxMult;
                    testParam.tp = tp;
                    testParam.sl = sl;
                    testParam.tpO = tpO;
                    testParam.slO = slO;
                    testParam.tpCnt = tpCnt;
                    testParam.tpCntB = tpCntB;
                    testParam.slCnt = slCnt;
                    testParam.slowN = slowN;
                    testParam.fastN = fastN;
                    testParam.ms = ms;
                    testParam.nl = nl;
                    testParam.n = n;
                    testParam.n2 = n2;
                    testParam.n3 = n3;
                    testParam.n_short = n_short;
                    testParam.CAP = CAP;
                    testParam.CAL = CAL;
                    testParam.POWER_LIMIT = POWER_LIMIT;
                    testParam.POWER_LIMIT_SHORT = POWER_LIMIT_SHORT;
                    testParam.POWER_LIMIT_S = POWER_LIMIT_S;
                    testParam.POWER_LIMIT_S2 = POWER_LIMIT_S2;
                    testParam.spedKoof = spdKoof;
                    testParam.speedFrame = speedFrame;
                    testParam.r = r;
                    testParam.r2 = r2;
                    testParam.ra = ra;
                    testParam.rr = rr;
                    testParam.rrr = rrr;
                    testParam.rrrr = rrrr;
                    testParam.d = d;
                    testParam.d2 = d2;
                    testParam.porog = porog;
                    testParam.topCoins = topCoins;
                    testParam.x = x;
                    testParam.x2 = x2;
                    testParam.z = z;
                    testParam.tpCnt2start  = tpCnt2start;
                    testParam.tpCnt2startS = tpCnt2startS;
                    testParam.tpCnt2startB = tpCnt2startB;
                    testParam.PWL = PWL;
                    testParam.PWL2 = PWL2;
                    testParam.PWL3 = PWL3;
                    testParam.PWL4 = PWL4;
                    testParam.USD = USD;
                    testParam.LUSD = LUSD;
                    testParam.martin = martin;
                    testParam.buySellDelta = buySellDelta;
                    testParam.proboyOtskDelta = proboyOtskDelta;
                    testParam.profPercentToClose = profPercentToClose;
                    testParam.stage = stage;
                    testParam.nn = nn;
                    testParam.mixer = mixer;
                    testParams.add(testParam);
                    counters.totalCnt++;
                }
                log.info("Starting test " + counters.totalCnt + " tasks.");
                testParams.parallelStream().forEach(pa ->
                {
                    String desc = "maxParallelDeals " + pa.maxParallelDeals
                             + " L-USD " + pa.LUSD + " mult " + pa.mult + " q " + pa.quants + " PWL " + pa.PWL
                            + " tp " + pa.tp + " tpo " + pa.tpO + " sl " + pa.sl + " slO " + pa.slO
                            + " tpCnt " + pa.tpCnt + " tpCntS " + pa.tpCnt2start
                            + " tpCntB " + pa.tpCntB + " tpCntSB " + pa.tpCnt2startB
                            + " powLim " + pa.POWER_LIMIT
                            + " powLim2 " + pa.POWER_LIMIT_S
                            + " r " + pa.r + " rr " + pa.rr + " rrr " + pa.rrr + " r2 " + pa.r2 + " -n- " + pa.n
                            + " USD " + pa.USD + " PWL3(max nn) " + pa.PWL3 + " PWL4 " + pa.PWL4
                            + " nn " + pa.nn
                            + " CAP " + pa.CAP + " x " + pa.x + " n2 " + pa.n2
                            + " buySellDelta " + pa.buySellDelta + " ms " + pa.ms + " n_short " + pa.n_short
                            + " maxHours  " + pa.maxHours
                            + " topC " + pa.topCoins + " n3 " + pa.n3 + " z " + pa.z + " pos2 " + pa.POWER_LIMIT_S2
                            + " pos3 " + pa.POWER_LIMIT_SHORT;
                    log.info(desc);

                    // Стартовый баланс при торговле БЕЗ сложного процента
                    double tradeamntWASlozhPercent = 1000.0;

                    // капитал, КОТОРЫМ ТОРГУЕМ БЕЗ СЛОЖНОГО ПРОЦЕНТА
                    if (tradeAmntWASlozhPercentGlobal.get(desc) == null)
                        tradeAmntWASlozhPercentGlobal.put(desc, tradeamntWASlozhPercent);

                    double allTradeAmnt = tradeamntWASlozhPercent;

                    // Общий накопленный капитал - со сложным процентом и без него
                    if (TradeStat.tradeAmntGlobalByKey.get(desc) == null)
                        TradeStat.tradeAmntGlobalByKey.put(desc,  allTradeAmnt);

                    int tradeCnt = 0;

                    Double maxLoss = 0.0;

                    MMA avgProfPercent = new MMA(100);

                    Map<String, Double> sumProfitBySymbol = new HashMap<>();

                    Map<String, SymbContainer> scBySymb = null;
                    TotalLossInfo totalLossInfo = null;
                    double startTradeAmnt = 0;      // Весь капитал! (а не то чем торгуем)
                    double startTradeAmntMax = 0;   // макс. весь капитал
                    int capCnt = 0;

                    long chunkStartTime = 0;
                    int ccnt = 0;
                    long lastDealTime = 0;

                    List<LearnData> history = new ArrayList<>();
                    CandleResp prevCr = null;

                    List<Event> events = new ArrayList<>();

                    for (String fName : files)
                    { // перебор блоков данных за разные периоды
                        ProboyDataContainer proboyDataContainer = dataSet.get(fName);
                        List<ProboyLine> data = proboyDataContainer.dataSet;
                        /*int cnt = 0;
                        for (String coin : proboyDataContainer.coins.keySet())
                            if (bybitCoins.contains(coin)) cnt++;
                        log.info(fName + " coins: " + proboyDataContainer.coins.size() + " bybit sync: " + cnt);
                        */
                        if (scBySymb == null || pa.stage == 0)
                        {
                            scBySymb = new HashMap<>();
                            totalLossInfo = new TotalLossInfo();
                            startTradeAmnt = TradeStat.tradeAmntGlobalByKey.get(desc);
                        }

                        if (TradeStat.tradeAmntGlobalByKey.get(desc) > startTradeAmntMax)
                            startTradeAmntMax = TradeStat.tradeAmntGlobalByKey.get(desc);

                        MMATema _upP = new MMATema(1, pa.n * 60 * 1000L);
                        MMATema _downP = new MMATema(1, pa.n * 60 * 1000L);
                        MMATema _upP2 = new MMATema(1, (pa.n + pa.r2) * 60 * 1000L);
                        MMATema _downP2 = new MMATema(1, (pa.n + pa.r2) * 60 * 1000L);

                        MMATema _upPot = new MMATema(1, pa.n * 60 * 1000L);
                        MMATema _downPot = new MMATema(1, pa.n * 60 * 1000L);
                        MMATema _upP2ot = new MMATema(1, (pa.n + pa.r2) * 60 * 1000L);
                        MMATema _downP2ot = new MMATema(1, (pa.n + pa.r2) * 60 * 1000L);

                        MMATema _upIce = new MMATema(1, pa.n * 60 * 1000L);
                        MMATema _downIce = new MMATema(1, pa.n * 60 * 1000L);
                        MMATema _upP2Ice = new MMATema(1, (pa.n + pa.r2) * 60 * 1000L);
                        MMATema _downP2Ice = new MMATema(1, (pa.n + pa.r2) * 60 * 1000L);

                        MMATema allMarketSpeed = new MMATema(1, pa.speedFrame * 60 * 1000L);

                        MMATema eventCounter = new MMATema(1, 15 * 1000L);

                        // Суммы всех айсбергов (разобранных объемов вверх и вних по всем моентам)
                        MMATema _upPI = new MMATema(pa.nn, pa.n * 1200 * 1000L);
                        MMATema _downPI = new MMATema(pa.nn, pa.n * 1200 * 1000L);

                        MMATema _buyers = new MMATema(pa.nn, pa.n_short * 60 * 1000L);
                        MMATema _sellers = new MMATema(pa.nn, pa.n_short * 60 * 1000L);
                        MMATema _buyersFast = new MMATema(pa.nn, pa.n_short * 60 * 1000L / 4);
                        MMATema _sellersFast = new MMATema(pa.nn, pa.n_short * 60 * 1000L / 4);

                        MMATema _upPshort = new MMATema(pa.nn, pa.n_short * 60 * 1000L);
                        MMATema _downPshort = new MMATema(pa.nn, pa.n_short * 60 * 1000L);

                        MMA avgUppDwn = new MMA(10);
                        MMA avgUppDwnOt = new MMA(10);
                        MMA avgUppDwnIce = new MMA(10);

                        int mixerCounter = 0;

                        MMA avgVolatile = new MMA(50);

                        Map<Long, Double> upIds = new HashMap<>();
                        Map<Long, Long> powerById = new HashMap<>();
                        Map<Long, Double> downIds = new HashMap<>();
                        //MMATema upPostSignals = new MMATema(1, pa.r2 * 1_000L);
                        //MMATema downPostSignals = new MMATema(1, pa.r2 * 1_000L);

                        MMATema upPostSignalsL = new MMATema(1, pa.d * 1_000L           );
                        MMATema downPostSignalsL = new MMATema(1, pa.d * 1_000L         );
                        PriceChaser chaserUp = new PriceChaser(pa.n_short * 1000L);
                        PriceChaser chaserDown = new PriceChaser(pa.n_short * 1000L);
                        //PriceChaser chaserUp2 = new PriceChaser(pa.n_short * 500L); // short
                        //PriceChaser chaserDown2 = new PriceChaser(pa.n_short * 500L); // short

                        long AGG_PER = pa.n_short * 60_000L;

                        MMATema aggB = new MMATema(1, AGG_PER);
                        MMATema aggS = new MMATema(1, AGG_PER);
                        MMATema aggPM = new MMATema(1, AGG_PER);

                        //Set<LearnData> localLearnData = new HashSet<>();
                        Map<String, List<LearnData>> tradesBySymbol = new HashMap<>();

                        Boolean ALL_UP = null;
                        long ALL_UP_TIME = 0;

                        Map<String, List<Event>> dbEventsBySymbol = new HashMap<>();
                        int dbEventsBuffered = 0;

                        MMATema overAvg = new MMATema(1,pa.POWER_LIMIT_S2 * 60_000L);

                        MMA avgAvg = new MMA(1000);

                    try {
                    for (ProboyLine pl : data) {
                        if (pl.symbol.startsWith("USDCUSDT")
                                //SKIPED.contains(pl.symbol)
                                ) continue;

                        //if (pl.postSignal) continue;

                        if (pl.time - chunkStartTime > CHUNK_LENGTH)
                        {
                            // Завершился чанк теста или просто это начала самого первого чанка
                            TradeStat.addChunkProfit(desc, TradeStat.tradeAmntGlobalByKey.get(desc),
                                    tradeAmntWASlozhPercentGlobal.get(desc) * totalLossInfo.getAvgPNL() / 100.0);
                            chunkStartTime = pl.time;
                        }

                        int ma1 = 0, ma2 = 0, ma3 = 0, ma4 = 0;
                        int l1 = 0, l2 = 0, l3 = 0, l4 = 0, l5 = 0, l6 = 0;
                        int smooth = 1;
                        int suxxes = 100;
                        int minProfitToClose = 100;
                        int startTpPercent = 50;
                        int criticalLossToClosePart = 55;

                        if (!pl.futures) continue;
                        if (pl.power < 0) continue;

/*
                        if ( pl.volume24h < pa.topCoins
                                   && !isSymbolTrading(scBySymb, pl.symbol)
                            )
                            continue; */

                        // Сохраним ID и цену, чтобы отследить подтверждения
                        if (!pl.postSignal && pl.proboy != null
                                //&& !pl.iceberg
                                //&& pl.proboy // ?
                                //&& pl.power > 50 // ?
                        )
                        { /*
                            if (    pl.up && pl.proboy
                                    ||
                                    !pl.up && !pl.proboy
                            ) {
                                //if (pl.proboy && pl.power > 50)
                                  //  chaserUp.chasePrice(pl.symbol, pl.price, pl.time, pl.price);
                                upIds.put(pl.id, pl.price);
                            } else if (
                                    !pl.up && pl.proboy
                                    ||
                                    pl.up && !pl.proboy
                            ) {
                                //if (pl.proboy && pl.power > 50)
                                  //  chaserDown.chasePrice(pl.symbol, pl.price, pl.time, pl.price);
                                downIds.put(pl.id, pl.price);
                            } */
                        } else if (pl.postSignal) {
                            // Это postSignal
                            /*Double price = upIds.remove(pl.id);
                            //Long power = powerById.remove(pl.id);
                            if (price != null)
                            { // POST signal движения вверх
                                //chaserUp.chasePrice(pl.symbol, pl.price, pl.time, pl.price);
                                if (pl.price >= price) {
                                    upPostSignalsL.addStat(1, pl.time);
                                } else if (pl.price < price) {
                                    upPostSignalsL.addStat(-1, pl.time);
                                }
                            } else
                            {
                                price = downIds.remove(pl.id);
                                if (price != null)
                                { // POST signal движения вниз
                                    //chaserDown.chasePrice(pl.symbol, price, pl.time, price);
                                    if (pl.price <= price)
                                    {
                                        downPostSignalsL.addStat(1, pl.time);
                                    } else if (pl.price > price) {
                                        downPostSignalsL.addStat(-1, pl.time);
                                    }
                                }
                            }*/
                        }

                        // плохой тест был с этим с 18 марта
                        if (pl.volume24h < pa.topCoins
                                    && !isSymbolTrading(scBySymb, pl.symbol))
                            continue;

                        SymbContainer sc = scBySymb.get(pl.symbol);
                        if (sc == null) {
                            sc = new SymbContainer(pl.symbol);

                            // Загрузка KLINES
                            {
                                pl.symbol = pl.symbol.intern();

                                synchronized (pl.symbol) {
                                    List<CandleResp> klines = klinesBySymbol.get(pl.symbol);
                                    if (klines == null) {
                                        Bobo2 klineContainer = loadCandlesBinance("2025-01-26 00:00:00",
                                                latestSignalTime, pl.symbol, true, 15);
                                        klines = klineContainer.candles;
                                        klinesBySymbol.put(pl.symbol, klineContainer.candles);
                                        //log.info(pl.symbol + " last kline: " + new Date(klines.get(klines.size() - 1).openTime));
                                    }
                                    sc.setKlines(klines);
                                }
                            }

                            scBySymb.put(pl.symbol, sc);
                            sc.symTradeStat = new TradeStat(pl.symbol, TradeStat.tradeAmntGlobalByKey.get(desc), ma1, ma2, ma3, ma4,
                                    pa.tp, pa.sl,
                                    pa.tpCnt, pa.slCnt, pa.mult,
                                    pa.sb, 1, smooth, pa.nl, pa.quants, 10, 0,
                                    l1, l2, l3, l4, l5, l6,
                                    pa.tpCnt2start, pa.tpCnt2startS, true, suxxes, pa.maxParallelDeals, minProfitToClose,
                                    pa.CAP, criticalLossToClosePart, pa.martin, desc);

                            sc.symTradeStat.slozhPercent = SLOZH_PERCENT;
                            sc.symTradeStat.TRADE_AMOUNT = tradeAmntWASlozhPercentGlobal.get(desc);

                            sc.upPowers = new MMAVT(1, pa.ms);
                            sc.downPowers = new MMAVT(1, pa.ms);
                            sc.upLL = new MMAVT(1, pa.ms);
                            sc.downLL = new MMAVT(1, pa.ms);

                            sc._upP = new MMATema(1, pa.n * 1200 * 1000L);
                            sc._downP = new MMATema(1, pa.n * 1200 * 1000L);

                            sc.proboyUpPrices = new MMA(pa.r);
                            sc.proboyDownPrices = new MMA(pa.r);

                            // Хранилище всех уровней за 4 часа
                            sc.levels = new MMATema(1, 4 * 60 * 60_000);

                            sc.aggBuyCollector = new MMATema(1, AGG_PER);
                            sc.aggSellCollector = new MMATema(1, AGG_PER);
                            sc.aggPriceMoveCollector = new MMATema(1, AGG_PER);
                            sc.speed = new MMATema(1, pa.speedFrame * 60_000L);
                            sc.overAvg = new MMATema(1,pa.POWER_LIMIT_S2 * 60_000L);
                        }

                        if (MAKE_CSV)
                        {
                            SymbContainer scBTC = scBySymb.get( CSV_SYMBOL );
                            if (scBTC == null) continue;
                            CandleResp cr = scBTC.getPrevKline(pl.time);
                            if (cr != null)
                            {
                                if (prevCr == null || prevCr.openTime < cr.openTime)
                                {   // Сменился бар!        Записать строку в CSV
                                    LearnData ld = new LearnData(cr.openTime, pl.price, false);

                                    ld.addVal("a_o", cr.o);
                                    ld.addVal("b_h", cr.h);
                                    ld.addVal("c_l", cr.l);
                                    ld.addVal("d_c", cr.c);
                                    ld.addVal("dd_c", TradeStat.tradeAmntGlobalByKey.get(desc));
                                    double upp = (_upP.getSum() + _upP2.getSum())/2.0;
                                    double dwn = (_downP.getSum() + _downP2.getSum())/2.0;
                                    double uppOts = (_upPot.getSum() + _upP2ot.getSum())/2.0;
                                    double dwnOts = (_downPot.getSum() + _downP2ot.getSum())/2.0;

                                    // Чистый айсберг - без лимиток.
                                    double uppIce = (_upIce.getSum() + _upP2Ice.getSum())/2.0 - upp;
                                    double dwnIce = (_downIce.getSum() + _downP2Ice.getSum())/2.0 - dwn;

                                    double cummulativeDownTend = uppIce + dwn;
                                    double cummulativeUpTend = dwnIce + upp;

                                    events.clear();
                                    //ld.addVal("e_uppDwnRatio", probi);
                                    //ld.addVal("e_uppDwnRatioOts", otski);
                                    //ld.addVal("e_uppDwnRatioIce", icei)
                                    history.add(ld);
                                    prevCr = cr;
                                }
                            }
                        }

                        sc.last24hvalues = pl.volume24h;

                        if (!pl.postSignal)
                        {
                            // Трейс для подсчета прибыли-убытков для конкретного теста
                            if (pl.min <= 0) pl.min = pl.price;
                            sc.symTradeStat.traceMinMax(pl.min, pl.max, pl.price);

                            chaserUp.priceUpdate(pl.symbol, pl.price, pl.time);
                            chaserDown.priceUpdate(pl.symbol, pl.price, pl.time);
                            //chaserUp2.priceUpdate(pl.symbol, pl.price, pl.time);
                            //chaserDown2.priceUpdate(pl.symbol, pl.price, pl.time);

                            if (LEARN_MODE)
                            {   // ТРЕЙСИМ ВСЕ ОТКРЫТЫЕ СДЕЛКИ - не закрывать ли их
                                List<LearnData> localtrades = tradesBySymbol.get(pl.symbol);
                                if (localtrades != null)
                                {
                                    Set<LearnData> trades2Del = new HashSet<>();
                                    for (LearnData ld : localtrades)
                                    {
                                        double tpslDelta = pl.price * 50.0/ 1000.0;
                                        double neytralDelta = pl.price * 1.0/ 1000.0;
                                        if (ld.buy)
                                        { // покупали
                                            if (ld.openPrice - pl.min > tpslDelta)
                                            { // SL
                                                ld.result = 0.0;
                                            } else if (pl.max - ld.openPrice > tpslDelta)
                                            { // TP
                                                ld.result = 2.0;
                                            }  else if (pl.time > ld.openTime + pa.maxHours * 60 * 60_000L)
                                            { // time close
                                                if (pl.price - ld.openPrice > neytralDelta)
                                                { // good
                                                    ld.result = 2.0;
                                                } else if (ld.openPrice - pl.price > neytralDelta)
                                                { // bad
                                                    ld.result = 0.0;
                                                } else
                                                    ld.result = 0.0; // внутри нейтрали тоже плохо
                                            }
                                        } else
                                        { // продавали
                                            if (pl.max - ld.openPrice > tpslDelta)
                                            { // SL
                                                ld.result = 0.0;
                                            } else if (ld.openPrice - pl.min > tpslDelta)
                                            { // TP
                                                ld.result = 2.0;
                                            } else if (pl.time > ld.openTime + pa.maxHours * 60 * 60_000L)
                                            { // time close
                                                if (ld.openPrice - pl.price > neytralDelta)
                                                { // good
                                                    ld.result = 2.0;
                                                } else if (pl.price - ld.openPrice > neytralDelta)
                                                { // bad
                                                    ld.result = 0.0;
                                                } else
                                                    ld.result = 0.0;    // внутри нейтрали тоже плохо
                                            }
                                        }
                                        if (ld.result != null)
                                            trades2Del.add(ld);
                                    }
                                    localtrades.removeAll(trades2Del);
                                    allLearnData.addAll(trades2Del);
                                }
                            }

                            // трейс для накопления статистики для NN
                            //sc.traceTrades(pl);

                            totalLossInfo.addOrReplacePL(sc);

                            double totalLoss = totalLossInfo.getMinPNL();

                            if (totalLoss < 0 && maxLoss < -totalLoss)
                                maxLoss = -totalLoss;
                            if (maxLoss > CRITICAL_LOSS) {
                                log.info(new Date(pl.time) + "-------------- CRITICAL LOSS INSIDE FILE " + maxLoss);
                                totalLossInfo.criticalLoss = true; // признак того что надо совсем выключить набор параметров
                                maxLoss = null;
                                break;
                            }

                            if (totalLossInfo.getAvgPNL() >= pa.CAP / 10.0
                               // || totalLossInfo.getAvgPNL() <= -pa.CAL / 10.0
                                )
                            {
                                capCnt++;
                                log.info(new Date(pl.time) + " --- close ALL " + capCnt + (totalLossInfo.getAvgPNL() > 0 ? " +++ " : " --- "));
                                double allQty = 0;
                                for (SymbContainer _sc : scBySymb.values()) {
                                    if (_sc.symTradeStat.didBuy != null) {
                                        _sc.symTradeStat.tradeCnt++;
                                        allQty += _sc.symTradeStat.allDealPrices.getTotalQty();
                                        //  tradedCnt++;
                                    }
                                    _sc.symTradeStat.allDealPrices.reset();
                                    _sc.symTradeStat.didBuy = null;
                                    _sc.symTradeStat.currentProfit = 0.0;
                                    _sc.symTradeStat.lossPercent = _sc.symTradeStat.profitPercent = 0;
                                    _sc.savedLossB = _sc.savedLossS = _sc.savedProfB = _sc.savedProfS = null;
                                }
                                // Накапливающийся капитал
                                double allTradeAmount = TradeStat.tradeAmntGlobalByKey.get(desc);
                                // капитал, которым торгуем (отличается если НЕ сложный процент)
                                double tradingTradeAmount = allTradeAmount;
                                if (!SLOZH_PERCENT)
                                    tradingTradeAmount = tradeAmntWASlozhPercentGlobal.get(desc);

                                double delta = tradingTradeAmount * pa.CAP / 1000.0;
                                if (totalLossInfo.getAvgPNL() < 0)
                                    delta = tradingTradeAmount * pa.CAL / -1000.0;

                                double newAllTradeAmnt = allTradeAmount + delta
                                            - allQty * TradeStat.comission * TradeStat.comissionZapasDef; // комиссии

                                TradeStat.tradeAmntGlobalByKey.put(desc, newAllTradeAmnt);
                                // TODO:  Исправить при БЕЗ-СЛОЖНЫЙ ПРОЦЕНТ !!

                                double oldTradeAmntWASlozhPerc = tradeAmntWASlozhPercentGlobal.get(desc);
                                double newTradingTradeAmntWaSlozhPercent = newAllTradeAmnt / WA_SLOZH_PERCENT_EXTRA_KOOF;

                                // if (newTradingTradeAmntWaSlozhPercent > oldTradeAmntWASlozhPerc * 2)
                                   // newTradingTradeAmntWaSlozhPercent = oldTradeAmntWASlozhPerc * 1.5;

                                if (!SLOZH_PERCENT) tradeAmntWASlozhPercentGlobal.put(desc, newTradingTradeAmntWaSlozhPercent);

                                for (SymbContainer _sc : scBySymb.values()) {
                                    _sc.symTradeStat.tradeAmnt = TradeStat.tradeAmntGlobalByKey.get(desc);
                                    _sc.symTradeStat.TRADE_AMOUNT = newTradingTradeAmntWaSlozhPercent;  // чем торгуем без сложного процента
                                }
                                //log.info("new TA = " + TradeStat.tradeAmntGlobalByKey.get(desc));
                                totalLossInfo.clear();
                            }
                        }

                        if (!pl.postSignal && pl.proboy == null) continue;

                        if (!pl.postSignal)
                        if ( pl.iceberg )
                        {   // Айсберг  - т.е. реально разобранный объем!
                            if ( //   pl.iceberg &&
                                    (pl.buyInterest > 0 || pl.sellInterest > 0)
                            )
                            {
                                boolean stakApproved = false;
                                boolean marketApproved = false;
                                if (pl.proboy && pl.up || !pl.proboy && !pl.up) {
                                    if (pl.buyInterest > pl.sellInterest) stakApproved = true;
                                    if (upPostSignalsL.getSum() > -1) marketApproved = true;
                                } else {
                                    if (pl.buyInterest < pl.sellInterest) stakApproved = true;
                                    if (downPostSignalsL.getSum() > -1) marketApproved = true;
                                }
                                Event event = new Event(pl.time, pl.id, pl.symbol, pl.up != null && pl.up, pl.proboy, pl.iceberg,
                                        pl.price, pl.levelLiq, pl.power, stakApproved, marketApproved,
                                        calcGlobalBuySellRatio(pl, _upP, _upP2, _downP, _downP2));
                                if (MAKE_CSV && CSV_SYMBOL.equals(pl.symbol)) {
                                    events.add(event);
                                }
                                if (FILL_DB) {
                                    dbEventsBuffered = addEventToDbBuffer(dbEventsBySymbol, event, dbEventsBuffered);
                                    if (dbEventsBuffered >= DB_EVENTS_BATCH) {
                                        flushDbEventsBuffer(dbEventsBySymbol);
                                        dbEventsBuffered = 0;
                                    }
                                }
                                ///// Расчет айсбергов
                                /*
                                {
                                    if (pl.up) {
                                        _upIce.addStat(pl.levelLiq, pl.time);
                                        _upP2Ice.addStat(pl.levelLiq, pl.time);
                                    } else {
                                        _downIce.addStat(pl.levelLiq, pl.time);
                                        _downP2Ice.addStat(pl.levelLiq, pl.time);
                                    }
                                }*/
                            }
                            continue;             /////////////////////////////////////////////////// !!!!!!!!!!!!!!!!
                        }

                        int overVolumeAll = 0;
                        int overVolume = 0;
                        if (!pl.postSignal)
                        {
                            MMAVT llCollector = sc.upLL;
                            if (!pl.up) { // && pl.proboy || pl.up && !pl.proboy) {
                                llCollector = sc.downLL;
                            }
                            llCollector.addStat(pl.price, pl.levelLiq, pl.time);

                            double ll = llCollector.getQtyInPriceRange( pa.x );
                            double volumes24h = pl.volume24h; // old version
                            double volPart = volumes24h / 100_000.0;
                            if (volPart > 0) {
                                overVolume = (int) (ll / volPart);
                            }

                            if ( //   pl.iceberg &&
                                   (pl.buyInterest > 0 || pl.sellInterest > 0)
                            )
                            {
                                if (pl.proboy)
                                {
                                    if (pl.up) {
                                        _upP.addStat(pl.levelLiq, pl.time);
                                        _upP2.addStat(pl.levelLiq, pl.time);
                                        chaserUp.chasePrice(pl.symbol, pl.price, pl.time, pl.price);
                                        //chaserUp2.chasePrice(pl.symbol, pl.price, pl.time, pl.price);
                                    } else {
                                        _downP.addStat(pl.levelLiq, pl.time);
                                        _downP2.addStat(pl.levelLiq, pl.time);
                                        chaserDown.chasePrice(pl.symbol, pl.price, pl.time, pl.price);
                                        //chaserDown2.chasePrice(pl.symbol, pl.price, pl.time, pl.price);
                                    }
                                } else {
                                    //  Расчет отскоков
                                    /*
                                    if (pl.up) {
                                        _upPot.addStat(pl.levelLiq, pl.time, pl.symbol, pl.price);
                                        _upP2ot.addStat(pl.levelLiq, pl.time, pl.symbol, pl.price);
                                    } else {
                                        _downPot.addStat(pl.levelLiq, pl.time, pl.symbol, pl.price);
                                        _downP2ot.addStat(pl.levelLiq, pl.time, pl.symbol, pl.price);
                                    }*/
                                }
                                if (FILL_DB || MAKE_CSV) {
                                    boolean stakApproved = false;
                                    boolean marketApproved = false;
                                    if (pl.proboy && pl.up || !pl.proboy && !pl.up) {
                                        if (pl.buyInterest > pl.sellInterest) stakApproved = true;
                                        if (upPostSignalsL.getSum() > -1) marketApproved = true;
                                    } else {
                                        if (pl.buyInterest < pl.sellInterest) stakApproved = true;
                                        if (downPostSignalsL.getSum() > -1) marketApproved = true;
                                    }
                                    Event event = new Event(pl.time, pl.id, pl.symbol, pl.up != null && pl.up, pl.proboy, pl.iceberg,
                                            pl.price, pl.levelLiq, pl.power, stakApproved, marketApproved,
                                            calcGlobalBuySellRatio(pl, _upP, _upP2, _downP, _downP2));
                                    if (MAKE_CSV && CSV_SYMBOL.equals(pl.symbol)) {
                                        events.add(event);
                                    }
                                    if (FILL_DB) {
                                        dbEventsBuffered = addEventToDbBuffer(dbEventsBySymbol, event, dbEventsBuffered);
                                        if (dbEventsBuffered >= DB_EVENTS_BATCH) {
                                            flushDbEventsBuffer(dbEventsBySymbol);
                                            dbEventsBuffered = 0;
                                        }
                                    }
                                }
                            }

                            double volPartAll = pl.volumeAll24h / 100_00_000.0;
                            overVolumeAll = (int)((double)pl.levelLiq / volPartAll);
                        }

                        boolean megaSignal = true;
                        int openedHours = sc.symTradeStat.getOpenHours(pl);
                        int buySellDiagram = 0;

                        //if (!pl.postSignal)
                        //if (!(pl.ratinng > proboyDataContainer.coins.size() * pa.topCoins/100.0)) continue;

                        LearnData ld = null;
                        if (    (LEARN_MODE || USE_ML)
                                && !pl.postSignal
                                // && pl.proboy
                                //&& pl.levelLiq > pa.LUSD
                                //&& overVolume > 20 // 100
                                && sc.speed.getAvg() * 10.0 / pl.speed > 5 // 20
                                && pl.speedAllMarket * 10.0 / allMarketSpeed.getAvg() > 5 // 10
                        )
                        {
                            int volKoof = 0;// (int) (sc.getVolatileKoof(pl.time, pl.price, pa.n2) * 10);

                            // пробитые объёмы
                            double upp = (_upP.getSum() + _upP2.getSum())/2.0;
                            double dwn = (_downP.getSum() + _downP2.getSum())/2.0;

                            if (volKoof >= 0) {
                                int upperBars =  sc.getKlinesUpperBefor(pl.time, pl.price, pa.d2+2, pa.x2);
                                int lowerBars =  sc.getKlinesLowerBefor(pl.time, pl.price, pa.d2+2, pa.x2);
                                if (//(!pl.up && !pl.proboy || pl.up && pl.proboy) &&
                                        upp * 10.0 / dwn > 5 // !
                                        && pl.buyInterest * 10.0 / pl.sellInterest > 5
                                        && pl.buyInterestDeep * 10.0 / pl.sellInterestDeep > 5
                                        && pl.agressSell * 10.0 / pl.agressBuy > 1 // !
                                        // && pl.agressBuy > pl.agressSell //
                                        //&& pl.agressBuy > pl.sellInterest * 1
                                        //&& upPostSignals.getSum() > 0
                                        && upPostSignalsL.getSum() > -5 // !
                                        //&& downPostSignals.getSum() < 0
                                        //&& downPostSignalsL.getSum() < 0 // !
                                        && upperBars > pa.d2
                                        //&& lowerBars > pa.d2
                                ) {
                                    // learn buy
                                    ld = new LearnData(pl.time, pl.price, true);
                                    ld.addVal("upDwnRatio", (double) (upp * 10.0 / dwn));
                                    ld.addVal("bsRatio", (double) (pl.buyInterest * 10.0 / pl.sellInterest));
                                    ld.addVal("bsDeepRatio", (double) (pl.buyInterestDeep * 10.0 / pl.sellInterestDeep));
                                    ld.addVal("aggBSRatio", (double) (pl.agressSell * 10.0 / pl.agressBuy));
                                    ld.addVal("postSig", (double) (upPostSignalsL.getSum()));
                                } else if (//(pl.up && !pl.proboy || !pl.up && pl.proboy) &&
                                        dwn * 10.0 / upp > 5 // !
                                        && pl.sellInterest * 10.0 / pl.buyInterest > 5
                                        && pl.sellInterestDeep * 10.0 / pl.buyInterestDeep > 5
                                        && pl.agressBuy * 10.0 / pl.agressSell > 1 // !
                                        //&& pl.sellInterest > pl.agressBuy * 1
                                        //&& downPostSignals.getSum() > 0
                                        && downPostSignalsL.getSum() > -5 // !
                                        //&& upPostSignals.getSum() < 0
                                        //&& upPostSignalsL.getSum() < 0 // !
                                        //&& lowerBars > 50
                                        //&& upperBars > 50
                                ) {
                                    // learn sell
                                    ld = new LearnData(pl.time, pl.price, false);
                                    ld.addVal("upDwnRatio", (double) (dwn * 10.0 / upp));
                                    ld.addVal("bsRatio", (double) (pl.sellInterest * 10.0 / pl.buyInterest));
                                    ld.addVal("bsDeepRatio", (double) (pl.sellInterestDeep * 10.0 / pl.buyInterestDeep));
                                    ld.addVal("aggBSRatio", (double) (pl.agressBuy * 10.0 / pl.agressSell));
                                    ld.addVal("postSig", (double) (downPostSignalsL.getSum()));
                                }
                            }
                            if (ld == null) continue;
                            ld.addVal("spd", (double)(sc.speed.getAvg() * 10.0 / pl.speed));
                            ld.addVal("spdAll", (double)(pl.speedAllMarket * 10.0 / allMarketSpeed.getAvg()));
                            ld.addVal("overVol", (double)overVolume);
                            ld.addVal("up", pl.up ? 1.0 : 0.0);
                            ld.addVal("proboy", pl.proboy ? 1.0 : 0.0);
                            if (LEARN_MODE) {
                                List<LearnData> localLearnData = tradesBySymbol.get(pl.symbol);
                                if (localLearnData == null) {
                                    localLearnData = new ArrayList<>();
                                    tradesBySymbol.put(pl.symbol, localLearnData);
                                }
                                localLearnData.add(ld);
                            }
                        }

                        boolean ML_BUY = false;
                        boolean ML_SELL = false;

                        // ПРЕДСКАЗАНИЕ!
                        if (ld != null && !LEARN_MODE && USE_ML)
                        {
                            AiSignalService.PredictionResult result = aiService.predict(ld);
                            if (result.isStrongSignal())
                            {
                                if (ld.buy) ML_BUY = true;
                                else ML_SELL = true;
                            }
                        }

                        // =============== EXTRA BUY / SELL ==================
                        if (sc.symTradeStat.didBuy != null && pa.quants > 1 && false
                                && pl.time - sc.symTradeStat.openTime < 60 * 60_000L
                            )
                        {
                            final double enter = sc.symTradeStat.getAvgEnterPrice();
                            final double percentMove = 0;//enter * (double)pa.n3 / 1000.0;
                            double upp = (_upP.getSum() + _upP2.getSum())/2.0;
                            double dwn = (_downP.getSum() + _downP2.getSum())/2.0;
                            if (sc.symTradeStat.didBuy)
                            {
                                if (upp > dwn)
                                if (pl.postSignal && pl.price < enter - percentMove || !pl.postSignal && pl.min < enter - percentMove)
                                {
                                    sc.symTradeStat.tpComissions = pa.tp;
                                    sc.symTradeStat.slComissions = pa.sl;
                                    sc.symTradeStat.tpCount = pa.tpCntB;
                                    sc.symTradeStat.tpCnt2start = pa.tpCnt2startB;
                                    sc.symTradeStat.buy(enter - percentMove, pa.mult, pl.id);
                                }
                            } else {
                                if (dwn > upp)
                                if (pl.postSignal && pl.price > enter + percentMove || !pl.postSignal && pl.max > enter + percentMove)
                                {
                                    sc.symTradeStat.tpComissions = pa.tpO;
                                    sc.symTradeStat.slComissions = pa.slO;
                                    sc.symTradeStat.tpCount = pa.tpCnt;
                                    sc.symTradeStat.tpCnt2start = pa.tpCnt2start;
                                    sc.symTradeStat.sell(enter + percentMove, pa.mult, pl.id);
                                }
                            }
                        }

                        if (!pl.postSignal) {
                            //overAvg.addStat(overVolume, pl.time);
                            //sc.overAvg.addStat(overVolume, pl.time);
                        }

                        //////////////   TRADE LOGIC =============== ///////
                        if (    !LEARN_MODE //&& !MAKE_CSV
                                && !pl.postSignal
                                    //&& pl.proboy
                                    //&& pl.power > pa.PWL4

                                && pl.time - sc.symTradeStat.openTime > pa.PWL * 1000L

                                    || ML_SELL || ML_BUY


                                ) { // PROBOY
                            if (
                                    overVolumeAll >= pa.ra &&
                                            //overVolume >= pa.r &&

                                            pl.levelLiq > pa.LUSD

                                            || ML_SELL || ML_BUY
                            ) {
                                int volKoof = (int) (sc.getVolatileKoof(pl.time, pl.price, pa.n2) * 10);

                                if (
                                        //volKoof > pa.nn &&
                                        (volKoof > pa.nn && !pl.proboy || pl.proboy)
                                        && (volKoof < pa.PWL3 && !pl.proboy || volKoof < pa.PWL4 && pl.proboy)

                                         || ML_SELL || ML_BUY
                                ) {

                                    //int upperBars =  sc.getKlinesUpperBefor(pl.time, pl.price, pa.d2+2, pa.x2);
                                    //int lowerBars =  sc.getKlinesLowerBefor(pl.time, pl.price, pa.d2+2, pa.x2);
                                    //upperBars = lowerBars = Math.max(upperBars, lowerBars);
                                    /*
                                    double allMarketAvgSpeed = allMarketSpeed.getAvg();
                                    double coinAvgSpeed = sc.speed.getAvg();
                                    // Считаем отклонение скорости в %%; > 0 значит скорость выше
                                    double allMarketSpeedDelta = allMarketAvgSpeed == 0 ? 0
                                            : ((double)pl.speedAllMarket - allMarketAvgSpeed) * 100.0 / allMarketAvgSpeed;
                                    double coinSpeedDelta = coinAvgSpeed == 0 ? 0
                                            : ((double)pl.speed - coinAvgSpeed) * 100.0 / coinAvgSpeed;
                                    */
                                    // Порядок значения upp : ~2.E7
                                    double upp = (_upP.getSum() + _upP2.getSum())/2.0;           // !!
                                    double dwn = (_downP.getSum() + _downP2.getSum())/2.0;       // !!

                                    //double uppIce = (_upIce.getSum() + _upP2Ice.getSum())/2.0;// - upp;
                                    //double dwnIce = (_downIce.getSum() + _downP2Ice.getSum())/2.0;// - dwn;
                                    //double uppO = (_upPot.getSum() + _upP2ot.getSum())/2.0;// - upp;
                                    //double dwnO = (_downPot.getSum() + _downP2ot.getSum())/2.0;// - dwn;


                                    int rrrr = pa.rrrr;
                                    int powLim = pa.POWER_LIMIT;
                                    int r = pa.r;

                                    //double ca = sc.getCorrelation(pl.time, scBySymb,2);
                                    //int da = (pa.POWER_LIMIT_S2-(int)ca) / pa.n_short;
                                    //if (da > 0) rrrr -= da;

                                    int zakis = (overVolume - pa.r) / pa.z;
                                    powLim = powLim - zakis;

                                    if (    ML_BUY ||

                                            (!pl.up && !pl.proboy || pl.up && pl.proboy)
                                                    && pl.buyInterest > pl.sellInterest
                                                    && overVolume >= r
                                                    // && allMarketSpeedDelta > pa.spedKoof
                                                    // && coinSpeedDelta > pa.spedKoof // !

                                            // && pl.buyInterest * 10.0 > pl.agressBuy * pa.PWL
                                                    // && pl.agressBuy < pl.agressSell // !!
                                            //&& sc.aggBuyCollector.getSum() > sc.aggSellCollector.getSum() // ?
                                            //&& sc.aggPriceMoveCollector.getSum() > 0
                                            //&& aggB.getSum() < aggS.getSum()
                                            //&& aggPM.getSum() > 0
                                            //&& uppIce > dwnIce

                                            && upp * 10.0 / dwn > powLim
                                                    //&& uppIce * 10.0 / dwnIce > pa.n_short
                                                    //&& dwnIce * 10.0 / uppIce > pa.n_short // revers
                                                    //&& upp > pl.volumeAll24h / pa.n_short / 1000

                                                    //&& chaserUp.getApprovePercent(true, pl.time) > pa.POWER_LIMIT_S2
                                                    //&& chaserDown.getApprovePercent(false, pl.time) < pa.POWER_LIMIT_SHORT

                                                    /*&& (
                                                    upPostSignalsL.getSum() > rrrr
                                                    && downPostSignalsL.getSum() < -rrrr
                                                    || pl.proboy && upPostSignalsL.getSum() > rrrr
                                                    ) */
                                                    //&& lowerBars >= pa.d2
                                                    //&& sc.getMMADirection(pl.time, pa.slowN, pa.fastN, pl.price) > -1000
                                                    //&& !sc.isPodzhim()
                                            //&& !(sc.symTradeStat.didBuy != null && sc.symTradeStat.didBuy)
                                    )
                                    {   // buy
                                        //refreshChasers(chaserUp, chaserDown, pl);
                                        if (chaserUp.getApprovePercent(true, pl.time) > pa.POWER_LIMIT_S2
                                                && chaserDown.getApprovePercent(false, pl.time) < pa.POWER_LIMIT_SHORT)
                                        if (
                                                (sc.symTradeStat.didBuy != null)
                                                        ||
                                                        newDealAvailable(pl.symbol, true, scBySymb, pa.profPercentToClose, pa.maxParallelDeals, totalLossInfo, pa.buySellDelta, pa.proboyOtskDelta, totalLossInfo.getAvgPNL(), megaSignal, true, pa.tpCnt)
                                        ) {
                                            int mult = calcProportionalMult(pa.mult, pa.maxMult, 18, 32, (int)(upp * 10 / dwn));

                                            sc.symTradeStat.tpComissions = pa.tp;
                                            sc.symTradeStat.slComissions = pa.sl;
                                            sc.symTradeStat.tpCount = pa.tpCntB;
                                            sc.symTradeStat.tpCnt2start = pa.tpCnt2startB;
                                            boolean ok = false;
                                            ok = sc.symTradeStat.buy(pl.price, mult, pl.id);
                                            if (ok) {
                                                //log.info(" chasedSymbs: "
                                                  //      + (chaserUp.getAllSymbols().size() + chaserDown.getAllSymbols().size()));
                                                sc.symTradeStat.openTime = pl.time;
                                                lastDealTime = pl.time;
                                                /* log.info(new Date(pl.time)
                                                        + " " + pl.id
                                                        + " " + pl.symbol
                                                        + " BUY @ " + pl.price
                                                        + " open Pwr " + pl.power
                                                        + " overVol " + overVolume
                                                        + " upPost " + upPostSignalsL.getSum()
                                                        + " dwnPost " + downPostSignalsL.getSum()
                                                        + " upp " + upp
                                                        + " dwn " + dwn
                                                        + " volKoof " + volKoof
                                                        + " 24 " + pl.volume24h); */
                                                sc.symTradeStat.proboy = true;
                                                sc.symTradeStat.openTime = pl.time;

                                                buySellDiagram = 10000;
                                                tradeCnt++;
                                                sc.buyLimitPrice = sc.sellLimitPrice = null;
                                                sc.symTradeStat.laterBuy = null;
                                            } else {
                                                refreshTotalLossInfo(totalLossInfo, sc, pl);
                                                //   log.info(new Date(pl.time) + " " + pl.symbol + " BUY @ " + pl.price + " may close sell P " + pl.power);
                                            }
                                        }
                                    }

                                    if (    ML_SELL ||

                                            (pl.up && !pl.proboy || !pl.up && pl.proboy)
                                                 && pl.sellInterest > pl.buyInterest
                                                    && overVolume >= r
                                                    // && pl.sellInterest + pl.sellInterestDeep > pl.buyInterest + pl.buyInterestDeep
                                                    //&& allMarketSpeedDelta > pa.spedKoof
                                                    // && coinSpeedDelta > pa.spedKoof

                                            // && pl.sellInterest * 10.0 > pl.agressSell * pa.PWL
                                            //&& pl.agressSell < pl.agressBuy // !!!
                                                    //&& pl.agressSell * 10.0 / pl.agressBuy > pa.n_short
                                            //&& sc.aggSellCollector.getSum() > sc.aggBuyCollector.getSum() // ?
                                            //&& sc.aggPriceMoveCollector.getSum() < 0
                                            //&& aggS.getSum() < aggB.getSum()
                                            //&& aggPM.getSum() < 0
                                                    //&& dwnIce > uppIce
                                                    && dwn * 10.0 / upp > powLim
                                                    //&& dwnIce * 10.0 / uppIce > pa.n_short
                                                    //&& uppIce * 10.0 / dwnIce > pa.n_short // reverse
                                                    //&& dwn > pl.volumeAll24h / pa.n_short / 1000
                                                    //&& chaserDown.getApprovePercent(false, pl.time) > pa.POWER_LIMIT_S2
                                                    //&& chaserUp.getApprovePercent(true, pl.time) < pa.POWER_LIMIT_SHORT

                                                    /* && (
                                                        downPostSignalsL.getSum() > rrrr
                                                        && upPostSignalsL.getSum() < -rrrr
                                                        || pl.proboy && downPostSignalsL.getSum() > rrrr
                                                    ) */
                                                    //&& upperBars >= pa.d2
                                                    //&& sc.getMMADirection(pl.time,pa.slowN, pa.fastN, pl.price) < 1000
                                                    //&& !sc.isPodzhim()
                                                    //&& !(sc.symTradeStat.didBuy != null && !sc.symTradeStat.didBuy)
                                    )
                                    {
                                        // sell
                                        //refreshChasers(chaserUp, chaserDown, pl);
                                        if (chaserDown.getApprovePercent(false, pl.time) > pa.POWER_LIMIT_S2
                                            && chaserUp.getApprovePercent(true, pl.time) < pa.POWER_LIMIT_SHORT)
                                        if ((sc.symTradeStat.didBuy != null)
                                                ||
                                                newDealAvailable(pl.symbol, false, scBySymb, pa.profPercentToClose, pa.maxParallelDeals, totalLossInfo, pa.buySellDelta, pa.proboyOtskDelta, totalLossInfo.getAvgPNL(), megaSignal, true, pa.tpCnt)
                                        )
                                        {
                                            int mult = calcProportionalMult(pa.mult, pa.maxMult, 18, 32, (int)(dwn * 10 / upp));
                                            sc.symTradeStat.tpComissions = pa.tpO;
                                            sc.symTradeStat.slComissions = pa.slO;
                                            sc.symTradeStat.tpCount = pa.tpCnt;
                                            sc.symTradeStat.tpCnt2start = pa.tpCnt2start;

                                            boolean ok = false;
                                            //if (pl.time > sc.symTradeStat.skipDealTill)
                                            ok = sc.symTradeStat.sell(pl.price, mult, pl.id);
                                            if (ok) {
                                                //log.info(" chasedSymbs: "
                                                  //      + (chaserUp.getAllSymbols().size() + chaserDown.getAllSymbols().size()));

                                                sc.symTradeStat.openTime = pl.time;
                                                lastDealTime = pl.time;
                                                //log.info(new Date(pl.time) + " " + pl.symbol + " SELL @ " + pl.price + " open P " + pl.power);
                                                sc.symTradeStat.proboy = true;
                                                buySellDiagram = -10000;
                                                sc.symTradeStat.openTime = pl.time;
                                                tradeCnt++;
                                                sc.buyLimitPrice = sc.sellLimitPrice = null;
                                                sc.symTradeStat.laterBuy = null;
                                            } else
                                                refreshTotalLossInfo(totalLossInfo, sc, pl);
                                            //   log.info(new Date(pl.time) + " " + pl.symbol + " SELL @ " + pl.price + " may close buy P " + pl.power);
                                        }
                                    }
                                }
                            }
                        }

                        if (    !pl.postSignal  )
                        {
                            // close strategy 1  -------------------------------------
                            if (
                                    sc.symTradeStat.didBuy != null
                                    && pl.levelLiq >= pa.USD
                            )
                            {
                                    if ( //!pl.proboy &&
                                            !sc.symTradeStat.didBuy &&
                                            (!pl.up && !pl.proboy || pl.up && pl.proboy) &&
                                                    pl.buyInterest > pl.sellInterest &&
                                                    //upPostSignalsL.getSum() > -pa.n3 &&
                                                    chaserUp.getApprovePercent(true, pl.time) > pa.n3 &&
                                                    ((double) overVolume >= (double)pa.rrr
                                                            ||
                                                            _upP.getSum() * 10.0 / _downP.getSum() > pa.POWER_LIMIT_S
                                                            && (double) overVolume >= pa.rr
                                                    )
                                    )
                                    {
                                        //log.info("close");
                                        //sc.symTradeStat.closePart(true, pl.price);
                                        //ccnt++;
                                        sc.symTradeStat.buy(pl.price, true, false);
                                        if (sc.symTradeStat.didBuy == null)
                                            refreshTotalLossInfo(totalLossInfo, sc, pl);
                                        // log.info(new Date(pl.time) + " " + pl.symbol + " BUY @ " + pl.price + " close " + pl.power);
                                    } else
                                    if (//  !pl.proboy &&
                                            sc.symTradeStat.didBuy &&
                                            (pl.up && !pl.proboy || !pl.up && pl.proboy) &&
                                                    pl.sellInterest > pl.buyInterest &&
                                                    //downPostSignalsL.getSum() > -pa.n3 &&
                                                    chaserDown.getApprovePercent(true, pl.time) > pa.n3 &&
                                                    ((double) overVolume >= (double)pa.rrr
                                                            ||
                                                            _downP.getSum() * 10.0 / _upP.getSum() > pa.POWER_LIMIT_S
                                                                    && (double) overVolume >= pa.rr
                                                    )
                                    )
                                    {
                                        //log.info("close");
                                        //sc.symTradeStat.closePart(false, pl.price);
                                        sc.symTradeStat.sell(pl.price, true, false);
                                        if (sc.symTradeStat.didBuy == null)
                                            refreshTotalLossInfo(totalLossInfo, sc, pl);
                                        // log.info(new Date(pl.time) + " " + pl.symbol + " SELL @ " + pl.price + " close " + pl.power);
                                    }
                            }
                            // ------------------------ close startegy 2 ----------------------------
                            if (
                                    sc.symTradeStat.didBuy != null
                                    && (pl.time - sc.symTradeStat.openTime) / (60 * 60_000L) > pa.maxHours
                                    //&& sc.symTradeStat.currentProfit > 0
                            )
                            {
                                    sc.symTradeStat.buy(pl.price, true, false);
                                    sc.symTradeStat.sell(pl.price, true, false);
                                    refreshTotalLossInfo(totalLossInfo, sc, pl);
                            }
                        }
                    }
                    } finally {
                        if (FILL_DB) {
                            flushDbEventsBuffer(dbEventsBySymbol);
                        }
                    }
                    saveChaseCache(true);
                    if (totalLossInfo.criticalLoss)
                    {
                        break;
                    } else {
                        if (pa.stage == 0) {
                            // Добавляем PNL только при РАЗДЕЛЬНОМ тесте файлов.
                            // Иначе PNL не добавляется т.к. со следующего файла продолжаются открытые прежде сделки
                            if (SLOZH_PERCENT) {
                                // Торгуем ВСЕЙ суммой
                                TradeStat.tradeAmntGlobalByKey.put(desc, TradeStat.tradeAmntGlobalByKey.get(desc) * (1 + totalLossInfo.getAvgPNL() / 100.0));
                            } else {
                                // Торгуем фикс. частью всей суммы
                                TradeStat.tradeAmntGlobalByKey.put(desc, TradeStat.tradeAmntGlobalByKey.get(desc)
                                        + tradeAmntWASlozhPercentGlobal.get(desc) * totalLossInfo.getAvgPNL() / 100.0);
                            }
                        }

                        if (TradeStat.tradeAmntGlobalByKey.get(desc) < startTradeAmntMax / PROSADKA)
                        {   // исключаем такой набор при котором была просадка более чем PROSADKA
                            log.info("----- LOSS OF FILE from " + startTradeAmntMax + " to " + TradeStat.tradeAmntGlobalByKey.get(desc)
                                    + "( > " + PROSADKA + ")");
                            maxLoss = null; break;
                        }

                        double profPercent = (TradeStat.tradeAmntGlobalByKey.get(desc) - startTradeAmnt) * 100.0
                                / startTradeAmnt;
                        avgProfPercent.addStat(profPercent);

                        if (printDetails)
                        log.info(fName + " Final PNL: " + String.format("%.1f", totalLossInfo.getMinPNL()) + " Start amnt " + String.format("%.1f", startTradeAmnt)
                                + " End amnt " + String.format("%.1f", TradeStat.tradeAmntGlobalByKey.get(desc)) + " trCnt " + tradeCnt
                                + " maxLoss " + String.format("%.1f", maxLoss) + " Sortino "
                                + String.format("%.1f", TradeStat.getSortinoKoof(desc)) +  " -- " + desc);
                    }

                    for (SymbContainer sc : scBySymb.values())
                    {
                        Double profit = sumProfitBySymbol.get(sc.symb);
                        if (profit == null)
                            profit = sc.symTradeStat.profit;
                        else
                            profit += sc.symTradeStat.profit;
                        sumProfitBySymbol.put(sc.symb, profit);
                    }

                    /// ========================== PRINT STAT FILE PER SYMBOL
                    if (PRINT_FILES)
                        for (String symbol : scBySymb.keySet())
                        {
                            SymbContainer sc = scBySymb.get(symbol);
                            String suffix = fName.substring(0, fName.indexOf("."));
                            prepareStatFile(symbol + "-" + suffix);
                            addStat( sc.printData.toString());
                            closeStatFile();
                        }
                        // log.info("---- AvgAvg more tna Percent: " + avgAvg.getAvg());
                } // end of цикл по файлам внутри одного конфига

                    if (MAKE_CSV && history != null && history.size() > 0)
                    {
                        LearnData any = history.get(0);
                        List<String> keys = any.propVals.keySet().stream().sorted().toList();
                        List<Map<String, Object>> rows = new ArrayList<>();
                        for (LearnData ld : history)
                        {
                            Map<String, Object> row = new LinkedHashMap<>();
                            row.put("open_time", ld.openTime);
                            for (String key : keys)
                                row.put(key, ld.propVals.get(key));
                            row.put("events", ld.events == null ? Collections.emptyList() : ld.events);
                            rows.add(row);
                        }
                        Map<String, Object> jsonExport = new LinkedHashMap<>();
                        jsonExport.put("headers", keys);
                        jsonExport.put("rows", rows);
                        log.info("========= HIST JSON SIZE = " + history.size());
                        try {
                            FileOutputStream fos = new FileOutputStream("__data2.json");
                            String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonExport);
                            fos.write(json.getBytes(StandardCharsets.UTF_8));
                            fos.flush();
                            fos.close();
                        } catch (Exception e) {}
                    }

                    if (LEARN_MODE) {
                        int goodCnt = 0;
                        int badCnt = 0;
                        int neytralcnt = 0;
                        LearnData any = allLearnData.iterator().next();
                        List<String> keys = any.propVals.keySet().stream().sorted().toList();
                        StringBuffer buyBuff = new StringBuffer();
                        StringBuffer sellBuff = new StringBuffer();
                        buyBuff.append("target");
                        sellBuff.append("target");
                        for (String key : keys)
                        {
                            buyBuff.append(",").append(key);
                            sellBuff.append(",").append(key);
                        }
                        buyBuff.append("\n");
                        sellBuff.append("\n");

                        for (LearnData ld : allLearnData)
                        {
                            if (ld.result == 2.0)
                                goodCnt++;
                            else if (ld.result == 1.0)
                                neytralcnt ++;
                            else
                                badCnt++;
                            StringBuffer sb = buyBuff;
                            if (!ld.buy)
                                sb = sellBuff;
                            sb.append(ld.result.intValue()+ "");
                            for (String key : keys)
                            {
                                String val = String.format(Locale.ENGLISH, "%.6f", ld.propVals.get(key));
                                sb.append(",").append(val);
                            }
                            sb.append("\n");
                        }
                        log.info("========= AL LEARN DATA SIZE = " + allLearnData.size()
                            + " Good " + goodCnt + " Bad " + badCnt + " Good%: "
                                + String.format(Locale.ENGLISH, "%.1f", goodCnt*100.0 /allLearnData.size() ));

                        try {
                            FileOutputStream fos = new FileOutputStream("_x_buy_hist.csv");
                            fos.write(buyBuff.toString().getBytes());
                            fos.flush();
                            fos.close();
                            fos = new FileOutputStream("_x_sell_hist.csv");
                            fos.write(sellBuff.toString().getBytes());
                            fos.flush();
                            fos.close();
                        } catch (Exception e) {}
                    }

                    if (GET_NN_DATA)
                    {
                        try {
                            FileOutputStream fos = new FileOutputStream("NN_in_data_" + System.currentTimeMillis() + ".data");
                            int i = 0;
                            for (SymbContainer sc : scBySymb.values()) {
                                for (ProboyTrade pt : sc.trades2trace)
                                {
                                    if (!pt.finished) continue;
                                    if ( i % 2 == 0)
                                        fos.write(pt.getCSV().getBytes());
                                    i++;
                                }
                            }
                            fos.flush();
                            fos.close();
                        } catch (Exception e) {}
                    }

                    for (String symb : sumProfitBySymbol.keySet())
                    {
                        StringDouble sd = bestSymbProfits.get(symb);
                        Double profit = sumProfitBySymbol.get(symb);
                        if (sd == null)
                            sd = new StringDouble(desc, profit);
                        else {
                            if (profit > sd.value)
                            {
                                sd = new StringDouble(desc, profit);
                            }
                        }
                        bestSymbProfits.put(symb, sd);
                    }

                    if (maxLoss == null) {
                        log.info("LOSS > " + CRITICAL_LOSS);
                    } else {
                        log.info(" Trade Amnt: " + String.format("%.1f", TradeStat.tradeAmntGlobalByKey.get(desc)) + "   maxLoss " + String.format("%.1f", maxLoss));
                        maxLossbyDesc.put(desc, maxLoss);
                        if (maxLoss < CRITICAL_LOSS) {
                            String _desc = desc + "  / tradeCnt = " + tradeCnt;
                            //descByProfit.put(avgProfPercent.getAvg(), _desc); // для сортировки по среднему профиту за 1 файл
                            descByProfit.put(new Double(TradeStat.tradeAmntGlobalByKey.get(desc)), _desc);
                            descBySortino.put(TradeStat.getSortinoKoof(desc), _desc);
                            profByDesc.put(_desc, new Double(TradeStat.tradeAmntGlobalByKey.get(desc)));
                            if (TradeStat.tradeAmntGlobalByKey.get(desc) > 1000.0)
                                counters.goodCnt++;
                        }
                    }
                    counters.doneCnt++;
                    log.info("Done " + counters.doneCnt + " of " + counters.totalCnt + " " + String.format("%.1f", counters.doneCnt * 100.0 / (double) counters.totalCnt) + " %");
                });
                ////
            //////////////////////////////////// SAVE CSV ////////////////////////////////
            /*
            List<String> headers = new ArrayList<>();
            Map<String, Double> series = dataSetByTime.values().iterator().next();
            for (String name : series.keySet())
                headers.add(name);
            StringBuilder sb = new StringBuilder();
            sb.append("Time");
            for (String h : headers)
            {
                sb.append(";").append(h);
            }
            sb.append("\n");
            List<CandleResp> klines = klinesBySymbol.get("BTCUSDT");
            for (int i=0; i < klines.size(); i++)
            {
                CandleResp cr = klines.get(i);
                String time = sdf2.format(new Date(cr.openTime));
                sb.append(time);
                series = dataSetByTime.get(time);
                for (String h : headers)
                {
                    sb.append(";");
                    if (series != null && series.get(h) != null) sb.append(series.get(h));
                }
                sb.append("\n");
            }
            FileOutputStream fos = new FileOutputStream("_zod.csv");
            fos.write(sb.toString().getBytes());
            fos.flush();
            fos.close();*/

            List<Double> sortedKeys = new ArrayList(descByProfit.keySet());
            List<Double> sortedKeysSortino = new ArrayList(descBySortino.keySet());
            Collections.sort(sortedKeys, Collections.reverseOrder());
            Collections.sort(sortedKeysSortino, Collections.reverseOrder());
            int i=0;
            log.info(" --- All variants good %: " + String.format("%.1f", (double)(100.0 * counters.goodCnt / counters.totalCnt)));
            log.info(" --- TOP by Profit ---");
            for (Double key : sortedKeys)
            {
                String desc = descByProfit.get(key);
                double sortino = 0.0;
                try {
                    sortino = TradeStat.getSortinoKoof(desc);
                } catch (Exception e) {}
                log.info(" ->  " + desc + " prof: " + String.format("%.2f", profByDesc.get(descByProfit.get(key))) + " Sortino: " + String.format("%.2f", sortino));
                i++;
                if (i > 50)
                    break;
            }
            log.info(" --- TOP by Sortino ---");
            i = 0;
            for (Double key : sortedKeysSortino)
            {
                String desc = descBySortino.get( key );
                Double _prof = profByDesc.get( desc );
                log.info(" ->  " + desc + " maxLoss " +
                        String.format("%.1f", maxLossbyDesc.get(desc.substring(0, desc.indexOf("  / tradeCnt"))))
                        + " prof: " + String.format("%.1f", _prof) + " Sortino: " + String.format("%.2f", key));
                i++;
                if (i > 50)
                    break;
            }
                if (PRINT_TOP_OF_SYMBOLS) {
                    log.info("\r\n Best Symbol configs:");
                    for (String symb : bestSymbProfits.keySet()) {
                        StringDouble sd = bestSymbProfits.get(symb);
                        log.info(symb + " Profit: " + String.format("%.1f", sd.value) + " " + sd.desc);
                    }
                }
/*
            log.info("Writing 1m file....");
            //Bobo2 klineContainer = loadCandlesBinance("2025-01-26 00:00:00", latestSignalTime, "ETHUSDT", false, 1);
            Bobo2 klineContainer = loadCandlesBinance("2025-02-16 16:00:00", latestSignalTime, "ETHUSDT", false, 1);
            double anyUp = upCollector.values().iterator().next();
            double anyDown = downCollector.values().iterator().next();
            double anyUp2 = upCollector2.values().iterator().next();
            double anyDown2 = downCollector2.values().iterator().next();
            prepareStatFile("_all_ETH");
            StringBuilder printData = new StringBuilder();
            for (CandleResp cr : klineContainer.candles)
            {
                long time = cr.openTime;
                Double up = upCollector.get(time);
                Double down = downCollector.get(time);
                Double up2 = upCollector2.get(time);
                Double down2 = downCollector2.get(time);
                if (up == null)
                    up = anyUp;
                else
                    anyUp = up;
                if (down == null)
                    down = anyDown;
                else
                    anyDown = down;

                if (up2 == null)
                    up2 = anyUp2;
                else
                    anyUp2 = up2;
                if (down2 == null)
                    down2 = anyDown2;
                else
                    anyDown2 = down2;
                printData.append("['").append(sdf2.format(new Date(cr.openTime))).append("'")
                        .append(",").append(String.format("%.6f", cr.l).replace(',', '.'))
                        .append(",").append(String.format("%.6f", cr.o).replace(',', '.'))
                        .append(",").append(String.format("%.6f", cr.c).replace(',', '.'))
                        .append(",").append(String.format("%.6f", cr.h).replace(',', '.'))
                        .append(",").append(String.format("%.6f", up).replace(',', '.'))
                        .append(",").append(String.format("%.6f", down).replace(',', '.'))
                        .append(",").append(String.format("%.6f", up2).replace(',', '.'))
                        .append(",").append(String.format("%.6f", down2).replace(',', '.'))
                        //          .append(",").append(String.format("%.6f", prevV3).replace(',', '.'))
                        //        .append(",").append(String.format("%.6f", prevV4).replace(',', '.'))
                        .append("],");
            }
            addStat( printData.toString());
            closeStatFile(); */
            Thread.sleep(3_000L);
        } catch (Throwable t)
        {
            t.printStackTrace();
        }
    }

    // coint rating by Symbol.  max rating is TOP traded (BTC)
    Map<String, Integer> topTradedCoins = new HashMap<>();

    boolean REAL_RUN = true;
    final int LSR_MINUTES = 15;

    @PostConstruct
    private void init()
    {
        System.setProperty("java.awt.headless", "true");
        // System.exit(0);

        // symbolInfos = new HashMap<>();
        // symbolInfos.put("BTCUSDT",new SymbolInfo("BTCUSDT"));
        // initCandles(60);
        // sentTgMessage("BTCUSDT", true, 97000, 12341234);
        //if (true) return;
        testProboys();
        System.exit( 0 );
        //REAL_RUN = false;

        log.info("============ STARTING ====================== REAL_RUN = " + REAL_RUN + "   <---------- ");
        dealPacks.add(new DealPack(MAX_DEALS, INITIAL_PROFIT_KOOF));
        log.info("Working with " + dealPacks.size() + " deal Packs.");

        long serverTime = binancer.getServerTime(true);

        symbolInfos = binancer.getSymbolInfo(true);
        ///////////////////////////////////////////////////////////
        /*
        List<String> _allSymbols = new ArrayList<>();
        for (String s : symbolInfos.keySet()) _allSymbols.add(s);
        BinanceHandler.instruments = _allSymbols;
        binanceFutHandler.init();
        binanceFutHandlerTrades.init();
        String _symb = "MOODENGUSDT";
        BinOrder bo = new BinOrder(BinOrder.SIDE_BUY, BinOrder.TYPE_MARKET);

        bo.setSymbol( _symb );
        bo.setQuantity( 65.0 );
        // bo.setReduceOnly( true );
        SymbolInfo sInfo = symbolInfos.get( _symb );
        BinOrder mainOrder = binancer.sendOrder(bo, sInfo.getDigitsAfterDotPrice(), sInfo.digitsAfterDotLots);
        try { Thread.sleep(2_000); } catch (Exception e) {}
        //// ....
        BinOrder tpOrder = createLimitOrder("testTP", bo.getSide(), bo.getQuantity(), 0.08625, _symb, sInfo.getDigitsAfterDotPrice(), sInfo.digitsAfterDotLots, BinOrder.TAKE_PROFIT);
        log.info("id = " + tpOrder.getOrderId());
        BinOrder slOrder = createLimitOrder("testSL", bo.getSide(), bo.getQuantity(), 0.084, _symb, sInfo.getDigitsAfterDotPrice(), sInfo.digitsAfterDotLots, BinOrder.STOP_LOSS);
        log.info("id = " + slOrder.getOrderId());
        try { Thread.sleep(1_000); } catch (Exception e) {}
        binancer.delOrder(slOrder);
        slOrder = createLimitOrder("testSL", bo.getSide(), bo.getQuantity(), 0.0845, _symb, sInfo.getDigitsAfterDotPrice(), sInfo.digitsAfterDotLots, BinOrder.STOP_LOSS);
        log.info("id = " + slOrder.getOrderId());

        try {
            Thread.sleep(600_000);
        } catch (Exception e) {}
        System.exit(0); */
        ///////////////////////////////////////////////////////////
        if (REAL_RUN)
            symbolInfos = binancer.leverageFilter(true, symbolInfos, 5, 50_000.0); // ВКЛЮЧИТЬ !!!

        // Для Binance оставляем только безопасные ASCII-символы.
        // Иначе отдельные невалидные тикеры ломают REST/WS инициализацию (HTTP 400).
        Map<String, SymbolInfo> filteredSymbolInfos = new HashMap<>();
        List<String> skippedSymbols = new ArrayList<>();
        for (Map.Entry<String, SymbolInfo> entry : symbolInfos.entrySet()) {
            String symbol = entry.getKey();
            if (isSafeBinanceSymbol(symbol)) {
                filteredSymbolInfos.put(symbol, entry.getValue());
            } else {
                skippedSymbols.add(symbol);
            }
        }
        symbolInfos = filteredSymbolInfos;
        if (!skippedSymbols.isEmpty()) {
            log.error("!!! CRITICAL: Из Binance-набора исключены невалидные символы ({}): {}",
                    skippedSymbols.size(), skippedSymbols);
        }

        Map<String, SymbolInfo> bbSymbolInfos = buyBitService.getInstruments();
        /*int ii = 0;
        for (String s : bbSymbolInfos.keySet())
        {
            System.out.println("bybit symbol[" + ii + "] = " + s);
            ii++;
        } System.exit(0); */

        Set<String> symbs2removeFromBinance = new HashSet<>();
        Set<String> symbs2removeFromBuyBit = new HashSet<>();
        for (String symb : symbolInfos.keySet())
        {
            if (!bbSymbolInfos.containsKey(symb))
                symbs2removeFromBinance.add(symb);
        }
        for (String symb : bbSymbolInfos.keySet())
        {
            if (!symbolInfos.containsKey(symb))
                symbs2removeFromBuyBit.add(symb);
        }

        // log.info("REMOVE FROM BINANCE: " + symbs2removeFromBinance);
        // System.exit(0);

        //for (String symb : symbs2removeFromBinance) symbolInfos.remove(symb);
        //for (String symb : symbs2removeFromBuyBit) bbSymbolInfos.remove(symb);
        //// Теперь в наборах символов есть только те, что есть на обоих биржах ----------------

        // test...
        //buyBitService.leverageFilter(bbSymbolInfos, 12);
        //log.info(" Got " + symbolInfos.size() + " symbols existing on 2 exchanges...");
        //System.exit(0);
        //
        log.info(" Got " + symbolInfos.size() + " symbols existing on 2 exchanges...");

        List<SymbolInfo> symbolsSortedByVol = binancer.get24hrStat(true, symbolInfos, 0); // сорт от малого к большому
        int i = 0;

        for (SymbolInfo si : symbolsSortedByVol)
        {
            i++;
            System.out.println("   coins.put(\""+si.symbol+"\", "+i+"); // " + i + " - 24 vol " + si.volumes24h);
            topTradedCoins.put(si.symbol, i);
        }

        log.info("Finaly we have " + symbolInfos.size() + " symbols");
        
        List<String> allSymbols = new ArrayList<>();
        for (String s : symbolInfos.keySet())
            allSymbols.add(s);
        
        BinanceHandler.instruments = allSymbols;

        BinanceTradeHandler.serverTimeDelta = serverTime - System.currentTimeMillis();

        if (REAL_RUN) {
            initCandles(120);
            // initLSR(LSR_MINUTES);
        }
        //binanceHandler.init();
        binanceFutHandler.init();
        binanceFutHandlerTrades.init();
        binanceFutHandlerAllDepth.init();
        binanceKLineHandler.init();
        // buyBitFutHandler.init();

        tradingStateStorageService.initStateOnStartup(this);
    }

    /** Проверяет, что тикер состоит только из ASCII-латиницы, цифр и '_' (без иероглифов и спецсимволов). */
    private boolean isSafeBinanceSymbol(String symbol)
    {
        if (symbol == null || symbol.isEmpty()) {
            return false;
        }
        for (int i = 0; i < symbol.length(); i++) {
            char ch = symbol.charAt(i);
            boolean isUpperLatin = ch >= 'A' && ch <= 'Z';
            boolean isDigit = ch >= '0' && ch <= '9';
            boolean isUnderscore = ch == '_';
            if (!isUpperLatin && !isDigit && !isUnderscore) {
                return false;
            }
        }
        return true;
    }


    /**  */
    private void initCandles(int limit)
    {
        if (symbolInfos == null || symbolInfos.isEmpty()) {
            log.info("Инициализация свечей пропущена: список инструментов пуст.");
            return;
        }
        for (String symbol : symbolInfos.keySet())
        {
            CandleRequest cr = new CandleRequest(symbol, limit, "15m");
            List<CandleResp> candles = null;
            try {
                candles = binancer.getCandles(true, cr);
            } catch (Exception e) {}
            if (candles == null || candles.isEmpty()) {
                log.error("!!! CRITICAL: свечи не получены для {} (interval=15m, limit={}). Логика StakanService требует свечи для корректной работы !!!",
                        symbol, limit);
                continue;
            }
            CandlePackage cp = new CandlePackage(symbol, limit);
            for (int i=0; i < candles.size(); i++)
            {
                CandleResp candleResp = candles.get(i);
                StreamCandle sc = new StreamCandle(candleResp, symbol);
                cp.addCandle(sc, true);
            }
            candlesBySymbol.put(symbol, cp);
        }
    }

    private long last24hVolumesReinitTime = System.currentTimeMillis();
    private long nextLSRCheck = 0;

    @Scheduled(fixedDelay = 2_000L)
    private void refresh24hVolumes()
    {
        //log.info("refresh volumes check");
        /*
        LongShortRatio btcLSR = lsrBySymbol.get("BTCUSDT");
        if (btcLSR != null && System.currentTimeMillis() - btcLSR.timestamp > LSR_MINUTES * 60_000L
                && System.currentTimeMillis() > nextLSRCheck)
        {   // время обновить LSR
            if (!initLSR(LSR_MINUTES))
            {   // не все проапдейчено!
                // запихнем назад тот LSR который снова даст сигнал о проверке через 2 сек.
                lsrBySymbol.put("BTCUSDT", btcLSR);
                nextLSRCheck = System.currentTimeMillis() + 100_000L; // мы не можем делать более 1000 запросов за 5 мин.
            }
        }
        */

        if (System.currentTimeMillis() - last24hVolumesReinitTime > 4 * 60 * 60_000L) {
            binancer.get24hrStat(true, symbolInfos, 0);
            last24hVolumesReinitTime = System.currentTimeMillis();
        }
    }

    /**  */
    private void initTopTradedCoinsForTests()
    {
        Map<String, Integer> coins = new HashMap<>();
        coins.put("BSWUSDT", 1); // 1 - 24 vol 4935891.3865792
        coins.put("STEEMUSDT", 2); // 2 - 24 vol 5067293.380704
        coins.put("SYSUSDT", 3); // 3 - 24 vol 5406962.5281012
        coins.put("QUICKUSDT", 4); // 4 - 24 vol 6014064.0605524
        coins.put("RIFUSDT", 5); // 5 - 24 vol 6097908.4854055
        coins.put("AKTUSDT", 6); // 6 - 24 vol 6150562.28441435
        coins.put("RLCUSDT", 7); // 7 - 24 vol 6246523.41009
        coins.put("ONGUSDT", 8); // 8 - 24 vol 6369962.3571743
        coins.put("SFPUSDT", 9); // 9 - 24 vol 6403492.0401
        coins.put("FLUXUSDT", 10); // 10 - 24 vol 6515328.123800401
        coins.put("ALPACAUSDT", 11); // 11 - 24 vol 6578070.1409664
        coins.put("USDCUSDT", 12); // 12 - 24 vol 6653140.3464903
        coins.put("NULSUSDT", 13); // 13 - 24 vol 7312392.4235886
        coins.put("WAXPUSDT", 14); // 14 - 24 vol 7932649.9401988
        coins.put("SPELLUSDT", 15); // 15 - 24 vol 8165316.1126128
        coins.put("VOXELUSDT", 16); // 16 - 24 vol 8176899.9974655
        coins.put("MTLUSDT", 17); // 17 - 24 vol 8229004.2216
        coins.put("LSKUSDT", 18); // 18 - 24 vol 8554568.788944
        coins.put("BALUSDT", 19); // 19 - 24 vol 8753456.695799999
        coins.put("SYNUSDT", 20); // 20 - 24 vol 8841540.306091001
        coins.put("CTSIUSDT", 21); // 21 - 24 vol 8861571.4308
        coins.put("SCRTUSDT", 22); // 22 - 24 vol 8948112.778655
        coins.put("KNCUSDT", 23); // 23 - 24 vol 9199265.61437
        coins.put("HIFIUSDT", 24); // 24 - 24 vol 9276148.937088799
        coins.put("JOEUSDT", 25); // 25 - 24 vol 9432630.841617301
        coins.put("BADGERUSDT", 26); // 26 - 24 vol 9448090.669192001
        coins.put("POWRUSDT", 27); // 27 - 24 vol 9671057.5007398
        coins.put("SAFEUSDT", 28); // 28 - 24 vol 9978293.6235
        coins.put("DUSKUSDT", 29); // 29 - 24 vol 1.0239496872E7
        coins.put("ARKUSDT", 30); // 30 - 24 vol 1.03369801254975E7
        coins.put("GUSDT", 31); // 31 - 24 vol 1.0377241842084E7
        coins.put("UMAUSDT", 32); // 32 - 24 vol 1.0398777717061E7
        coins.put("BNTUSDT", 33); // 33 - 24 vol 1.0635161446644E7
        coins.put("CHESSUSDT", 34); // 34 - 24 vol 1.0911212285617601E7
        coins.put("STGUSDT", 35); // 35 - 24 vol 1.09404996808994E7
        coins.put("ACEUSDT", 36); // 36 - 24 vol 1.119687544799829E7
        coins.put("TWTUSDT", 37); // 37 - 24 vol 1.1265589843572E7
        coins.put("IOSTUSDT", 38); // 38 - 24 vol 1.1302930359585E7
        coins.put("BICOUSDT", 39); // 39 - 24 vol 1.14780761786544E7
        coins.put("ICXUSDT", 40); // 40 - 24 vol 1.1650141308E7
        coins.put("FLMUSDT", 41); // 41 - 24 vol 1.1769595245000001E7
        coins.put("ILVUSDT", 42); // 42 - 24 vol 1.1932147018784E7
        coins.put("BNXUSDT", 43); // 43 - 24 vol 1.2198550670829E7
        coins.put("BATUSDT", 44); // 44 - 24 vol 1.2204183719940001E7
        coins.put("QTUMUSDT", 45); // 45 - 24 vol 1.22723778922E7
        coins.put("HIGHUSDT", 46); // 46 - 24 vol 1.2275309067905E7
        coins.put("OGNUSDT", 47); // 47 - 24 vol 1.22796719492E7
        coins.put("ETHWUSDT", 48); // 48 - 24 vol 1.2281430094692E7
        coins.put("EDUUSDT", 49); // 49 - 24 vol 1.23261137551734E7
        coins.put("BSVUSDT", 50); // 50 - 24 vol 1.2445549675110001E7
        coins.put("PERPUSDT", 51); // 51 - 24 vol 1.2484736288494198E7
        coins.put("NTRNUSDT", 52); // 52 - 24 vol 1.2514678004864E7
        coins.put("NFPUSDT", 53); // 53 - 24 vol 1.2518501056716802E7
        coins.put("RPLUSDT", 54); // 54 - 24 vol 1.268841305164606E7
        coins.put("USTCUSDT", 55); // 55 - 24 vol 1.2810885281879399E7
        coins.put("ACHUSDT", 56); // 56 - 24 vol 1.30549715739037E7
        coins.put("DENTUSDT", 57); // 57 - 24 vol 1.3357553989428E7
        coins.put("MOVRUSDT", 58); // 58 - 24 vol 1.352209461727155E7
        coins.put("ONTUSDT", 59); // 59 - 24 vol 1.352391225486E7
        coins.put("ALPHAUSDT", 60); // 60 - 24 vol 1.3569980431E7
        coins.put("RAREUSDT", 61); // 61 - 24 vol 1.36342840280682E7
        coins.put("SKLUSDT", 62); // 62 - 24 vol 1.3799904722040001E7
        coins.put("ANKRUSDT", 63); // 63 - 24 vol 1.4020243615109999E7
        coins.put("RVNUSDT", 64); // 64 - 24 vol 1.422537450282E7
        coins.put("MBOXUSDT", 65); // 65 - 24 vol 1.42922589499185E7
        coins.put("WOOUSDT", 66); // 66 - 24 vol 1.4391585031200001E7
        coins.put("MAGICUSDT", 67); // 67 - 24 vol 1.4415071128214702E7
        coins.put("PONKEUSDT", 68); // 68 - 24 vol 1.4794183898344899E7
        coins.put("SLERFUSDT", 69); // 69 - 24 vol 1.48711857797086E7
        coins.put("SWELLUSDT", 70); // 70 - 24 vol 1.5225665875551E7
        coins.put("IOTXUSDT", 71); // 71 - 24 vol 1.533429666153E7
        coins.put("VIDTUSDT", 72); // 72 - 24 vol 1.59374356428096E7
        coins.put("CELRUSDT", 73); // 73 - 24 vol 1.615537569124E7
        coins.put("BANANAUSDT", 74); // 74 - 24 vol 1.619184226856856E7
        coins.put("1000XECUSDT", 75); // 75 - 24 vol 1.629187664256E7
        coins.put("KAVAUSDT", 76); // 76 - 24 vol 1.638386092602E7
        coins.put("GASUSDT", 77); // 77 - 24 vol 1.6487302209994001E7
        coins.put("BAKEUSDT", 78); // 78 - 24 vol 1.6535506734000001E7
        coins.put("TUSDT", 79); // 79 - 24 vol 1.6666846160373699E7
        coins.put("TRUUSDT", 80); // 80 - 24 vol 1.67072581198072E7
        coins.put("IDUSDT", 81); // 81 - 24 vol 1.71039604602702E7
        coins.put("1000XUSDT", 82); // 82 - 24 vol 1.73604760594768E7
        coins.put("LRCUSDT", 83); // 83 - 24 vol 1.7519012360850003E7
        coins.put("YGGUSDT", 84); // 84 - 24 vol 1.79033634049999E7
        coins.put("XVSUSDT", 85); // 85 - 24 vol 1.7981118181298997E7
        coins.put("BANDUSDT", 86); // 86 - 24 vol 1.84691335776E7
        coins.put("ASTRUSDT", 87); // 87 - 24 vol 1.86076857606717E7
        coins.put("MYROUSDT", 88); // 88 - 24 vol 1.90929839125172E7
        coins.put("TOKENUSDT", 89); // 89 - 24 vol 1.9275275717814602E7
        coins.put("MAVUSDT", 90); // 90 - 24 vol 1.9398183030752398E7
        coins.put("CHRUSDT", 91); // 91 - 24 vol 1.9955324832000002E7
        coins.put("FIOUSDT", 92); // 92 - 24 vol 1.99794096973845E7
        coins.put("LITUSDT", 93); // 93 - 24 vol 2.02569256098E7
        coins.put("GMXUSDT", 94); // 94 - 24 vol 2.0389058217232384E7
        coins.put("SUNUSDT", 95); // 95 - 24 vol 2.04756208718763E7
        coins.put("AEROUSDT", 96); // 96 - 24 vol 2.075106813675952E7
        coins.put("ALICEUSDT", 97); // 97 - 24 vol 2.0761468157899998E7
        coins.put("COTIUSDT", 98); // 98 - 24 vol 2.0989746064319998E7
        coins.put("METISUSDT", 99); // 99 - 24 vol 2.1001625503808E7
        coins.put("HOTUSDT", 100); // 100 - 24 vol 2.1033751319620002E7
        coins.put("DARUSDT", 101); // 101 - 24 vol 2.104637489054E7
        coins.put("C98USDT", 102); // 102 - 24 vol 2.1154336191E7
        coins.put("QNTUSDT", 103); // 103 - 24 vol 2.1468700475331903E7
        coins.put("CKBUSDT", 104); // 104 - 24 vol 2.24298270547995E7
        coins.put("1000000MOGUSDT", 105); // 105 - 24 vol 2.3225646051484577E7
        coins.put("SXPUSDT", 106); // 106 - 24 vol 2.37590535754E7
        coins.put("PORTALUSDT", 107); // 107 - 24 vol 2.392408551596472E7
        coins.put("AIUSDT", 108); // 108 - 24 vol 2.4064886670088E7
        coins.put("ENJUSDT", 109); // 109 - 24 vol 2.40702561788E7
        coins.put("API3USDT", 110); // 110 - 24 vol 2.437595514621E7
        coins.put("OMNIUSDT", 111); // 111 - 24 vol 2.4627935292268E7
        coins.put("PIXELUSDT", 112); // 112 - 24 vol 2.48327001053706E7
        coins.put("POLYXUSDT", 113); // 113 - 24 vol 2.48855698928592E7
        coins.put("RDNTUSDT", 114); // 114 - 24 vol 2.49861549534222E7
        coins.put("TNSRUSDT", 115); // 115 - 24 vol 2.501578906228883E7
        coins.put("ZECUSDT", 116); // 116 - 24 vol 2.5346286385E7
        coins.put("DRIFTUSDT", 117); // 117 - 24 vol 2.56675989605568E7
        coins.put("ROSEUSDT", 118); // 118 - 24 vol 2.6168108430239998E7
        coins.put("DASHUSDT", 119); // 119 - 24 vol 2.773590477286E7
        coins.put("REZUSDT", 120); // 120 - 24 vol 2.8239929317537498E7
        coins.put("IMXUSDT", 121); // 121 - 24 vol 2.8656982035E7
        coins.put("LPTUSDT", 122); // 122 - 24 vol 2.87704985936E7
        coins.put("BRETTUSDT", 123); // 123 - 24 vol 2.8981198079267997E7
        coins.put("DYMUSDT", 124); // 124 - 24 vol 2.9410808469146997E7
        coins.put("CAKEUSDT", 125); // 125 - 24 vol 2.97741551321369E7
        coins.put("LUNA2USDT", 126); // 126 - 24 vol 2.98241390239023E7
        coins.put("CATIUSDT", 127); // 127 - 24 vol 2.99769692593646E7
        coins.put("DEGENUSDT", 128); // 128 - 24 vol 3.0124321069437E7
        coins.put("AERGOUSDT", 129); // 129 - 24 vol 3.01812809101056E7
        coins.put("MANTAUSDT", 130); // 130 - 24 vol 3.1389085644568957E7
        coins.put("VANRYUSDT", 131); // 131 - 24 vol 3.1411393595505003E7
        coins.put("XAIUSDT", 132); // 132 - 24 vol 3.1850051198777E7
        coins.put("AXLUSDT", 133); // 133 - 24 vol 3.1903862026476722E7
        coins.put("CELOUSDT", 134); // 134 - 24 vol 3.1980708805499997E7
        coins.put("LISTAUSDT", 135); // 135 - 24 vol 3.21273385760649E7
        coins.put("ONEUSDT", 136); // 136 - 24 vol 3.310669859088E7
        coins.put("EGLDUSDT", 137); // 137 - 24 vol 3.37779703996E7
        coins.put("HMSTRUSDT", 138); // 138 - 24 vol 3.3871362265917E7
        coins.put("ORCAUSDT", 139); // 139 - 24 vol 3.469321786209792E7
        coins.put("SSVUSDT", 140); // 140 - 24 vol 3.46941897125761E7
        coins.put("1000RATSUSDT", 141); // 141 - 24 vol 3.51203478279928E7
        coins.put("CFXUSDT", 142); // 142 - 24 vol 3.52323388370954E7
        coins.put("SUPERUSDT", 143); // 143 - 24 vol 3.54702930469265E7
        coins.put("KDAUSDT", 144); // 144 - 24 vol 3.5828384541701995E7
        coins.put("KSMUSDT", 145); // 145 - 24 vol 3.59238977515E7
        coins.put("SNXUSDT", 146); // 146 - 24 vol 3.62872301792E7
        coins.put("KASUSDT", 147); // 147 - 24 vol 3.7649942693200804E7
        coins.put("1000LUNCUSDT", 148); // 148 - 24 vol 3.8190455679182395E7
        coins.put("YFIUSDT", 149); // 149 - 24 vol 3.890356916E7
        coins.put("1INCHUSDT", 150); // 150 - 24 vol 3.9336354766600005E7
        coins.put("NEOUSDT", 151); // 151 - 24 vol 4.025880324848E7
        coins.put("ALTUSDT", 152); // 152 - 24 vol 4.03133043026445E7
        coins.put("MEWUSDT", 153); // 153 - 24 vol 4.07827072181696E7
        coins.put("GMTUSDT", 154); // 154 - 24 vol 4.117189369821E7
        coins.put("MINAUSDT", 155); // 155 - 24 vol 4.20919449289527E7
        coins.put("KOMAUSDT", 156); // 156 - 24 vol 4.2773291369132005E7
        coins.put("FIDAUSDT", 157); // 157 - 24 vol 4.30374995459376E7
        coins.put("MEMEUSDT", 158); // 158 - 24 vol 4.33895883893433E7
        coins.put("GRTUSDT", 159); // 159 - 24 vol 4.685647073628E7
        coins.put("HIPPOUSDT", 160); // 160 - 24 vol 4.81190793056145E7
        coins.put("BIGTIMEUSDT", 161); // 161 - 24 vol 4.8126532731920995E7
        coins.put("FLOWUSDT", 162); // 162 - 24 vol 4.81913143216E7
        coins.put("COMPUSDT", 163); // 163 - 24 vol 5.238237064795E7
        coins.put("SAGAUSDT", 164); // 164 - 24 vol 5.2613914568837464E7
        coins.put("GLMUSDT", 165); // 165 - 24 vol 5.32544051154536E7
        coins.put("JTOUSDT", 166); // 166 - 24 vol 5.3358737583498E7
        coins.put("XMRUSDT", 167); // 167 - 24 vol 5.3988417615660004E7
        coins.put("AGLDUSDT", 168); // 168 - 24 vol 5.42027328721935E7
        coins.put("AEVOUSDT", 169); // 169 - 24 vol 5.688934124781075E7
        coins.put("MANAUSDT", 170); // 170 - 24 vol 5.94106361856E7
        coins.put("ARUSDT", 171); // 171 - 24 vol 6.1620801520500004E7
        coins.put("CHZUSDT", 172); // 172 - 24 vol 6.235031144453E7
        coins.put("IOTAUSDT", 173); // 173 - 24 vol 6.263974357926E7
        coins.put("AXSUSDT", 174); // 174 - 24 vol 6.3194717685E7
        coins.put("KAIAUSDT", 175); // 175 - 24 vol 6.4214547318525E7
        coins.put("STRKUSDT", 176); // 176 - 24 vol 6.508911939959375E7
        coins.put("NEIROETHUSDT", 177); // 177 - 24 vol 6.54157457777912E7
        coins.put("JUPUSDT", 178); // 178 - 24 vol 6.78662002184148E7
        coins.put("ARKMUSDT", 179); // 179 - 24 vol 7.001013304802701E7
        coins.put("BLURUSDT", 180); // 180 - 24 vol 7.2172604468448E7
        coins.put("GTCUSDT", 181); // 181 - 24 vol 7.285984462660001E7
        coins.put("RSRUSDT", 182); // 182 - 24 vol 7.447731451545599E7
        coins.put("TRBUSDT", 183); // 183 - 24 vol 7.4493295083E7
        coins.put("THETAUSDT", 184); // 184 - 24 vol 7.454022781808001E7
        coins.put("AUCTIONUSDT", 185); // 185 - 24 vol 7.58273451613997E7
        coins.put("VETUSDT", 186); // 186 - 24 vol 7.6388919544062E7
        coins.put("POLUSDT", 187); // 187 - 24 vol 7.917119785543801E7
        coins.put("ICPUSDT", 188); // 188 - 24 vol 8.0721212377902E7
        coins.put("WUSDT", 189); // 189 - 24 vol 8.21970152294343E7
        coins.put("PEOPLEUSDT", 190); // 190 - 24 vol 8.407555493478E7
        coins.put("SPXUSDT", 191); // 191 - 24 vol 8.52779399585696E7
        coins.put("NOTUSDT", 192); // 192 - 24 vol 8.5985279363291E7
        coins.put("PENDLEUSDT", 193); // 193 - 24 vol 8.998704904324141E7
        coins.put("MORPHOUSDT", 194); // 194 - 24 vol 9.014407450068013E7
        coins.put("BANUSDT", 195); // 195 - 24 vol 9.038727546384999E7
        coins.put("DOGSUSDT", 196); // 196 - 24 vol 9.09231428252472E7
        coins.put("MKRUSDT", 197); // 197 - 24 vol 9.307871182841E7
        coins.put("TROYUSDT", 198); // 198 - 24 vol 9.463084521477899E7
        coins.put("XTZUSDT", 199); // 199 - 24 vol 9.597099112020001E7
        coins.put("IOUSDT", 200); // 200 - 24 vol 9.81310028182635E7
        coins.put("SCRUSDT", 201); // 201 - 24 vol 9.8460582206364E7
        coins.put("ACXUSDT", 202); // 202 - 24 vol 1.0057521726766855E8
        coins.put("RENDERUSDT", 203); // 203 - 24 vol 1.0386647315689974E8
        coins.put("PYTHUSDT", 204); // 204 - 24 vol 1.0572155134117019E8
        coins.put("OMUSDT", 205); // 205 - 24 vol 1.0662723281318901E8
        coins.put("INJUSDT", 206); // 206 - 24 vol 1.091249289277932E8
        coins.put("THEUSDT", 207); // 207 - 24 vol 1.102155001710922E8
        coins.put("COSUSDT", 208); // 208 - 24 vol 1.1208142124404919E8
        coins.put("TAOUSDT", 209); // 209 - 24 vol 1.1513289831946E8
        coins.put("GRASSUSDT", 210); // 210 - 24 vol 1.2742539609214029E8
        coins.put("JASMYUSDT", 211); // 211 - 24 vol 1.3355728942032501E8
        coins.put("BOMEUSDT", 212); // 212 - 24 vol 1.376025786760647E8
        coins.put("ATOMUSDT", 213); // 213 - 24 vol 1.3938249791232002E8
        coins.put("APEUSDT", 214); // 214 - 24 vol 1.44143934328E8
        coins.put("SEIUSDT", 215); // 215 - 24 vol 1.50785522133883E8
        coins.put("POPCATUSDT", 216); // 216 - 24 vol 1.546865491756415E8
        coins.put("MOCAUSDT", 217); // 217 - 24 vol 1.5770798975081998E8
        coins.put("DYDXUSDT", 218); // 218 - 24 vol 1.6063538455499998E8
        coins.put("MASKUSDT", 219); // 219 - 24 vol 1.611825609285E8
        coins.put("TONUSDT", 220); // 220 - 24 vol 1.6152562631854978E8
        coins.put("EOSUSDT", 221); // 221 - 24 vol 1.6184578994349998E8
        coins.put("RUNEUSDT", 222); // 222 - 24 vol 1.636666215161E8
        coins.put("BBUSDT", 223); // 223 - 24 vol 1.639387799367777E8
        coins.put("SUSHIUSDT", 224); // 224 - 24 vol 1.639989541122E8
        coins.put("VELODROMEUSDT", 225); // 225 - 24 vol 1.7345664065318123E8
        coins.put("ALGOUSDT", 226); // 226 - 24 vol 1.8345208515849E8
        coins.put("ETHFIUSDT", 227); // 227 - 24 vol 1.998536418304202E8
        coins.put("CETUSUSDT", 228); // 228 - 24 vol 2.10365693547959E8
        coins.put("GALAUSDT", 229); // 229 - 24 vol 2.1449311215379998E8
        coins.put("CHILLGUYUSDT", 230); // 230 - 24 vol 2.181490003471647E8
        coins.put("SANDUSDT", 231); // 231 - 24 vol 2.2121647797744003E8
        coins.put("OPUSDT", 232); // 232 - 24 vol 2.3207208332407576E8
        coins.put("LDOUSDT", 233); // 233 - 24 vol 2.39528248240068E8
        coins.put("MEUSDT", 234); // 234 - 24 vol 2.402519550339996E8
        coins.put("ORDIUSDT", 235); // 235 - 24 vol 2.42855170438573E8
        coins.put("1000BONKUSDT", 236); // 236 - 24 vol 2.5390678304965898E8
        coins.put("1000FLOKIUSDT", 237); // 237 - 24 vol 2.5787596799379238E8
        coins.put("MOVEUSDT", 238); // 238 - 24 vol 2.645768915204064E8
        coins.put("NEARUSDT", 239); // 239 - 24 vol 2.690587871712E8
        coins.put("APTUSDT", 240); // 240 - 24 vol 2.69712782221995E8
        coins.put("BCHUSDT", 241); // 241 - 24 vol 2.6991051641862E8
        coins.put("TIAUSDT", 242); // 242 - 24 vol 2.831661743093478E8
        coins.put("UNIUSDT", 243); // 243 - 24 vol 2.875456723696E8
        coins.put("AVAUSDT", 244); // 244 - 24 vol 2.916683283059951E8
        coins.put("EIGENUSDT", 245); // 245 - 24 vol 3.043052548986555E8
        coins.put("FILUSDT", 246); // 246 - 24 vol 3.19409059375E8
        coins.put("ARBUSDT", 247); // 247 - 24 vol 3.24887471714673E8
        coins.put("ETCUSDT", 248); // 248 - 24 vol 3.3308970448428E8
        coins.put("STXUSDT", 249); // 249 - 24 vol 3.352262974322038E8
        coins.put("XLMUSDT", 250); // 250 - 24 vol 3.4305682876428E8
        coins.put("ACTUSDT", 251); // 251 - 24 vol 3.4727527920883924E8
        coins.put("TRXUSDT", 252); // 252 - 24 vol 3.4990574346468E8
        coins.put("HBARUSDT", 253); // 253 - 24 vol 3.5373701062065E8
        coins.put("DOTUSDT", 254); // 254 - 24 vol 3.706169543272E8
        coins.put("CRVUSDT", 255); // 255 - 24 vol 3.8433585421000004E8
        coins.put("MOODENGUSDT", 256); // 256 - 24 vol 4.26686905137082E8
        coins.put("GOATUSDT", 257); // 257 - 24 vol 4.573869807525153E8
        coins.put("LTCUSDT", 258); // 258 - 24 vol 4.6298812191984004E8
        coins.put("VANAUSDT", 259); // 259 - 24 vol 4.7103449120002145E8
        coins.put("WLDUSDT", 260); // 260 - 24 vol 4.989016791332052E8
        coins.put("VIRTUALUSDT", 261); // 261 - 24 vol 5.1786165460716873E8
        coins.put("WIFUSDT", 262); // 262 - 24 vol 5.86100355637783E8
        coins.put("PNUTUSDT", 263); // 263 - 24 vol 5.898123435381856E8
        coins.put("1000CATUSDT", 264); // 264 - 24 vol 6.157761448472397E8
        coins.put("AVAXUSDT", 265); // 265 - 24 vol 6.301694277616E8
        coins.put("UXLINKUSDT", 266); // 266 - 24 vol 6.928161842684125E8
        coins.put("ENSUSDT", 267); // 267 - 24 vol 7.42856469112E8
        coins.put("BNBUSDT", 268); // 268 - 24 vol 7.5234081074522E8
        coins.put("ONDOUSDT", 269); // 269 - 24 vol 8.48649110301169E8
        coins.put("ADAUSDT", 270); // 270 - 24 vol 8.8029080287368E8
        coins.put("AAVEUSDT", 271); // 271 - 24 vol 1.0231584325469998E9
        coins.put("ENAUSDT", 272); // 272 - 24 vol 1.1763234773054514E9
        coins.put("SUIUSDT", 273); // 273 - 24 vol 1.3150024299290946E9
        coins.put("LINKUSDT", 274); // 274 - 24 vol 1.45128316531968E9
        coins.put("COWUSDT", 275); // 275 - 24 vol 1.5368165084015825E9
        coins.put("1000PEPEUSDT", 276); // 276 - 24 vol 1.8356879903683906E9
        coins.put("DOGEUSDT", 277); // 277 - 24 vol 2.727596736895728E9
        coins.put("SOLUSDT", 278); // 278 - 24 vol 3.4463250151789002E9
        coins.put("XRPUSDT", 279); // 279 - 24 vol 4.7555832413451605E9
        coins.put("ETHUSDT", 280); // 280 - 24 vol 1.714983350564352E10
        coins.put("BTCUSDT", 281); // 281 - 24 vol 3.1364971381715843E10
        topTradedCoins = coins;
    }
    
    /**  */
    private void getBtcCandlesForValues()
    {
                // ========= Достать часовые свечи по битку для оценки средних объемов =====
        CandleRequest req = new CandleRequest("BTCUSDT", 100, "1h");
        log.info("Get 100 candles for BTCUSDT to check avg values");
        List<CandleResp> hist = null;
        try {
            hist = binancer.getCandles(false, req);
        } catch (Exception e) {
            log.error("ERROR 1h CANDLES FOR BTCUSDT", e);
            return;
        }
        if (hist == null || hist.size() < 1) {
            log.info("CANT GET 1h CANDLES FOR BTCUSDT");
            return;
        }
        
        final int PART_BAR_CNT = 10;
        
        double totalVol = 0.0;
        double lastPartVol = 0.0;
        CandleResp c = null;
        for (int i=0; i < hist.size(); i++)
        {
            c = hist.get(i);
            totalVol += c.vol;
            if (i > hist.size() - PART_BAR_CNT - 1)
                lastPartVol += c.vol;
        }
        double allAvgVol = totalVol / hist.size();
        double lastPartAvgVol = lastPartVol / PART_BAR_CNT;
        log.info("AVG VOL:           " + String.format("%.2f", allAvgVol));
        log.info("LAST PART AVG VOL: " + String.format("%.2f", lastPartAvgVol));
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis( System.currentTimeMillis() );
        int curMinute = cal.get(Calendar.MINUTE);
        double lastPartVolNormalized = lastPartAvgVol;
        if (curMinute > 0)
        {
            lastPartVolNormalized = lastPartVol - c.vol + c.vol / (double)curMinute * 60.0;
        }
        double lastPartAvgVolNormalized = lastPartVolNormalized / PART_BAR_CNT;
        log.info("Last bar minutes: " + curMinute);
        log.info("LAST PART AVG VOL N: " + String.format("%.2f", lastPartAvgVolNormalized));
        //System.exit(0);
    }
    
 
    /**  */
    public void clearBinanceTrades()
    {
        // TODO: !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        //    Очистить историю трейдрв!!
        binanceFrame = new TimeFrame( TIMEFRAME_SIZE );
        binanceFrameBig = new TimeFrame( TIMEFRAME_SIZE_BIG );
        // binancer.resetHistory(); // Если сбросить историю то будут более точные данные НО! зато нам снова придется ждать пока там история накопиться, что плохо
    }

    
    /**  */
    public void clearBinanceStakan(boolean futures)
    {
        log.info("Setting binanceStakan = null, futures = " + futures);
        
        if (!futures)
            bstakanBySymbol = new HashMap<>();
        else 
            bstakanBySymbolFutures = new HashMap();
    }
    
    private List<Long> pnlPrintTimeForDealPack = new ArrayList<>();
    private Long getPnlPrintTime(int i)
    {
        while (pnlPrintTimeForDealPack.size() < i+1)
            pnlPrintTimeForDealPack.add(new Long(0));
        return pnlPrintTimeForDealPack.get(i);
    }
    private void setPnlPrintTime(int i)
    {
        pnlPrintTimeForDealPack.set(i, new Long(System.currentTimeMillis()));
    }

    Map<String, Double> maxPrices = new HashMap<>();
    Map<String, Double> minPrices = new HashMap<>();
    
    long lastReinitTime = 0L;
    int totalPNLSignalCounter = 0;

    private void storeMinMaxPrices(String symbol, Double price)
    {
        synchronized (maxPrices) {
            Double maxPrice = maxPrices.get(symbol);
            if (maxPrice == null || maxPrice < price)
                maxPrices.put(symbol, price);

            Double minPrice = minPrices.get(symbol);
            if (minPrice == null || minPrice > price)
                minPrices.put(symbol, price);
        }
    }

    /**  */
    private MinMax getMinMaxPrices(String symbol, Double defaultPrice)
    {
        MinMax rv = new MinMax();
        synchronized (maxPrices)
        {
            rv.min = minPrices.remove(symbol);
            rv.max = maxPrices.remove(symbol);
        }
        if (rv.max == null)
            rv.max = defaultPrice;
        if (rv.min == null)
            rv.min = defaultPrice;
        return rv;
    }

    /**  */
    public synchronized void updateBinanceLiquidations(Liquidation li)
    {
        String comment = "BUY (go up) ";
        if (!li.buy)
            comment = "SELL (go down) ";

        log.info("-$- Liquidation " + comment + li.symbol + "  " + (int)li.qtyUSD + " USD  "
            + " price " + formatPriceForSignalLog(li.price)
            + " avgPrice " + formatPriceForSignalLog(li.avgPrice));
    }

    Map<String, MMATema> aggresiveBuyVolumesBySymbol = new ConcurrentHashMap<>();
    Map<String, MMATema> aggresiveSellVolumesBySymbol = new ConcurrentHashMap<>();
    Map<String, MMATema> priceMoversBySymbol = new ConcurrentHashMap<>();

    /** Запоминает объем рыночных Buy/Sell ордеров, чтобы считать Aggressive Volume Ratio (taker volume / total volume)
     *   к моменту сигнала   */
    private void storeAVRData(BTrade trade)
    {
        Map<String, MMATema> aggresiveVolumesBySymbol = aggresiveSellVolumesBySymbol;
        if (!trade.isBuyerMarketMaker)
        {   // лимитник - продавец
            // По рынку (агрессивный!) - покупатель
            aggresiveVolumesBySymbol = aggresiveBuyVolumesBySymbol;
        }
        MMATema volSummator = aggresiveVolumesBySymbol.get(trade.symbol);
        if (volSummator == null)
        {
            volSummator = new MMATema(1, 15_000);
            aggresiveVolumesBySymbol.put(trade.symbol, volSummator);
        }
        MMATema priceMove = priceMoversBySymbol.get(trade.symbol);
        if (priceMove == null)
        {
            priceMove = new MMATema(1, 15_000);
            priceMoversBySymbol.put(trade.symbol, priceMove);
        }
        priceMove.addStat(trade.price, trade.time);
        volSummator.addStat(trade.qty, trade.time);
    }
    /**  */
    private double getPriceMove(String symbol, double price)
    {
        MMATema priceMove = priceMoversBySymbol.get(symbol);
        if (priceMove == null)
            return 0.0;
        return price - priceMove.getFirst();
    }

    /**  */
    private double getAgressiveBuyVol(String symbol)
    {
        MMATema summator = aggresiveBuyVolumesBySymbol.get(symbol);
        if (summator == null)
            return 0.0;
        return summator.getSum();
    }
    /**  */
    private double getAgressiveSellVol(String symbol)
    {
        MMATema summator = aggresiveSellVolumesBySymbol.get(symbol);
        if (summator == null)
            return 0.0;
        return summator.getSum();
    }

    /**
     * Форматирует цену для логов сигналов без научной нотации и без искусственного обрезания до 6 знаков.
     * Это важно, потому что часть пайплайна восстанавливает события из логов.
     */
    private String formatPriceForSignalLog(double price)
    {
        if (Double.isNaN(price) || Double.isInfinite(price))
            return "0";
        return BigDecimal.valueOf(price)
                .setScale(12, RoundingMode.HALF_UP)
                .stripTrailingZeros()
                .toPlainString();
    }

    Map<String, MMATema> tradeSpeedBySymbol = new HashMap<>();
    final long TRADE_SPEED_TIME_WINDOW = 2_000;

    /** Запоминает прошедший объем сделки (добавляет к объему который недавно прошел по этой же цене)     
     */
    public synchronized void updateBinanceTrades(BTrade trade, boolean futures)
    {
        try {
            if (futures) {
                storeMinMaxPrices(trade.symbol, trade.price);
                storeAVRData(trade);

                MMATema tradeSpeed = tradeSpeedBySymbol.get(trade.symbol);
                if (tradeSpeed == null)
                {
                    tradeSpeed = new MMATema(1, TRADE_SPEED_TIME_WINDOW);
                    tradeSpeedBySymbol.put(trade.symbol, tradeSpeed);
                }
                tradeSpeed.addStat(trade.qty * trade.price, System.currentTimeMillis());
            }
            findLevels( trade, futures );
            if (futures) {

                List<Long> postEventTriggers = getTriggeredEventIdsForSymbol(trade.symbol);
                if (!postEventTriggers.isEmpty())
                {
                    for (Long eventId : postEventTriggers)
                    {
                        log.info("-++- " + trade.symbol + " eId " + eventId
                                + " Price: " + formatPriceForSignalLog(trade.price)
                                + " Buy interest: " + getInterest(trade.symbol,true, true)
                                + " Sell interest: " + getInterest(trade.symbol,true, false)
                                + getExtraString(trade.symbol, trade.price));

                        Double price = globalUpIds.remove(eventId);
                        if (price != null)
                        { // POST signal движения вверх
                            if (trade.price >= price) {
                                globalUpPostSignalsL.addStat(1, System.currentTimeMillis());
                            } else {
                                globalUpPostSignalsL.addStat(-1, System.currentTimeMillis());
                            }
                        } else
                        {
                            price = globalDownIds.remove(eventId);
                            if (price != null)
                            { // POST signal движения вниз
                                if (trade.price <= price)
                                {
                                    globalDownPostSignalsL.addStat(1, System.currentTimeMillis());
                                } else {
                                    globalDownPostSignalsL.addStat(-1, System.currentTimeMillis());
                                }
                            }
                        }
                    }
                }

                // checkIfProboyOldOtskokLevel( trade );
                for (int i = 0; i < dealPacks.size(); i++) {   // Обработка пакета сделок. Если его нет - просто пройдем мимо
                    
                    DealPack dp = dealPacks.get(i);
                    
                    double totalPNL = 0;
                    double totalExpences = 0;
                    Set<Deal> _currDeals = dp.deals;
                    Deal deal1st = null;
                    int dealCounter = 0;
                    upCnt = 0;
                    downCnt = 0;
                    long printTime = getPnlPrintTime(i);
                    boolean shouldPrintDetails = System.currentTimeMillis() - printTime > 60_000L;
                    StringBuilder dealDetailedText = shouldPrintDetails ? new StringBuilder(512) : null;
                    for (Deal d : _currDeals) {
                        if (d.symbol.equals(trade.symbol)) {
                            d.processNewPrices(trade.price, BALANCE);
                            deal1st = d;
                        }
                        if (shouldPrintDetails) {
                            String side = d.up ? "BUY " : "SELL";
                            String proboy = d.proboy ? "PROBOY" : "OTSKOK";
                            dealDetailedText
                                    .append("    ")
                                    .append(d.symbol).append(" ")
                                    .append(proboy).append(" ")
                                    .append(side)
                                    .append(" qty ").append(String.format("%.6f", d.getTotalQty()))
                                    .append(" avgPrice ").append(String.format("%.6f", d.getAvgPrice()))
                                    .append(" pnl ").append(String.format("%.2f", d.getPNL()))
                                    .append(" quant cnt ").append(d.quants.size())
                                    .append("\r\n");
                        }
                        totalPNL += d.getPNL();
                        dealCounter ++;
                        totalExpences += d.getFullComissionExpences();
                        if (d.up)
                            upCnt++;
                        else
                            downCnt++;

                        if ((System.currentTimeMillis() - d.openTime) > MAX_HOURS * 60 * 60_000L)
                        {
                            log.info("Closing Deal " + d.symbol + " buy time...");
                            closeDeal(d, false);
                            //tgSender.sendText("Closing " + d.symbol + " " + (d.up ? "BUY" : "SELL") + " pnl " + d.getPNL());
                        }
                    }
                    
                    if (deal1st != null) {
                        // TODO: закрыть все если мы заработали более ХХХ
                        // TRADE_AMOUNT_REINITED - баланс ДО запуска пула
                        currentFullBalance = BALANCE + totalPNL;
                        double one_percent = BALANCE / 100.0;
                        double grow = totalPNL / one_percent;

                        if (shouldPrintDetails) {
                            // TODO: надо починить вычисление PNL для накопительного режима. т.к. и currentLotSize меняется и price !
                            log.info( "\r\n   PNL: " + String.format("%.2f", totalPNL)
                                    + "\r\n   exp: " + String.format("%.2f", totalExpences)
                                    + "\r\n   initial Balance:  " + String.format("%.2f", BALANCE)
                                    + "\r\n   currFull Balance: " + String.format("%.2f", currentFullBalance)
                                    + "\r\n   grow % :    " + String.format("%.2f", grow));
                            log.info("\r\n" + dealDetailedText);
                            setPnlPrintTime(i);
                        }

                        if (grow > CAP)
                        {
                            log.info("Current grow % is " + grow + " (> " + CAP + "). Closing all deals..");
                            log.info("PNL: " + totalPNL);
                            log.info("Signal: " + trade.symbol + " price " + trade.price + " futures: " + futures);
                            closeAllDeals();
                        } else
                        if (grow < -1 * CAL)
                        {
                            log.info("Current loss % (-grow) is " + grow + " (< " + (-CAL) + "). Closing all deals..");
                            log.info("PNL: " + totalPNL);
                            log.info("Signal: " + trade.symbol + " price " + trade.price + " futures: " + futures);
                            closeAllDeals();
                        }
                    }
                }
            }


            // надо запомнить прошеджий объем чтобы сравнить его с уменьшением уровня 
            //  - чтобы понять что уровень реально разъеден, а не просто убрали объем
            /*
            String key = futures ? "F_" + trade.symbol : "S_" + trade.symbol;            
            synchronized (latestVolumesBySumbol) {
                List<BTrade> latestTrades = latestVolumesBySumbol.get(key);
                if (latestTrades == null) {
                    latestTrades = new ArrayList<>();
                    latestVolumesBySumbol.put(key, latestTrades);
                }
                
                long curTime = System.currentTimeMillis();
                
                boolean replaced = false;
                for (int i = 0; i < latestTrades.size(); i++) {
                    BTrade t = latestTrades.get(i);
                    if (curTime - t.createTime > 600_000L) {
                        t.price = trade.price;
                        t.isBuyerMarketMaker = trade.isBuyerMarketMaker;
                        t.createTime = trade.createTime;
                        t.time = trade.time;
                        t.qty = trade.qty;
                        replaced = true;
                        break;
                    }
                }
                if (!replaced) {
                    latestTrades.add(trade);
                }
            }*/
        } catch (Throwable t) {
            log.info("ERROR! ", t);
        }
    }

    /**  */
    private boolean checkIfProboyOldOtskokLevel(BTrade trade)
    {
        DoubleLongPair lastOtskokUp = getLastOtskok(trade.symbol, true);
        DoubleLongPair lastOtskokDown = getLastOtskok(trade.symbol, false);

        if (lastOtskokUp != null && trade.price > lastOtskokUp.d)
        {
            removeLastOtskok(trade.symbol, true);
            log.info("~~ Proboy old otskok level UP: " + trade.symbol + " power: " + lastOtskokUp.l + " price: " + lastOtskokUp.d);
            makeProboyTrade(trade.symbol, true, lastOtskokUp.d, lastOtskokUp.l.intValue(), 0, true,true, false, true, 0);
            return true;
        } else if (lastOtskokDown != null && trade.price < lastOtskokDown.d)
        {
            removeLastOtskok(trade.symbol, false);
            log.info("~~ Proboy old otskok level DOWN: " + trade.symbol + " power: " + lastOtskokDown.l + " price: " + lastOtskokDown.d);
            makeProboyTrade(trade.symbol, false, lastOtskokDown.d, lastOtskokDown.l.intValue(), 0, true,true, false, true, 0);
            return true;
        }
        return false;
    }

    public boolean autoChangeAmount = true;
    public double oneDealLeverage = 0.1;

    private void saveTradeState(String eventName)
    {
        tradingStateStorageService.saveStateByEvent(this, eventName);
    }
    
    /**  */
    public synchronized void updateBalance(String symbol, double balance)
    {
        if (FIXED_BALANCE && dealPacks.get(0).deals.size() > 0)
        {
            log.info("Received new Balance = "+ String.format("%.1f", balance) + ", but skip change. We trade FIX BALANCE = "
                + String.format("%.1f", BALANCE));
            return;
        }

        try {
            if (!"USDT".equals(symbol) && !"BUSD".equals(symbol)) {
                log.info("Skip params update as this is not tradable coin");
                return;
            }
            BALANCE = balance;
            double newTradeAmount = balance * oneDealLeverage;
            log.info(">>>>>>>> Changing one trade Amount from "
                    + String.format("%.1f", TRADE_AMOUNT_USD) + " to "
                    + String.format("%.1f", newTradeAmount));
            TRADE_AMOUNT_USD = newTradeAmount;
            saveTradeState("updateBalance");
        } catch (Throwable t)
        {
            log.info("ERROR! ", t);
        }
    }
    
    private Double MAX_DEVIATION_PER = 5.0;
    
    Map<String, SFDeltaProps> sfDeltaPropsBySymbol = new HashMap<>();
    
    private SFDeltaProps getSFDeltaProps(String symbol)
    {
        SFDeltaProps sfd = sfDeltaPropsBySymbol.get( symbol );
        if (sfd == null)
        {
            sfd = new SFDeltaProps( symbol );
            sfDeltaPropsBySymbol.put(symbol, sfd);
        }
        return sfd;
    }
    
    /**
     *
     */
    private void processDeltasStat(SFDeltaProps deltaProps, double delta) {

        Stat _stat = new Stat( delta );
        deltaProps.statList.add(_stat);

        if (System.currentTimeMillis() - deltaProps.lastStatPrintedTime > 20_000L) 
        {
            /*
            Раз в 20 сек мы
            1. Пересчитываем максимумы и диапазон
            2. Удаляем стату ранее 15 минут назад
            */
            
            deltaProps.lastStatPrintedTime = System.currentTimeMillis();
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            
            double prevValue = 0;
            double prevPrevValue = 0;
            
            // List<Double> localMaxs = new ArrayList<>();
            // List<Double> localMins = new ArrayList<>();
            
            for (Stat stat : deltaProps.statList) 
            {
                if (min > stat.priceDelta)
                    min = stat.priceDelta;
                if (max < stat.priceDelta)
                    max = stat.priceDelta;
                /*
                if (prevValue > 0 && prevPrevValue > 0)
                {
                    if (prevPrevValue < prevValue && stat.priceDelta < prevValue
                            && prevValue > deltaProps.lastMax2 - deltaProps.deltaPerDeviation2/3.0)
                        localMaxs.add(new Double(prevValue));
                    else if (prevPrevValue > prevValue && stat.priceDelta > prevValue
                            && prevValue < deltaProps.lastMin2 + deltaProps.deltaPerDeviation2/3.0)
                        localMins.add(new Double(prevValue));
                } */
                        
                prevPrevValue = prevValue;
                prevValue = stat.priceDelta;
            }
            
            deltaProps.lastMin = min;
            deltaProps.lastMax = max;
            
            /*
            double maxSum = 0;
            double minSum = 0;
            for (Double v : localMaxs)
            {
                maxSum += v;
            }
            for (Double v : localMins)
            {
                minSum += v;
            }
                
            if (localMins.size() > 0)
                deltaProps.lastMin2 = minSum / (double)localMins.size();
            else
                deltaProps.lastMin2 = min;
            
            if (localMaxs.size() > 0)
                deltaProps.lastMax2 = maxSum / (double)localMaxs.size();
            else
                deltaProps.lastMax2 = max; 
            deltaProps.deltaPerDeviation2 = (deltaProps.lastMax2 - deltaProps.lastMin2) / 100.0 * MAX_DEVIATION_PER; // 5%
            */
            
            deltaProps.deltaPerDeviation = (max - min) / 100.0 * MAX_DEVIATION_PER; // 5%

            // clean Hist
            while (deltaProps.statList.size() > 0) {
                Stat s = deltaProps.statList.get(0);
                if (s.time < System.currentTimeMillis() - 15 * 60_000L)
                    deltaProps.statList.remove(0);
                else
                    break;
            }
        }
    }
    
    
    public boolean CREATE_TP = false;
    public boolean CREATE_SL = true;
    
    public boolean EXTRA_FILTERING = false;
    public volatile double FILTER_KOOF = 1.0005;
    
    public synchronized double getFilterKoof()
    {
        return FILTER_KOOF;
    }
    public synchronized void setFilterKoof(double v)
    {
        FILTER_KOOF = v;
    }
    
    //public double PRICE_GAP_DELIM = 4000.0;
    
    // режим когда можно докупать и допродавать по кусочкам.
    public boolean NAKOPLENIE = false;
    public boolean LIMIT_TRADES = false;
    private volatile int SIG_CNT = 1;
    private volatile int SIG_CNT2 = 2;
    private volatile int SIG_CNT3 = 1;
    private volatile int SIG_CNT4 = 1;

    
    // Высчитывается тестилкой. Чтобы отключить можно поставить значение -10
    public int EXTREME_DELTA_FILTER = 14;            // Высчитывается тестилкой
    public int CLOSE_DELTA_FILTER = 2;

    
    public double deltaFilter = (double) -EXTREME_DELTA_FILTER * 10.0;
    public double deltaFilterClose = (double) -CLOSE_DELTA_FILTER * 10.0;
    
    private Map<String, Long> lastSFDealTimeBySymbol = new HashMap<>();
    private void rememberSFSignalTime(String symbol)
    {
        lastSFDealTimeBySymbol.put(symbol, new Long(System.currentTimeMillis()));
    }
    private boolean isSFSignalOnFreeze(String symbol, int freezeDelaySec)
    {
        Long lastSignalTime = lastSFDealTimeBySymbol.get(symbol);
        if (lastSignalTime == null || System.currentTimeMillis() - lastSignalTime >= (long)freezeDelaySec * 1000L)
            return false;
        return true;
    }
    
    Map<String, String> lastSigBuSymbol = new HashMap<>();
    private boolean wasSameSig(String symbol, String signatura)
    {
        String sgn = lastSigBuSymbol.get( symbol );
        if (signatura.equals( sgn ))
            return true;
        lastSigBuSymbol.put(symbol, signatura);
        return false;
    }
    
    Map<String, MA> volsBySymbol = new HashMap<>();
    private MA storeVols(BTrade t)
    {
        MA ma = getVols(t.symbol);
        ma.addStat(new Stat(t.qty));
        return ma;
    }
    private MA getVols(String symbol)
    {
        MA ma = volsBySymbol.get(symbol);
        if (ma == null)
        {
            ma = new MA(60L * 60_000L); 
            volsBySymbol.put(symbol, ma);
        }
        return ma;
    }
    
    
    Map<String, Double> minBySymbol = new HashMap<>();
    Map<String, Double> maxBySymbol = new HashMap<>();
    private void soreMinMax(BTrade trade)
    {
        Double min = minBySymbol.get(trade.symbol);
        if (min == null || trade.price < min)
            minBySymbol.put(trade.symbol, trade.price);
        Double max = maxBySymbol.get(trade.symbol);
        if (max == null || trade.price > max)
            maxBySymbol.put(trade.symbol, trade.price);
    }
    
    private double getAndResetMinPrice(SFDeltaProps dp)
    {
        Double min = minBySymbol.get(dp.symbol);
        if (min == null)
            return dp.lastFutTrade.price;
        minBySymbol.remove(dp.symbol);
        return min;
    }
    private double getAndResetMaxPrice(SFDeltaProps dp)
    {
        Double max = maxBySymbol.get(dp.symbol);
        if (max == null)
            return dp.lastFutTrade.price;
        maxBySymbol.remove(dp.symbol);
        return max;
    }
    
    Map<String, Boolean> trendBySymbol = new HashMap<>();
    private void setTrend(String symbol, boolean up)
    {
        trendBySymbol.put(symbol, up);
    }
    private boolean isTrendBuy(String symbol)
    {
        Boolean t =  trendBySymbol.get(symbol);
        if (t != null && t)
            return true;
        return false;
    }
    private boolean isTrendSell(String symbol)
    {
        Boolean t =  trendBySymbol.get(symbol);
         if (t != null && !t)
             return true;
         return false;
    }
    
    private Map<String, SignalCounter> sigCounterBySymbol = new HashMap<>();
    private Map<String, SignalCounter> sigCounterBySymbol2 = new HashMap<>();
    private Map<String, SignalCounter> sigCounterBySymbol3 = new HashMap<>();
    private Map<String, SignalCounter> sigCounterBySymbol4 = new HashMap<>();
    private SignalCounter getSignalCounter(String symbol, int num)
    {
        Map<String, SignalCounter> scMap = sigCounterBySymbol;
        if (num == 2)
            scMap = sigCounterBySymbol2;
        else if (num == 3)
            scMap = sigCounterBySymbol3;
        else if (num == 4)
            scMap = sigCounterBySymbol4;
        
        SignalCounter sc = scMap.get( symbol );
        if (sc == null)
        {
            sc = new SignalCounter();
            scMap.put(symbol, sc);
        }
        return sc;
    }
    
    Map<String, SignalPair> signalPairByCoin = new HashMap<>();
    /**  */
    private SignalPair getSignalPair(String symbol)
    {
        String coin = symbol.substring(0, symbol.length() - 4);
        if (coin.startsWith("H-") || coin.startsWith("B-"))
            coin = coin.substring(2);
        SignalPair sPair = signalPairByCoin.get( coin );
        if (sPair == null)
        {
            sPair = new SignalPair(symbol);
            signalPairByCoin.put(coin, sPair);
        }
        return sPair;
    }
    
    /**  */
    private int getTotalSignalDirected(boolean up)
    {
        int cnt = 0;
        for (SignalPair sp : signalPairByCoin.values())
        {
            if (!sp.busdSignal.symbol.startsWith("BTC") && !sp.busdSignal.symbol.startsWith("ETH"))
                continue;
            
            try {
                if (sp.busdSignal.up == up)
                    cnt++;
                if (sp.usdtSignal.up == up)
                    cnt++;
            } catch (Exception e) {}
        }
        return cnt;
    }
    
    long BTC_BUSD_SIG_DELAY = 600_000;
    
    private long getMaxDelta(long x, long y, long z)
    {
        return Math.max(Math.max(x, y), z) - Math.min(Math.min(x, y), z);
    }
    private long getMaxDelta(long x, long y, long z, long zz)
    {
        return Math.max(Math.max(x, y), Math.max(z, zz)) - Math.min(Math.min(x, y), Math.min(z, zz));
    }

    /**  */
    public synchronized int getSIG_CNT() {
        return SIG_CNT;
    }
    /**  */
    public synchronized int getSIG_CNT2() {
        return SIG_CNT2;
    }
    /**  */
    public synchronized int getSIG_CNT3() {
        return SIG_CNT3;
    }

    /**  */
    public synchronized void setSIG_CNT(int SIG_CNT) {
        this.SIG_CNT = SIG_CNT;
    }
    /**  */
    public synchronized void setSIG_CNT2(int SIG_CNT2) {
        this.SIG_CNT2 = SIG_CNT2;
    }
    /**  */
    public synchronized void setSIG_CNT3(int SIG_CNT3) {
        this.SIG_CNT3 = SIG_CNT3;
    }
    
    /**  */
    private synchronized LevelProps getLevelProps(String symbol, double price)
    {
        LevelProps lp = new LevelProps();
        try {
            SymbolInfo sInfo = symbolInfos.get(symbol);
            CandlePackage candles = this.candlesBySymbol.get( symbol );
            LevelAnalizDecision decision = candles.isLevelGood(true, price, sInfo.comissionExample * sInfo.volatileKoof, 5);
            lp.upLevelBars = decision.barsTillCross;
            lp.upLevelKosanie = decision.kasanieCnt;
            
            decision = candles.isLevelGood(false, price, sInfo.comissionExample * sInfo.volatileKoof, 5);
            lp.downLevelBars = decision.barsTillCross;
            lp.downLevelKosanie = decision.kasanieCnt;
        } catch (Exception e)
        {
        }
        return lp;
    }
    
    private Set<Signal> sfSignals = new HashSet<>();
    
    /**  */
    private void rememberSignal(String symbol, boolean up, double price, double deltaPer)
    {
        Signal s = new Signal(symbol, up, price, deltaPer);
        sfSignals.remove( s );
        sfSignals.add( s );
    }
    
    /**  */
    private IntPair getSFSignalQty(long timeFrame)
    {
        IntPair dp = new IntPair();
        long curTime = System.currentTimeMillis();
        for (Signal s : sfSignals)
        {
            if (s.time < curTime - timeFrame)
                continue;
            if (s.up)
                dp.buy++;
            else
                dp.sell++;
        }
        return dp;
    }
    
    /** вернет все сигналы по стратегии разницы цены спота и фьючерса - за последние ХХ сек */
    private Set<Signal> getLastNSecSFSignals(long timeFrame, boolean buy)
    {
        Set<Signal> rv = new HashSet<>();
        long curTime = System.currentTimeMillis();
        for (Signal s : sfSignals)
        {
            if (s.time < curTime - timeFrame)
                continue;
            if (s.up == buy)
                rv.add( s );
        }
        return rv;
    }
    
    /**  */
    public synchronized void closeAllDeals()
    {
        Set<Deal> deals2close = new HashSet<>();
        for (DealPack dp : dealPacks) {
            for (Deal d : dp.deals) {
                deals2close.add(d);
            }
        }
        for (Deal d : deals2close)
            closeDeal(d, false);
    }
    
    /**  */
    public synchronized void closeDealPack(int i) {
        DealPack dp = dealPacks.get(i);
        for (Deal d : dp.deals) {
            closeDeal(d, false);
        }
    }
    
    /**  */
    public synchronized int getDealCnt(int packNum)
    {
        DealPack dp = dealPacks.get(packNum);
        return dp.deals.size();
    }
    
    public synchronized int getDealPackCnt()
    {
        return dealPacks.size();
    }
    

    /**  */
    public LatestVolumes getLatestVolumes(String symbol, boolean futures)
    {
        LatestVolumes volumes = !futures ? volumesBySymbol.get( symbol ) : volumesBySymbolFutures.get( symbol );
        if (volumes == null)
        {
            volumes = new LatestVolumes(60_000L);
            if (!futures)
                volumesBySymbol.put(symbol, volumes);            
            else
                volumesBySymbolFutures.put(symbol, volumes);
        }
        return volumes;
    }
    
    Map<String, CandlePackage> candlesBySymbol = new ConcurrentHashMap<>();
    
    
    long lastCandleInfoPrinted = 0;
    int candlUpdtCnt=0;
    int prevSig = 0;
    int sigCount = 0;
    /**  */

    Map<String, Double> lastPriceBySymb = new HashMap<>();

    /**  */
    private void addCandle(DeltaProcessor deltaProcessor, StreamCandle candle, boolean futures)
    {
        Integer sig = null;
        if (futures)
        {
            lastPriceBySymb.put(candle.symbol, candle.c);
        }
        if (deltaProcessor.addCandle(futures, candle) != null)
            sig = deltaProcessor.getSignal();
        if (sig == null) { // Сигнал еще не готов
        } else if (sig == 1) {
            log.info(" ======== BUY SIG " + deltaProcessor.baseCcy);
            createMarketOrder(deltaProcessor.baseCcy, BinOrder.SIDE_BUY,
                    0, lastPriceBySymb.get(candle.symbol), deltaProcessor.TP_COMMS, deltaProcessor.SL_COMMS,
                    deltaProcessor.NO_LOSS_COMMS, deltaProcessor.tpCnt, deltaProcessor.tpCnt, deltaProcessor.MULT / MULT_DECREMENTER,
                    deltaProcessor.useTP, deltaProcessor.useSL, false, false, true);

            if (prevSig == sig)
                sigCount++;
            else
                sigCount = 1;
            prevSig = sig;
        } else if (sig == -1) {
            log.info(" ======== SELL SIG " + deltaProcessor.baseCcy);
            createMarketOrder(deltaProcessor.baseCcy, BinOrder.SIDE_SELL,
                    0, lastPriceBySymb.get(candle.symbol), deltaProcessor.TP_COMMS, deltaProcessor.SL_COMMS,
                    deltaProcessor.NO_LOSS_COMMS, deltaProcessor.tpCnt, deltaProcessor.tpCnt,deltaProcessor.MULT / MULT_DECREMENTER,
                    deltaProcessor.useTP, deltaProcessor.useSL, false, false, true);

            if (prevSig == sig)
                sigCount++;
            else
                sigCount = 1;
            prevSig = sig;
        } else if (sig == 2) {
            log.info(" ======== CLOSE SELL SIG " + deltaProcessor.baseCcy);
            closeDealIfDirection(deltaProcessor.baseCcy, false);
        } else if (sig == -2) {
            log.info(" ======== CLOSE BUY SIG " + deltaProcessor.baseCcy);
            closeDealIfDirection(deltaProcessor.baseCcy, true);
        }
    }

    /**  */
    public synchronized void updateBinanceCandles(StreamCandle candle, boolean futures)
    {
        if (!futures || !candle.closed)
        {
            return;
        }

        CandlePackage candles = candlesBySymbol.get(candle.symbol);
        if (candles == null) {
            log.info("WARNING: No candle package for " + candle.symbol);
            return;
        }

        log.info("update kline for " + candle.symbol);
        candles.addCandle(candle, false);
    }


    Map<String, Bstakan> buyBitStakasBySymbol = new HashMap<>();
    JsonParser bbParser = new JsonParser();

    /**  Получить доступный обьем сделки в рамках уровня прибыли minDelta (дельта цены) */
    private double getPossibleAmount(Bstakan buy /* где покупаем */, Bstakan sell /* где продаем */, double minDelta)
    {
        // покупаем по цене из bibs
        // продаем  по цене из asks
        // цены BID < цен ASK.               ASK - это по чем продают (я могу купить).   BID - по чем покупают (я могу продать)
        // BID'ы от большего к меньшему
        // ASK'и от меньшего к большему  - самое выгодное вначале

        double possibleAmount = 0;

        double buyOstatokQty = 0;
        double sellOstatokQty = 0;
        double buyOstatokPrice = 0;
        double sellOstatokPrice = 0;

        boolean needDecrementBuyWithOstatok = true;
        boolean needDecrementSellWithOstatok = true;

        int i = 0;
        int j = 0;

        for (; i < buy.asks.size(); i++)
        {
            double buyQty = buyOstatokQty;
            double buyPrice = buyOstatokPrice;

            if (buyQty > 0)
            {
                if (needDecrementBuyWithOstatok)
                    i--;
                needDecrementBuyWithOstatok = false;
            } else {
                PriceQty buyPQ = buy.asks.get(i);
                buyQty = buyPQ.qty;
                buyPrice = buyPQ.price;
                needDecrementBuyWithOstatok = true;
            }

            for (; j < sell.bids.size(); j++)
            {

                double sellQty = sellOstatokQty;
                double sellPrice = sellOstatokPrice;

                if (sellQty > 0)
                {
                    if (needDecrementSellWithOstatok)
                        j--;
                    needDecrementSellWithOstatok = false;
                } else {
                    PriceQty sellPQ = sell.bids.get(j);
                    sellQty = sellPQ.qty;
                    sellPrice = sellPQ.price;
                    needDecrementSellWithOstatok = true;
                }

                double thisDelta = sellPrice - buyPrice;

                if (thisDelta >= minDelta)
                {
                    // профитная разница в цене - можно учесть этот объем
                    if (buyQty >= sellQty)
                    {  // на покупку обьема больше - скушаем эту всю продаждую запись и перейдем к следующей + оставим остаток у покупной стороны
                        possibleAmount += sellQty;
                        sellOstatokQty = 0;
                        buyOstatokQty = buyQty - sellQty;
                        buyOstatokPrice = buyPrice;
                        continue; // перемотка sell : j++
                    } else {
                        possibleAmount += buyQty;
                        sellOstatokQty = sellQty - buyQty;
                        sellOstatokPrice = sellPrice;
                        buyOstatokQty = 0;
                        break; // перемотка buy : i++
                    }
                } else {
                    // цена не подходит - Больше нечего складывать
                    //log.info(" i = " + i + "    j = " + j);
                    //log.info("Where BUY (asks):");
                    //printArray(buy.asks);
                    //log.info("Where SELL (bids):");
                    //printArray(sell.bids);
                    return possibleAmount;
                }
            }
        }


        return possibleAmount;
    }

    /**  */
    private void printArray(List<PriceQty> l)
    {
        for (int i=0; i < l.size(); i++)
        {
            PriceQty pq = l.get(i);
            log.info(i + ": p " + pq.price + " q " + pq.qty);
        }
    }

    /**  */
    private void checkStakanDeltas(String symbol)
    {

        long LAST_UPDATE_DISTANCE = 5_000;

        StakanContainer sContainer = bstakanBySymbolFutures.get( symbol );
        if (sContainer == null)
            return;
        Bstakan st1 = sContainer.bstakan;
        Bstakan st2 = buyBitStakasBySymbol.get( symbol );
        if (st2 == null)
            return;

        if (System.currentTimeMillis() - st1.updateTime > LAST_UPDATE_DISTANCE
            || System.currentTimeMillis() - st2.updateTime > LAST_UPDATE_DISTANCE)
            return;

        double bestSell1 = st1.getBestBuy().price; // я могу продать!
        double bestBuy1 = st1.getBestSell().price; // я могу купить!
        double bestSell2 = st2.getBestBuy().price; // я могу продать!
        double bestBuy2 = st2.getBestSell().price; // я могу купить!

        double buy1Sell2 = bestSell2 - bestBuy1;
        double buy2Sell1 = bestSell1 - bestBuy2;

        double MIN_DELTA_PERCENT = 0.003;
        double MIN_DELTA_PRICE = bestBuy1 * MIN_DELTA_PERCENT;

        if (buy1Sell2 > MIN_DELTA_PRICE)
        {
            double per = buy1Sell2 / bestBuy1 * 100.0;
            double qty = getPossibleAmount(st1, st2, bestBuy1 * MIN_DELTA_PERCENT);
            double qtyUSD = qty * bestBuy1;
            log.info("  DEAL " + String.format("%.2f", per) + " % " + symbol + " buy Binance " + bestBuy1 + " sell BuyBit " + bestSell2 +
                    " qty " + qty + "    qtyUSD " + qtyUSD);
            return;
        }

        if (buy2Sell1 > MIN_DELTA_PRICE)
        {
            double per = buy2Sell1 / bestBuy1 * 100.0;
            double qty = getPossibleAmount(st2, st1, bestBuy1 * MIN_DELTA_PERCENT);
            double qtyUSD = qty * bestBuy1;
            log.info("# DEAL " + String.format("%.2f", per) + " % " + symbol + " buy BuyBit " + bestBuy2 + " sell Binance " + bestSell1 +
                    " qty " + qty + "    qtyUSD " + qtyUSD);
            return;
        }
    }

    /**  */
    public synchronized void updateBuyBitStakan(String packet)
    {
        try {
            JsonObject obj = bbParser.parse( packet ).getAsJsonObject();
            String topic = obj.get("topic").getAsString();
            String type = obj.get("type").getAsString();
            JsonObject data = obj.get("data").getAsJsonObject();
            String symbol = data.get("s").getAsString();
            JsonArray bids = data.get("b").getAsJsonArray(); // Заявки на покупку - от более дорогих (актуальных) к более дешевым
            JsonArray asks = data.get("a").getAsJsonArray(); // Заявки на продажу - от более дешевых (актуальных) к более дорогим
            if ("snapshot".equals(type))
            {
                Bstakan bstakan = new Bstakan();
                for (int i= 0; i < bids.size(); i++)
                {
                    JsonArray record = bids.get(i).getAsJsonArray();
                    Double price = new Double(record.get(0).getAsString());
                    Double qty = new Double(record.get(1).getAsString());
                    PriceQty pq = new PriceQty( price, qty );
                    bstakan.bids.add( pq );
                    bstakan.bidsMap.put(pq.price, pq);
                }
                for (int i= 0; i < asks.size(); i++)
                {
                    JsonArray record = asks.get(i).getAsJsonArray();
                    Double price = new Double(record.get(0).getAsString());
                    Double qty = new Double(record.get(1).getAsString());
                    PriceQty pq = new PriceQty( price, qty );
                    bstakan.asks.add( pq );
                    bstakan.asksMap.put(pq.price, pq);
                }
                buyBitStakasBySymbol.put(symbol, bstakan);
                bstakan.updateTime = System.currentTimeMillis();
            } else if ("delta".equals(type))
            {
                Bstakan bstakan = buyBitStakasBySymbol.get(symbol);
                if (bstakan == null)
                {
                    log.info("ALARM! got 'delta' message for " + symbol + ", but no cached data exists!");
                    return;
                }
                for (int i= 0; i < bids.size(); i++)
                {
                    JsonArray record = bids.get(i).getAsJsonArray();
                    Double price = new Double(record.get(0).getAsString());
                    Double qty = new Double(record.get(1).getAsString());
                    PriceQty pq = new PriceQty( price, qty );

                    PriceQty oldPQ = bstakan.bidsMap.get( pq.price );
                    if (qty <= 0)
                    {
                        if (oldPQ != null)
                        {
                            bstakan.deleteLine(true, pq);
                        }
                    } else {
                        if (oldPQ != null)
                            oldPQ.qty = qty;
                        else
                            bstakan.addStakanRecord(true, pq);
                    }
                }
                for (int i= 0; i < asks.size(); i++)
                {
                    JsonArray record = asks.get(i).getAsJsonArray();
                    Double price = new Double(record.get(0).getAsString());
                    Double qty = new Double(record.get(1).getAsString());
                    PriceQty pq = new PriceQty( price, qty );
                    PriceQty oldPQ = bstakan.asksMap.get( pq.price );
                    if (qty <= 0)
                    {
                        if (oldPQ != null)
                        {
                            bstakan.deleteLine(false, pq);
                        }
                    } else {
                        if (oldPQ != null)
                            oldPQ.qty = qty;
                        else
                            bstakan.addStakanRecord(false, pq);
                    }
                }
                bstakan.updateTime = System.currentTimeMillis();
            } else {
                log.info("WARNING: urecognized type of message " + packet);
            }
            checkTopLevelBB(symbol);
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    final int SIGNAL_KOOF = 15;
    final boolean checkPriceChange = true;

    // ByBit :::

    Map<String, MMAT> _avgStakanVolBySymbolBB = new HashMap<>();
    Map<String, VolumeAtLevel> _symbolLevelsBB = new HashMap<>();
    Map<String, Double> lastBBuyPriceBySymbolBB = new HashMap<>();
    Map<String, Double> lastBSellPriceBySymbolBB = new HashMap<>();
    // Если TRUE то статистика по стакану добавится только при смене цены

    /**  */
    private void checkTopLevelBB(String symbol)
    {
        Bstakan st1 = buyBitStakasBySymbol.get(symbol);

        PriceQty bestBuy = st1.getBestBuy(); // Цены покупки - по чем хотят купить - они Меньше!        Сорт от бОльшего к меньшиму     - т.е. 1-я самая актуальная к рынку!
        PriceQty bestSell = st1.getBestSell(); // Цены продажи - за которые хотят продать - они бОльше!   Сорт от меньшего к большему     - т.е. 1-я самая актуальная к рынку!

        int MMA_SIZE = 5000;

        Map<String, MMAT> avgStakanVolBySymbol = _avgStakanVolBySymbolBB;
        Map<String, VolumeAtLevel> symbolLevels = _symbolLevelsBB;

        MMAT avgTopStakanVol = avgStakanVolBySymbol.get( symbol );
        if (avgTopStakanVol == null)
        {
            avgTopStakanVol = new MMAT( MMA_SIZE, STAKAN_STAT_TIMEFRAME );
            avgStakanVolBySymbol.put(symbol, avgTopStakanVol);
        }
        long curTime = System.currentTimeMillis();

        if (!checkPriceChange) {
            avgTopStakanVol.addStat(bestSell.qty, curTime);
            avgTopStakanVol.addStat(bestBuy.qty, curTime);
        } else
        { // добавляем стату только при смене цены!
            Double prevBestBuyPrice = lastBBuyPriceBySymbolBB.get(symbol);
            if (prevBestBuyPrice == null || !prevBestBuyPrice.equals(bestBuy.price))
            {
                avgTopStakanVol.addStat(bestBuy.qty, curTime);
                //avgTopStakanVol.addStat(bestBuy.qty);
                lastBBuyPriceBySymbolBB.put(symbol, bestBuy.price);
            }
            Double prevBestSellPrice = lastBSellPriceBySymbolBB.get(symbol);
            if (prevBestSellPrice == null || !prevBestSellPrice.equals(bestSell.price))
            {
                avgTopStakanVol.addStat(bestSell.qty, curTime);
                lastBSellPriceBySymbolBB.put(symbol, bestBuy.price);
            }
        }
        VolumeAtLevel vat = symbolLevels.get(symbol);
        if (avgTopStakanVol.stat.size() >= MMA_SIZE)
        {
            if (bestSell.qty > avgTopStakanVol.getAvg() * (double) SIGNAL_KOOF)
            {
                MinMax minMax = getMinMaxPrices(symbol, bestSell.price);

                double power = bestSell.qty / avgTopStakanVol.getAvg();
            //    log.info("bb   " + symbol + " UP   LEVEL " + String.format("%.6f", bestSell.price) + "  (level x " + (int)power
              //          + " min " + String.format("%.6f", minMax.min) + " max " + String.format("%.6f", minMax.max));

                vat = addVolumeAtPrice(true, symbol, vat, bestSell.price, power, symbolLevels);
            }
            if (bestBuy.qty > avgTopStakanVol.getAvg() * (double) SIGNAL_KOOF)
            {
                MinMax minMax = getMinMaxPrices(symbol, bestBuy.price);

                double power = bestBuy.qty / avgTopStakanVol.getAvg();
              //  log.info("bb   " + symbol + " DOWN LEVEL " + String.format("%.6f", bestBuy.price) + "  (level x " + (int)power
                //        + " min " + String.format("%.6f", minMax.min) + " max " + String.format("%.6f", minMax.max));

                vat = addVolumeAtPrice(false, symbol, vat, bestBuy.price, power, symbolLevels);
            }
        }

        //////////////////////////////////////////////
        // Если был VAT - проверим - А) сместилась ли цена Б) упал ли power критически
        if (vat != null)
        { // уровень был!
            if (vat.up)
            {  // UP level
                if (vat.price > bestSell.price) {
                    // отскок от верхнего уровня вниз
                    log.info("bbO <- " + symbol + " UP   LEVEL " + String.format("%.6f", vat.price) + "  (level x " + (int)vat.startPower + " " + vat.getLifeTime() + "ms");
                    symbolLevels.remove(symbol);
                    makeProboyTrade(symbol, true, vat.price, (int)vat.startPower,
                            new Long(vat.getLifeTime()).intValue(), false, true, true, false, 0);
                } else if (vat.price < bestSell.price) {
                    // ПРОБОЙ верхнего уровня вверх
                    log.info("bbP -> " + symbol + " UP   LEVEL " + String.format("%.6f", vat.price) + "  (level x " + (int)vat.startPower + " " + vat.getLifeTime() + "ms");
                    symbolLevels.remove(symbol);
                    makeProboyTrade(symbol, true, vat.price, (int)vat.startPower,
                            new Long(vat.getLifeTime()).intValue(), true, true, true, false, 0);
                } else {
                    // мы еще на уровне... можно проверить сильно ли упал power
                }
            } else
            {  // DOWN level
                if (vat.price < bestBuy.price) {
                    // отскок от нижнего уровня вверх
                    log.info("bbO <- " + symbol + " DOWN LEVEL " + String.format("%.6f", vat.price) + "  (level x " + (int)vat.startPower + " " + vat.getLifeTime() + "ms");
                    symbolLevels.remove(symbol);
                    makeProboyTrade(symbol, false, vat.price, (int)vat.startPower,
                            new Long(vat.getLifeTime()).intValue(), false,true, true, false, 0);
                } else if (vat.price > bestBuy.price) {
                    // ПРОБОЙ нижнего уровня вниз
                    log.info("bbP -> " + symbol + " DOWN LEVEL " + String.format("%.6f", vat.price) + "  (level x " + (int)vat.startPower + " " + vat.getLifeTime() + "ms");
                    symbolLevels.remove(symbol);
                    makeProboyTrade(symbol, false, vat.price, (int)vat.startPower,
                            new Long(vat.getLifeTime()).intValue(), true,true, true, false, 0);
                } else {
                    // мы еще на уровне... можно проверить сильно ли упал power
                }
            }
        }
    }


    // Binance :::

    Map<String, MMAT> _avgStakanVolBySymbol = new HashMap<>();
    Map<String, MMAT> _avgStakanVolBySymbolS = new HashMap<>();

    Map<String, VolumeAtLevel> _symbolLevels = new HashMap<>();
    Map<String, VolumeAtLevel> _symbolLevelsS = new HashMap<>();

    Map<String, Double> lastBBuyPriceBySymbol = new HashMap<>();
    Map<String, Double> lastBSellPriceBySymbol = new HashMap<>();
    // Если TRUE то статистика по стакану добавится только при смене цены

    long GLOBAL_SIG_ID = 0;

    private String getExtraString(String symbol, double price)
    {
        String extraString = " id " + GLOBAL_SIG_ID;
        Double agressiveBuyVol = getAgressiveBuyVol(symbol);
        Double agressiveSellVol = getAgressiveSellVol(symbol);
        extraString = extraString + " AggB " + Double.valueOf(agressiveBuyVol * price).intValue();
        extraString = extraString + " AggS " + Double.valueOf(agressiveSellVol * price).intValue();
        double priceMove = getPriceMove(symbol, price) * 1000.0 / price;
        extraString = extraString + " PM " + String.format("%.2f", priceMove);
        return extraString;
    }

    private Map<String, Deque<EventMark>> eventTriggersBySymbol = new HashMap<>();
    private synchronized void addEventTrigger(String symbol, long eventId, long triggerTime)
    {
        Deque<EventMark> triggers = eventTriggersBySymbol.get(symbol);
        if (triggers == null)
        {
            triggers = new ArrayDeque(100);
            eventTriggersBySymbol.put(symbol, triggers);
        }
        triggers.add(new EventMark(eventId, triggerTime));
    }
    /**  */
    private static final List<Long> EMPTY_TRIGGER_IDS = Collections.emptyList();

    private synchronized List<Long> getTriggeredEventIdsForSymbol(String symbol)
    {
        Deque<EventMark> triggers = eventTriggersBySymbol.get(symbol);
        if (triggers == null)
            return EMPTY_TRIGGER_IDS;
        List<Long> rv = null;
        long curTime = System.currentTimeMillis();
        while (!triggers.isEmpty()) {
            EventMark first = triggers.peekFirst();
            if (first.triggerTime <= curTime)
            {
                if (rv == null) {
                    rv = new ArrayList<>(4);
                }
                rv.add(first.eventId);
                triggers.removeFirst(); // O(1)
            } else
                break;
        }
        if (rv == null) {
            return EMPTY_TRIGGER_IDS;
        }
        return rv;
    }

    /**  */
    private void checkTopLevel(String symbol, boolean fututres)
    {
        // Bstakan bstakan = buyBitStakasBySymbol.get(symbol);

        StakanContainer sContainer = null;
        if (fututres)
            sContainer = bstakanBySymbolFutures.get( symbol );
        else
            sContainer = bstakanBySymbol.get( symbol );
        if (sContainer == null)
            return;
        Bstakan st1 = sContainer.bstakan;

        PriceQty bestBuy = st1.getBestBuy(); // Цены покупки - по чем хотят купить - они Меньше!        Сорт от бОльшего к меньшиму     - т.е. 1-я самая актуальная к рынку!
        PriceQty bestSell = st1.getBestSell(); // Цены продажи - за которые хотят продать - они бОльше!   Сорт от меньшего к большему     - т.е. 1-я самая актуальная к рынку!

        int MMA_SIZE = 5000;

        Map<String, MMAT> avgStakanVolBySymbol = _avgStakanVolBySymbol;
        //Map<String, MMA> avgStakanVolBySymbol = _avgStakanVolBySymbol;
        Map<String, VolumeAtLevel> symbolLevels = _symbolLevels;
        if (!fututres)
        {
            avgStakanVolBySymbol = _avgStakanVolBySymbolS;
            symbolLevels = _symbolLevelsS;
        }

        MMAT avgTopStakanVol = avgStakanVolBySymbol.get( symbol );
       // MMA avgTopStakanVol = avgStakanVolBySymbol.get( symbol );
        if (avgTopStakanVol == null)
        {
            avgTopStakanVol = new MMAT( MMA_SIZE, STAKAN_STAT_TIMEFRAME );
          //  avgTopStakanVol = new MMA( MMA_SIZE );
            avgStakanVolBySymbol.put(symbol, avgTopStakanVol);
        }
        long curTime = System.currentTimeMillis();

        if (!fututres || !checkPriceChange) {
            avgTopStakanVol.addStat(bestSell.qty, curTime);
            avgTopStakanVol.addStat(bestBuy.qty, curTime);
           // avgTopStakanVol.addStat(bestSell.qty);
           // avgTopStakanVol.addStat(bestBuy.qty);
        } else if (fututres)
        { // добавляем стату только при смене цены!
            Double prevBestBuyPrice = lastBBuyPriceBySymbol.get(symbol);
            if (prevBestBuyPrice == null || !prevBestBuyPrice.equals(bestBuy.price))
            {
                avgTopStakanVol.addStat(bestBuy.qty, curTime);
                //avgTopStakanVol.addStat(bestBuy.qty);
                lastBBuyPriceBySymbol.put(symbol, bestBuy.price);
            }
            Double prevBestSellPrice = lastBSellPriceBySymbol.get(symbol);
            if (prevBestSellPrice == null || !prevBestSellPrice.equals(bestSell.price))
            {
                avgTopStakanVol.addStat(bestSell.qty, curTime);
                //avgTopStakanVol.addStat(bestSell.qty);
                lastBSellPriceBySymbol.put(symbol, bestBuy.price);
            }
        }

        VolumeAtLevel vat = symbolLevels.get(symbol);

        if (avgTopStakanVol.stat.size() >= MMA_SIZE)
        {
            boolean logged = false;
            if (bestSell.qty > avgTopStakanVol.getAvg() * (double) SIGNAL_KOOF)
            {
                MinMax minMax = getMinMaxPrices(symbol, bestSell.price);

                double power = bestSell.qty / avgTopStakanVol.getAvg();
                log.info("     " + symbol + " UP   LEVEL " + formatPriceForSignalLog(bestSell.price) + "  (level x " + (int)power
                        + " min " + formatPriceForSignalLog(minMax.min) + " max " + formatPriceForSignalLog(minMax.max));

                vat = addVolumeAtPrice(true, symbol, vat, bestSell.price, power, symbolLevels);
            }
            if (bestBuy.qty > avgTopStakanVol.getAvg() * (double) SIGNAL_KOOF)
            {
                MinMax minMax = getMinMaxPrices(symbol, bestBuy.price);

                double power = bestBuy.qty / avgTopStakanVol.getAvg();
                log.info("     " + symbol + " DOWN LEVEL " + formatPriceForSignalLog(bestBuy.price) + "  (level x " + (int)power
                        + " min " + formatPriceForSignalLog(minMax.min) + " max " + formatPriceForSignalLog(minMax.max));

                vat = addVolumeAtPrice(false, symbol, vat, bestBuy.price, power, symbolLevels);
            }
        }

        //////////////////////////////////////////////
        // Если был VAT - проверим - А) сместилась ли цена Б) упал ли power критически
        if (vat != null)
        { // уровень был!
            if (vat.up)
            {  // UP level
                if (vat.price > bestSell.price) {
                    // отскок от верхнего уровня вниз
                    // TODO: надо для этого id сделки через какое то время выдать доп. инфомрацию в логи
                    String extraString = getExtraString(symbol, vat.price);
                    long evntId = GLOBAL_SIG_ID;
                    addEventTrigger(symbol, GLOBAL_SIG_ID, System.currentTimeMillis() + 15_000);
                    GLOBAL_SIG_ID++;

                    log.info("O <- " + symbol + " UP   LEVEL " + formatPriceForSignalLog(vat.price)
                            + "  (level x " + (int)vat.startPower + " " + vat.getLifeTime() + "ms" + extraString);
                    symbolLevels.remove(symbol);
                    makeProboyTrade(symbol, true, vat.price, (int)vat.startPower,
                            new Long(vat.getLifeTime()).intValue(), false, fututres, false, false, evntId);
                } else if (vat.price < bestSell.price) {
                    // ПРОБОЙ верхнего уровня вверх
                    // TODO: надо для этого id сделки через какое то время выдать доп. инфомрацию в логи
                    String extraString = getExtraString(symbol, vat.price);
                    long evntId = GLOBAL_SIG_ID;
                    addEventTrigger(symbol, GLOBAL_SIG_ID, System.currentTimeMillis() + 15_000);
                    GLOBAL_SIG_ID++;

                    log.info("P -> " + symbol + " UP   LEVEL " + formatPriceForSignalLog(vat.price)
                            + "  (level x " + (int)vat.startPower + " " + vat.getLifeTime() + "ms" + extraString);
                    symbolLevels.remove(symbol);
                    makeProboyTrade(symbol, true, vat.price, (int)vat.startPower,
                            new Long(vat.getLifeTime()).intValue(), true, fututres, false, false, evntId);
                } else {
                    // мы еще на уровне... можно проверить сильно ли упал power
                }
            } else
            {  // DOWN level
                if (vat.price < bestBuy.price) {
                    // отскок от нижнего уровня вверх
                    // TODO: надо для этого id сделки через какое то время выдать доп. инфомрацию в логи
                    String extraString = getExtraString(symbol, vat.price);
                    long evntId = GLOBAL_SIG_ID;
                    addEventTrigger(symbol, GLOBAL_SIG_ID, System.currentTimeMillis() + 15_000);
                    GLOBAL_SIG_ID++;

                    log.info("O <- " + symbol + " DOWN LEVEL " + formatPriceForSignalLog(vat.price)
                            + "  (level x " + (int)vat.startPower + " " + vat.getLifeTime() + "ms" + extraString);
                    symbolLevels.remove(symbol);
                    makeProboyTrade(symbol, false, vat.price, (int)vat.startPower,
                            new Long(vat.getLifeTime()).intValue(), false,fututres, false, false, evntId);
                } else if (vat.price > bestBuy.price) {
                    // ПРОБОЙ нижнего уровня вниз
                    // TODO: надо для этого id сделки через какое то время выдать доп. инфомрацию в логи
                    String extraString = getExtraString(symbol, vat.price);
                    long evntId = GLOBAL_SIG_ID;
                    addEventTrigger(symbol, GLOBAL_SIG_ID, System.currentTimeMillis() + 15_000);
                    GLOBAL_SIG_ID++;

                    log.info("P -> " + symbol + " DOWN LEVEL " + formatPriceForSignalLog(vat.price)
                            + "  (level x " + (int)vat.startPower + " " + vat.getLifeTime() + "ms" + extraString);
                    symbolLevels.remove(symbol);
                    makeProboyTrade(symbol, false, vat.price, (int)vat.startPower,
                            new Long(vat.getLifeTime()).intValue(), true,fututres, false, false, evntId);
                } else {
                    // мы еще на уровне... можно проверить сильно ли упал power
                }
            }
        }
    }

 //   MMA proboyMMA = new MMA( 40 );
 //   MMA proboyMMA2 = new MMA( 15 );

    final long STAKAN_STAT_TIMEFRAME = 10 * 60_000L;

    Map<String, ProboyLine> lastBinancePlBySymbol = new HashMap<>();
    Map<String, ProboyLine> lastBybitPlBySymbol = new HashMap<>();
    private boolean isBuyBitAndBinanceProboy(String symbol, long timeframe, int powerSum, int powerMin)
    {
        ProboyLine lastBBPl = lastBybitPlBySymbol.get(symbol);
        ProboyLine lastBinPl = lastBinancePlBySymbol.get(symbol);
        if  (lastBBPl == null || lastBinPl == null
                || lastBBPl.proboy != lastBinPl.proboy
                || lastBBPl.up != lastBinPl.up
        )
            return false;
        if (lastBBPl.power < powerMin ||
                lastBinPl.power < powerMin ||
                lastBBPl.power + lastBinPl.power < powerSum)
            return false;
        if (Math.abs(lastBBPl.time - lastBinPl.time) > timeframe)
            return false;
        return true;
    }

    Map<String, RSI> levelRsiBySymbol = new HashMap<>();
    private RSI getLevelRSI(String symbol, int N)
    {
        RSI rsi = levelRsiBySymbol.get(symbol);
        if (rsi == null) {
            rsi = new RSI(N);
            levelRsiBySymbol.put(symbol, rsi);
        }
        return rsi;
    }


    Map<String, DoubleLongPair> lastOtskokUpBySymbol = new ConcurrentHashMap<>();
    Map<String, DoubleLongPair> lastOtskokDownBySymbol = new ConcurrentHashMap<>();
    private DoubleLongPair getLastOtskok(String symbol, boolean up)
    {
        if (up)
            return lastOtskokUpBySymbol.get(symbol);
        else
            return lastOtskokDownBySymbol.get(symbol);
    }
    private void removeLastOtskok(String symbol, boolean up)
    {
        if (up)
            lastOtskokUpBySymbol.remove(symbol);
        else
            lastOtskokDownBySymbol.remove(symbol);
    }
    private void setLastOtskok(ProboyLine pl)
    {
        if (pl.up)
            lastOtskokUpBySymbol.put(pl.symbol, new DoubleLongPair(pl.price, (long)pl.power));
        else
            lastOtskokDownBySymbol.put(pl.symbol, new DoubleLongPair(pl.price, (long)pl.power));
    }

    /**  */
    private int getInterest(String symbol, boolean futures, boolean buy)
    {
        StakanContainer sContainer;
        if (futures)
            sContainer = bstakanBySymbolFutures.get( symbol );
        else
            sContainer = bstakanBySymbol.get( symbol );
        if (sContainer == null)
            return 0;
        Bstakan st1 = sContainer.bstakan;

        List<PriceQty> levels = st1.bids;
        if (!buy)
            levels = st1.asks;

        Double total = 0.0;
        Double fPrice = 0.0;
        for (int i=0; i < levels.size(); i++)
        {
                PriceQty pq = levels.get(i);
                if (i < 1)
                    fPrice = pq.price;
                total += pq.qty;
        }
        return Double.valueOf(total * fPrice).intValue();
    }

    /** Проверит сколько обьемов от указанной цены сверху и сколько снизу и какова разница */
    private void checkStakanSupport(ProboyLine pl)
    {
        ////////////////////////// проверка большого стакана
        long allMarketBuyInterest = 0;
        long allMarketSellInterest = 0;
        int deepBuyInterest = 0;
        int deepSellInterest = 0;
        for (String symb : bigDepthAskBySymbol.keySet())
        { // тут суммы уже в USDT а не в монетах
            MMATema bids = bigDepthBidBySymbol.get(symb);
            allMarketBuyInterest += Double.valueOf(bids.getSum()).longValue();
            MMATema asks = bigDepthAskBySymbol.get(symb);
            allMarketSellInterest += Double.valueOf(asks.getSum()).longValue();
            if (pl.symbol.equals(symb))
            {
                deepBuyInterest = Double.valueOf(bids.getSum()).intValue();
                deepSellInterest = Double.valueOf(asks.getSum()).intValue();
            }
        }
        ////////////////////////// Получим число сделок по монете и по всему рынку за окно = TRADE_SPEED_TIME_WINDOW
        int coinSpeed = 0;
        int allMarketSpeed = 0;
        for (String symb : tradeSpeedBySymbol.keySet()) {
            MMATema speed = tradeSpeedBySymbol.get(symb);
            allMarketSpeed += Double.valueOf(speed.getSum()).intValue();
            if (pl.symbol.equals(symb)) {
                coinSpeed = Double.valueOf(speed.getSum()).intValue();
            }
        }

        ////////////////////////// проверка ближнего стакана 1го символа
        StakanContainer sContainer;
        if (pl.futures)
            sContainer = bstakanBySymbolFutures.get( pl.symbol );
        else
            sContainer = bstakanBySymbol.get( pl.symbol );
        if (sContainer == null)
            return;
        Bstakan st1 = sContainer.bstakan;

        // bids - покупатели
        // asks - кто хочет лимитно продать

        Double buyersVol = 0.0;
        Double sellersVol = 0.0;

        for (int i=0; i < st1.bids.size(); i++)
        {
            PriceQty pq = st1.bids.get(i);
            if (pq.price != pl.price)
                buyersVol += pq.qty;
        }

        for (int i=0; i < st1.asks.size(); i++)
        {
            PriceQty pq = st1.asks.get(i);
            if (pq.price != pl.price)
                sellersVol += pq.qty;
        }

        if (sellersVol <= 0 || buyersVol <= 0)
            log.info("ERROR: No nearest level depth data..");

        MMAT avgTopStakanVol = _avgStakanVolBySymbol.get( pl.symbol );
        if (avgTopStakanVol != null)
        {
            pl.levelLiq = (int)(avgTopStakanVol.getAvg() * pl.power * pl.price);
        }

        pl.buyInterest = Double.valueOf(buyersVol * pl.price).intValue();
        pl.sellInterest = Double.valueOf(sellersVol * pl.price).intValue();
        pl.buyInterestDeep = deepBuyInterest;
        pl.sellInterestDeep = deepSellInterest;
        pl.buyIntAllmarket = allMarketBuyInterest;
        pl.sellIntallMarket = allMarketSellInterest;
        pl.speed = coinSpeed;
        pl.speedAllMarket = allMarketSpeed;
        log.info(" -+-"
                + " Buy interest: " + pl.buyInterest
                + " Sell interest: " + pl.sellInterest
                + " LL: " + pl.levelLiq
                + " DeepBuy i: " + deepBuyInterest
                + " DeepSell i: " + deepSellInterest
                + " allMBuy i: " + allMarketBuyInterest
                + " allMSell i: " + allMarketSellInterest
                + " spd: " + coinSpeed
                + " allMSpd: " + allMarketSpeed
        );
    }

    Map<String, MMAVT> powerUpCollectorBySymbol = new HashMap<>();
    Map<String, MMAVT> powerDownCollectorBySymbol = new HashMap<>();

    Map<String, MMAVT> llUpCollectorBySymbol = new HashMap<>();
    Map<String, MMAVT> llDownCollectorBySymbol = new HashMap<>();

    private MMAVT getPowerCollector(String symbol, boolean up, long ms)
    {
        MMAVT pc = null;
        if (up)
        {
            pc = powerUpCollectorBySymbol.get(symbol);
            if (pc == null)
            {
                pc = new MMAVT(1, ms);
                powerUpCollectorBySymbol.put(symbol, pc);
            }
        } else {
            pc = powerDownCollectorBySymbol.get(symbol);
            if (pc == null)
            {
                pc = new MMAVT(1, ms);
                powerDownCollectorBySymbol.put(symbol, pc);
            }
        }
        return pc;
    }

    private MMAVT getLLCollector(String symbol, boolean up, long ms)
    {
        MMAVT pc = null;
        if (up)
        {
            pc = llUpCollectorBySymbol.get(symbol);
            if (pc == null)
            {
                pc = new MMAVT(1, ms);
                llUpCollectorBySymbol.put(symbol, pc);
            }
        } else {
            pc = llDownCollectorBySymbol.get(symbol);
            if (pc == null)
            {
                pc = new MMAVT(1, ms);
                llDownCollectorBySymbol.put(symbol, pc);
            }
        }
        return pc;
    }

    /**  */
    private int getKlinesUpperBefore(ProboyLine pl, int comissionsDelta)
    {
        CandlePackage candlePackage = candlesBySymbol.get(pl.symbol);
        if (candlePackage == null)
        {
            log.info("Error: no candle pack for " + pl.symbol);
            return 0;
        }
        if (candlePackage.candles.size() < 1)
        {
            log.info("Error: candle pack empty for " + pl.symbol);
            return 0;
        }
        int counter = 0;
        for (int i = candlePackage.candles.size()-1; i >= 0; i--)
        {
            StreamCandle candle = candlePackage.candles.get(i);
            if (candle.l + comissionsDelta * pl.price / 1000.0 > pl.price)
                counter ++;
            else break;
        }
        return counter;
    }

    /**  */
    private int getKlinesLowerBefore(ProboyLine pl, int comissionsDelta)
    {
        CandlePackage candlePackage = candlesBySymbol.get(pl.symbol);
        if (candlePackage == null)
        {
            log.info("Error: no candle pack for " + pl.symbol);
            return 0;
        }
        if (candlePackage.candles.size() < 1)
        {
            log.info("Error: candle pack empty for " + pl.symbol);
            return 0;
        }
        int counter = 0;
        for (int i = candlePackage.candles.size()-1; i >= 0; i--)
        {
            StreamCandle candle = candlePackage.candles.get(i);
            if (candle.h - comissionsDelta * pl.price / 1000.0 < pl.price)
                counter ++;
            else break;
        }
        return counter;
    }

    /**   */
    private double getVolatileKoof(String symbol, double curPrice, int maxGo)
    {
        CandlePackage candlePackage = candlesBySymbol.get(symbol);
        if (candlePackage == null)
        {
            log.info("WARNING: no candle pack for " + symbol);
            return 100;
        }
        if (candlePackage.candles.size() < maxGo)
        {
            log.info("WARNING: not enough candles for " + symbol + " have only " + candlePackage.candles.size());
            return 100;
        }
        double priceExample = 1;
        EMMA emma = new EMMA(maxGo);
        StreamCandle sc = null;
        for (int i = candlePackage.candles.size() - maxGo; i < candlePackage.candles.size(); i++)
        {
            sc = candlePackage.candles.get(i);
            priceExample = sc.c;
            emma.addStat((sc.h - sc.l) / priceExample);
        }
        if (sc != null) emma.addStat(Math.abs(sc.c - curPrice)/ curPrice);
        return emma.getAvg() * 100.0;
    }

    /**  */
    private MMATema getSpeedCollector(String symbol)
    {
        MMATema speed = speedCollectorBySymb.get(symbol);
        if (speed == null) {
            speed = new MMATema(1, SPEED_FRAME * 60_000L);
            speedCollectorBySymb.put(symbol, speed);
        }
        return speed;
    }

    final long SPEED_FRAME = 2; // Число минут накопления "скорости"
    final int GLOBAL_NN = 6;    //
    final int GLOBAL_NN_MAX = 80;
    final int GLOBAL_N = 15;    // сколько минут собираум ликвидность
    final int GLOBAL_N_SHORT = 5;    // сколько минут собираум ликвидность
    final int GLOBAL_N2 = 22;   // сколько минут собираум ликвидность
    int MAX_HOURS_SIGNAL = 11;

    final int MS_R2 = 20;
    final int MS_D = 8;
    Map<Long, Double> globalUpIds = new HashMap<>();
    Map<Long, Double> globalDownIds = new HashMap<>();
    MMATema globalUpPostSignals = new MMATema(1, MS_R2 * 1_000L);
    MMATema globalDownPostSignals = new MMATema(1, MS_R2 * 1_000L);
    MMATema globalUpPostSignalsL = new MMATema(1, MS_D * 1_000L);
    MMATema globalDownPostSignalsL = new MMATema(1, MS_D * 1_000L);

    MMATema globalEventCounter = new MMATema(1, 15 * 1000L);
    MMATema globalUpPower = new MMATema(1, GLOBAL_N * 60 * 1000L);
    MMATema globalDownPower = new MMATema(1, GLOBAL_N * 60 * 1000L);
    MMATema globalUpPower2 = new MMATema(1, (GLOBAL_N + MS_R2) * 60 * 1000L);
    MMATema globalDownPower2 = new MMATema(1, (GLOBAL_N + MS_R2) * 60 * 1000L);
    MMATema globalUpPower_short = new MMATema(1, GLOBAL_N_SHORT * 60 * 1000L);
    MMATema globalDownPower_short = new MMATema(1, GLOBAL_N_SHORT * 60 * 1000L);

    Map<String, DoubleLongPair> levelBySymbol_goUp = new HashMap<>();
    Map<String, DoubleLongPair> levelBySymbol_goDown = new HashMap<>();

    Map<String, MMATema> speedCollectorBySymb = new HashMap<>();

    /**  */
    private void makeProboyTrade(String _symbol, boolean up, double levPrice,
                                 int power, int ms, boolean proboy, boolean futures, boolean bybit, boolean iceberg, long id)
    {
         if (!REAL_RUN) return;

        String symbol = _symbol.toUpperCase();
        if (symbol.startsWith("USDCUSDT")) return;

        DoubleLongPair lastOtskokUp = getLastOtskok(symbol, true);
        DoubleLongPair lastOtskokDown = getLastOtskok(symbol, false);

        int addPower = 0;

        if (iceberg /* && proboy */ ) {
            if (lastOtskokUp != null && up && levPrice == lastOtskokUp.d) {
                removeLastOtskok(symbol, true);
                log.info("~~ Touch ld otskok level UP: " + symbol + " power: " + lastOtskokUp.l + " price: " + lastOtskokUp.d + " adding power " + lastOtskokUp.l);
                addPower = lastOtskokUp.l.intValue();
            } else if (lastOtskokDown != null && !up && levPrice == lastOtskokDown.d) {
                removeLastOtskok(symbol, false);
                log.info("~~ Touch old otskok level DOWN: " + symbol + " power: " + lastOtskokDown.l + " price: " + lastOtskokDown.d + " adding power " + lastOtskokDown.l);
                addPower = lastOtskokDown.l.intValue();
            }
        }

    //    if (true) return;
        ProboyLine pl = new ProboyLine();
        pl.symbol = symbol;
        pl.up = up;
        pl.proboy = proboy;
        pl.time = System.currentTimeMillis();
        pl.futures = futures;
        pl.price = levPrice;
        pl.power = power + addPower;
        pl.ms = ms;
        pl.bybit = bybit;
        pl.iceberg = iceberg;
        pl.priceMove = getPriceMove(symbol, pl.price) * 1000.0 / pl.price;
        pl.id = id;

        checkStakanSupport(pl); // тут проставим ликвидность покупателей и продавцов вокруг этого уровня

        Double agressiveBuyVol = getAgressiveBuyVol(symbol);
        Double agressiveSellVol = getAgressiveSellVol(symbol);
        if (agressiveBuyVol != null) pl.agressBuy = Double.valueOf(agressiveBuyVol * pl.price).intValue();
        if (agressiveSellVol != null) pl.agressSell = Double.valueOf(agressiveSellVol * pl.price).intValue();

        if (bybit)
            lastBybitPlBySymbol.put(symbol, pl);
        else
            lastBinancePlBySymbol.put(symbol, pl);

        if (!futures)
            return;

        if (bybit)
            return;

        // Сохраним ID и цену, чтобы отследить подтверждения
        {
            if (    pl.up && pl.proboy
                    ||
                    !pl.up && !pl.proboy
            )
                globalUpIds.put(pl.id, pl.price);
            else if (
                    !pl.up && pl.proboy
                            ||
                            pl.up && !pl.proboy
            )
                globalDownIds.put(pl.id, pl.price);
        }

        if (iceberg)
            return;

        final double MIN_24_VOLUME = 2_400_000.0;
        SymbolInfo si = symbolInfos.get(symbol);
        if (si.volumes24h < MIN_24_VOLUME && getDeal(symbol) == null)
        {
            System.out.println("skip vol " + si.volumes24h);
            return;
        }

        final int AVR = 4;
        final int AVR2 = 6;
        final int PWL3 = 90;
        final int PWL4 = 20;
        final int MS = 45_500;            // макс. число милисек в течении котороых уровень разбирался
        final int MIN_LEVEL_LIQUIDITY = 10_000; // мин разробранная сумма на самом уровне
        final int MIN_LEVEL_LIQUIDITY_CLOSE = 10_000; // мин разробранная сумма на самом уровне
        final int n_short = 5;
        final int spedKoof = 20;
        final int R = 600;
        final int R2 = 30;
        final int RR = 500;
        final int RRR = 1000;
        final int N3 = 0;
        final int RRRR = -1;
        final int PS2 = 3;
        final int RA = 0;
        final int D = 11;
        final int D2 = 20;
        final int X = 1;            // Level delta Comission (for iceberg detect)
        final int Z = 300;
        final int X2 = 3;           // Level delta comissions (for price)
        final int _powLim = 24;          // во сколько раз ликвидность всего рынка превышает противоположную сторону (десятая часть)
        final int powLim_short = 11;    // во сколько раз ликвидность всего рынка превышает противоположную сторону (десятая часть)
        final int powLim2 = 8;         // то же самое, но для закрытия..
        final int POWER_LIMIT_S2 = 0;  // Минимальное число сигналов вверх (вниз) по всему рынку за время 'n'
        final int TP = 130;
        final int TPO = 80;
        final int SL = 100;
        final int SLO = 90;
        final int TP_CNT = 1;
        final int TP_CNT_START = 1;
        final int TP_CNT_B = 5;
        final int TP_CNT_START_B = 3;
        final double LEVERAGE = 0.28;

        if (pl.proboy) {
            if (pl.up) {
                globalUpPower.addStat(pl.levelLiq, pl.time);
                globalUpPower2.addStat(pl.levelLiq, pl.time);
            } else {
                globalDownPower.addStat(pl.levelLiq, pl.time);
                globalDownPower2.addStat(pl.levelLiq, pl.time);
            }
        }

        /*
        MMAVT powerCollector = getPowerCollector(pl.symbol, pl.up, MS);
        if (pl.proboy)
            powerCollector.addStat(pl.price, pl.power, pl.time);
        double powerCollected = powerCollector.getQtyInPriceRange( X ); */

        MMAVT llCollector = getLLCollector(pl.symbol, pl.up, MS);
        llCollector.addStat(pl.price, pl.levelLiq, pl.time);
        double llCollected = llCollector.getQtyInPriceRange( X );

        boolean megaSignal = true;
        //if (pl.power > X) megaSignal = true;

        double volumes24h = symbolInfos.get(pl.symbol).volumes24h;
        double volPart = volumes24h / 100_000.0;
        int overVolume = 0;
        if (volPart > 0)
            // overVolume = (int)((double)pl.levelLiq / volPart);
            overVolume = (int)( llCollected / volPart);

        double volPartAll = binancer.ALL_MARKET_24H_VOL / 100_00_000.0;
        int overVolumeAll = (int)((double)pl.levelLiq / volPartAll);

        //globalEventCounter.addStat(1, pl.time);

        /*     // ------------ DEBUG BLOCK --------------
        int _upperBars = getKlinesUpperBefore(pl, X2);
        int _lowerBars = getKlinesLowerBefore(pl, X2);
        log.info("Glbal UP: " + String.format("%.1f", globalUpPower.getSum2())
                    + " Global Down: " + String.format("%.1f", globalDownPower.getSum2())
                    + " Up bars: " + _upperBars
                    + " Down Bars: " + _lowerBars
                    + " " + pl.symbol + " UP: " + pl.up + " Proboy: " + pl.proboy + " LL " + pl.levelLiq + " LLC " + llCollected
                    + " in sell: " + pl.sellInterest
                    + " in buy: " + pl.buyInterest + "  over " + overVolume);
        if (true) return; */

        int volKoof = (int)(getVolatileKoof(pl.symbol, pl.price, GLOBAL_N2) * 10);
        System.out.println(volKoof + " " + overVolume + " " + globalUpPostSignalsL.getSum() + " " + globalDownPostSignalsL.getSum());

        {
            if (
                    (volKoof > GLOBAL_NN && !pl.proboy || pl.proboy)
                            && (volKoof < PWL3 && !pl.proboy || volKoof < PWL4 && pl.proboy)

                            && overVolumeAll >= RA
                            && overVolume >= R
                            && pl.levelLiq > MIN_LEVEL_LIQUIDITY
            ) {

                int zakis = (overVolume - R) / Z;
                int powLim = _powLim - zakis;

                //int upperBars = getKlinesUpperBefore(pl, X2);
                //int lowerBars = getKlinesLowerBefore(pl, X2);
                double upp = (globalUpPower.getSum() + globalUpPower2.getSum())/2.0;
                double dwn = (globalDownPower.getSum() + globalDownPower2.getSum())/2.0;
                System.out.println(upp + " " + dwn + " " + zakis);
                if (
                        (!pl.up && !pl.proboy || pl.up && pl.proboy)
                                && pl.buyInterest > pl.sellInterest
                                && upp * 10.0 / dwn > powLim
                                && (globalUpPostSignalsL.getSum() > RRRR
                                && globalDownPostSignalsL.getSum() < -RRRR
                                || pl.proboy && globalUpPostSignalsL.getSum() > RRRR
                                )
                ) {
                        Deal deal = createMarketOrder(symbol, BinOrder.SIDE_BUY, 0, levPrice, TP, SL, 150,
                                TP_CNT_B, TP_CNT_START_B, LEVERAGE, true, true,
                                false, megaSignal, true);
                        // sentTgMessage(symbol, true, pl.price, pl.levelLiq, deal);
                        return;
                }

                if (
                        (pl.up && !pl.proboy || !pl.up && pl.proboy)
                                && pl.sellInterest > pl.buyInterest
                                && dwn * 10.0 / upp > powLim
                                && (globalDownPostSignalsL.getSum() > RRRR
                                && globalUpPostSignalsL.getSum() < -RRRR
                                || pl.proboy && globalDownPostSignalsL.getSum() > RRRR
                                )
                ) {
                        Deal deal = createMarketOrder(symbol, BinOrder.SIDE_SELL, 0, levPrice, TPO, SLO, 150,
                                TP_CNT, TP_CNT_START, LEVERAGE, true, true,
                                false, megaSignal, true);
                        // sentTgMessage(symbol, false, pl.price, pl.levelLiq, deal);
                        return;
                }
            }
        }

        {
            if (
                    pl.levelLiq >= MIN_LEVEL_LIQUIDITY_CLOSE
            ) { // -- CLOSE --
                if (
                        (!pl.up && !pl.proboy || pl.up && pl.proboy)
                        && pl.buyInterest > pl.sellInterest
                        && globalUpPostSignalsL.getSum() > -1
                        && (overVolume >= RRR
                                ||
                            globalUpPower.getSum() * 10.0 / globalDownPower.getSum() > powLim2
                            && overVolume >= RR
                        )
                    )
                {
                    createMarketOrder(symbol, BinOrder.SIDE_BUY, 0, levPrice, TP, SL, 190,
                            TP_CNT, TP_CNT_START, LEVERAGE, true, true,
                            true, false, true);
                } else if (
                        (pl.up && !pl.proboy || !pl.up && pl.proboy)
                        && pl.sellInterest > pl.buyInterest
                        && globalDownPostSignalsL.getSum() > -1
                        && (overVolume >= RRR
                                ||
                            globalDownPower.getSum() * 10.0 / globalUpPower.getSum() > powLim2
                            && overVolume >= RR
                        )
                        )
                {
                    createMarketOrder(symbol, BinOrder.SIDE_SELL, 0, levPrice, TP, SL, 190,
                            TP_CNT, TP_CNT_START, LEVERAGE, true, true,
                            true, false, true);
                }
            }
        }
    }

    /**  */
    private void sentTgMessage(String symbol, boolean buy, double price, int liquidity, Deal d)
    {
        try {
                CandlePackage cp = candlesBySymbol.get(symbol);
                String fname = chartFileCreator.createCandlestickChart(symbol, cp.candles, 48, price, buy);
                String text = "\uD83D\uDD25<b>BUY</b>";
                String emo = "\uD83D\uDFE2";
                if (!buy) {
                    text = "\uD83D\uDD25<b>SELL</b>";
                    emo = "\uD83D\uDD34";
                }
                DecimalFormat df = new DecimalFormat("0.######");
                text = text + " " + symbol + " " + emo + " price " + df.format(price) + "\r\n\r\n";
                text = text + "Level iceberg: " + (int)(liquidity/1000) + " K USD";
                if (d == null)
                    text = text + "\r\nSkip. B: " + String.format("%.1f", BALANCE);
                else {
                    DealPack dealPack = getAnyDealPack();
                    int upC = 0, downC = 0;
                    for (Deal dx : dealPack.deals)
                    {
                        if (dx.up) upC++;
                        else downC++;
                    }
                    text = text + "\r\nq: " + d.quants.size() + " UP: " + upC + " DOWN: " + downC + " B: " + String.format("%.1f", BALANCE);
                }

                tgSender.sendPhotoFromFile(fname, text);
        } catch (Throwable t)
        {
            t.printStackTrace();
        }
    }

    /**  */
    private VolumeAtLevel addVolumeAtPrice(boolean up, String symbol,
                                           VolumeAtLevel existingVat, Double curPrice,
                                           double power,
                                           Map<String, VolumeAtLevel> symbolLevels)
    {
        if (existingVat == null)
        {
            VolumeAtLevel vat = new VolumeAtLevel(up, curPrice, power);
            symbolLevels.put(symbol, vat);
            return vat;
        } else {
            if (existingVat.up != up || existingVat.price != curPrice)
            { // уровень сместился - надо перезаписать как новый
                VolumeAtLevel vat = new VolumeAtLevel(up, curPrice, power);
                symbolLevels.put(symbol, vat);
                return vat;
            }
        }
        return existingVat;
    }

    // объемы на покупку и продажу по всему ближнему стакану - сразу в USDT
    Map<String, MMATema> bigDepthBidBySymbol = new HashMap<>();
    Map<String, MMATema> bigDepthAskBySymbol = new HashMap<>();
    // Глубокий стакан по depth20 (или меньше, если биржа прислала урезанный снимок).
    // Храним отдельно от основного depth5, чтобы не смешивать разную глубину.
    Map<String, Bstakan> deepBstakanBySymbolFutures = new ConcurrentHashMap<>();
    final long ALL_MARKET_DEPTH_WINDOW = 2_000;

    /** большой стакан  */
    public synchronized void updateBinanceStakanDeep(BstakanUpdate update, boolean futures)
    {
        //log.info("....... u  " + update);
        long curTime = System.currentTimeMillis();

        // Быстрый snapshot-путь для depth20: берем порядок уровней как прислал Binance.
        // Количество уровней может быть меньше 20 — это штатно и обрабатывается напрямую по факту размера списков.
        Bstakan deepBook = buildFastSnapshotBstakan(update);
        deepBook.updateTime = curTime;
        deepBstakanBySymbolFutures.put(update.sumbol, deepBook);

        MMATema depthBid = bigDepthBidBySymbol.get(update.sumbol);
        MMATema depthAsk = bigDepthAskBySymbol.get(update.sumbol);
        if (depthBid == null)
        {
            depthBid = new MMATema(1, ALL_MARKET_DEPTH_WINDOW);
            bigDepthBidBySymbol.put(update.sumbol, depthBid);
            depthAsk = new MMATema(1, ALL_MARKET_DEPTH_WINDOW);
            bigDepthAskBySymbol.put(update.sumbol, depthAsk);
        }
        depthBid.addStat(update.bidSum, curTime);
        depthAsk.addStat(update.askSum, curTime);
    }

    /**  */
    public synchronized void updateBinanceStakan(BstakanUpdate update, boolean futures)
    {      
        StakanContainer sContainer = getStakanContainerBySymbol(update.sumbol, futures);
        //SymbolInfo sInfo = symbolInfos.get(sContainer.symbol);
        try {
            Bstakan bstakan = buildFastSnapshotBstakan(update);
            sContainer.bstakan = bstakan;
            sContainer.bstakan.updateTime = System.currentTimeMillis();
            if (futures)
                checkTopLevel( update.sumbol, futures );
            /*
            if (System.currentTimeMillis() - lastStakanPrintTime > 5_00)
            {
                lastStakanPrintTime = System.currentTimeMillis();
                log.info(update.sumbol + " Best Buy: " + sContainer.bstakan.getBestBuy() +
                        " Best Sell: " + sContainer.bstakan.getBestSell());
            } */
            /*
            if (sContainer.bstakan == null)
            {
                log.info("Binance stakan initing ... " + futures);
                Bstakan stakan = binancer.getStakan( futures, update.sumbol);
                sContainer.bstakan = stakan;
            ///    sContainer.prevBinanceEventID = stakan.lastUpdateId;
            } else if (update.prevUpdateId != null && sContainer.prevBinanceEventID != 0 && update.prevUpdateId.longValue() != sContainer.prevBinanceEventID)
            {
                log.info("Prev binance eventId (saved) = prevBinanceEventID "
                        + sContainer.prevBinanceEventID +" !=    prevEventId from message: "
                        + update.prevUpdateId + "   ->   reload BStakan");
                Bstakan stakan = binancer.getStakan( futures, update.sumbol);
                sContainer.bstakan = stakan;
             ///   sContainer.prevBinanceEventID = stakan.lastUpdateId;
            } else
            {
                // Можно апдейтить...
                binancer.updateStakan(sContainer, update);
                if (System.currentTimeMillis() - lastStakanPrintTime > 5_000)
                {
                    lastStakanPrintTime = System.currentTimeMillis();
                    log.info(update.sumbol + " Best Buy: " + sContainer.bstakan.getBestBuy() +
                            " Best Sell: " + sContainer.bstakan.getBestSell());
                }
            } */
            //saveStakanContainer(sContainer, futures);
        } catch (Throwable e)
        {
            log.info("Exception processing bstakan update!", e);
            e.printStackTrace();
        }
    }

    /** Собирает снимок стакана напрямую из апдейта, сохраняя исходный порядок Binance. */
    private Bstakan buildFastSnapshotBstakan(BstakanUpdate update)
    {
        Bstakan bstakan = new Bstakan();
        bstakan.symbol = update.sumbol;
        if (update.lastUpdateId != null) {
            bstakan.lastUpdateId = update.lastUpdateId;
        }

        if (update.bids != null) {
            bstakan.bids = new ArrayList<>(update.bids.size());
            bstakan.bidsMap = new HashMap<>(Math.max(16, update.bids.size() * 2));
            for (int i = 0; i < update.bids.size(); i++) {
                PriceQty pq = update.bids.get(i);
                bstakan.bids.add(pq);
                bstakan.bidsMap.put(pq.price, pq);
                bstakan.totalBidLots += pq.qty;
            }
        }

        if (update.asks != null) {
            bstakan.asks = new ArrayList<>(update.asks.size());
            bstakan.asksMap = new HashMap<>(Math.max(16, update.asks.size() * 2));
            for (int i = 0; i < update.asks.size(); i++) {
                PriceQty pq = update.asks.get(i);
                bstakan.asks.add(pq);
                bstakan.asksMap.put(pq.price, pq);
                bstakan.totalAskLots += pq.qty;
            }
        }

        return bstakan;
    }

    Map<String, MMAT> _tradeSummatorBBySymbol = new HashMap<>();
    Map<String, MMAT> _tradeSummatorSBySymbol = new HashMap<>();
    Map<String, Double> _lastBpriceBySymbol = new HashMap<>();
    Map<String, Double> _lastSpriceBySymbol = new HashMap<>();

    Map<String, MMAT> _tradeSummatorBBySymbol_S = new HashMap<>();
    Map<String, MMAT> _tradeSummatorSBySymbol_S = new HashMap<>();
    Map<String, Double> _lastBpriceBySymbol_S = new HashMap<>();
    Map<String, Double> _lastSpriceBySymbol_S = new HashMap<>();
    final long TRADE_SUMMATOR_PRERIOD = 1200; // ms
    /**  */
    private void findLevels(BTrade trade, boolean futures)
    {

        Map<String, MMAT> tradeSummatorBBySymbol = _tradeSummatorBBySymbol;
        Map<String, MMAT> tradeSummatorSBySymbol = _tradeSummatorSBySymbol;
        Map<String, Double> lastBpriceBySymbol = _lastBpriceBySymbol;
        Map<String, Double> lastSpriceBySymbol = _lastSpriceBySymbol;
        if (!futures)
        {
            tradeSummatorBBySymbol = _tradeSummatorBBySymbol_S;
            tradeSummatorSBySymbol = _tradeSummatorSBySymbol_S;
            lastBpriceBySymbol = _lastBpriceBySymbol_S;
            lastSpriceBySymbol = _lastSpriceBySymbol_S;
        }

        Boolean proboy = null;
        Double sumQty = null;
        Double levelPrice = null;
        Boolean up = null;
        if (trade.isBuyerMarketMaker)
        { // Лимитник - покупатель
            up = false;
            Double lastPrice = lastBpriceBySymbol.get(trade.symbol);
            if (lastPrice == null)
            {
                lastBpriceBySymbol.put(trade.symbol, trade.price);
                MMAT summator = new MMAT(1, TRADE_SUMMATOR_PRERIOD);
                tradeSummatorBBySymbol.put(trade.symbol, summator);
                summator.addStat(trade.qty, trade.time);
            } else if (trade.price < lastPrice)
            {
                levelPrice = lastPrice;
                lastBpriceBySymbol.put(trade.symbol, trade.price);
                proboy = true;
                MMAT summator = tradeSummatorBBySymbol.get(trade.symbol);
                sumQty = summator.getSum();
                summator = new MMAT(1, TRADE_SUMMATOR_PRERIOD);
                tradeSummatorBBySymbol.put(trade.symbol, summator);
                summator.addStat(trade.qty, trade.time);
            } else if (trade.price > lastPrice)
            { // otskok
                levelPrice = lastPrice;
                lastBpriceBySymbol.put(trade.symbol, trade.price);
                proboy = false;
                MMAT summator = tradeSummatorBBySymbol.get(trade.symbol);
                sumQty = summator.getSum();
                summator = new MMAT(1, TRADE_SUMMATOR_PRERIOD);
                tradeSummatorBBySymbol.put(trade.symbol, summator);
                summator.addStat(trade.qty, trade.time);
            } else {
                // same price
                MMAT summator = tradeSummatorBBySymbol.get(trade.symbol);
                if (summator == null) {
                    summator = new MMAT(1, TRADE_SUMMATOR_PRERIOD);
                    tradeSummatorBBySymbol.put(trade.symbol, summator);
                }
                summator.addStat(trade.qty, trade.time);
            }
        } else {
            // Лимитник - продавец
            up = true;
            Double lastPrice = lastSpriceBySymbol.get(trade.symbol);
            if (lastPrice == null)
            {
                lastSpriceBySymbol.put(trade.symbol, trade.price);
                MMAT summator = new MMAT(1, TRADE_SUMMATOR_PRERIOD);
                tradeSummatorSBySymbol.put(trade.symbol, summator);
                summator.addStat(trade.qty, trade.time);
            } else if (trade.price > lastPrice)
            {
                levelPrice = lastPrice;
                lastSpriceBySymbol.put(trade.symbol, trade.price);
                proboy = true;
                MMAT summator = tradeSummatorSBySymbol.get(trade.symbol);
                sumQty = summator.getSum();
                summator = new MMAT(1, TRADE_SUMMATOR_PRERIOD);
                tradeSummatorSBySymbol.put(trade.symbol, summator);
                summator.addStat(trade.qty, trade.time);
            } else if (trade.price < lastPrice)
            { // otskok
                levelPrice = lastPrice;
                lastSpriceBySymbol.put(trade.symbol, trade.price);
                proboy = false;
                MMAT summator = tradeSummatorSBySymbol.get(trade.symbol);
                sumQty = summator.getSum();
                summator = new MMAT(1, TRADE_SUMMATOR_PRERIOD);
                tradeSummatorSBySymbol.put(trade.symbol, summator);
                summator.addStat(trade.qty, trade.time);
            } else {
                // same price
                MMAT summator = tradeSummatorSBySymbol.get(trade.symbol);
                if (summator == null) {
                    summator = new MMAT(1, TRADE_SUMMATOR_PRERIOD);
                    tradeSummatorSBySymbol.put(trade.symbol, summator);
                }
                summator.addStat(trade.qty, trade.time);
            }
        }

        //////////////////////
        if (proboy != null)
        {
            String upStr = "UP";
            if (!up)
                upStr = "DOWN";

            int power = 15; // default for spot
            int levelLiq = (int)(sumQty * trade.price);
            String letter = "X";
            if (futures) {
                MMAT avgTopStakanVol = _avgStakanVolBySymbol.get(trade.symbol);
                if (avgTopStakanVol == null)
                    return;
                power = (int) (sumQty / avgTopStakanVol.getAvg());
                if (power < SIGNAL_KOOF)
                    return;
            } else {
                if (levelLiq < 60_000)
                    return;
                letter = "z";
            }

            // TODO: надо для этого id сделки через какое то время выдать доп. инфомрацию в логи
            long evntId = GLOBAL_SIG_ID;
            String extraString = getExtraString(trade.symbol, levelPrice);
            addEventTrigger(trade.symbol, GLOBAL_SIG_ID, System.currentTimeMillis() + 15_000);
            GLOBAL_SIG_ID++;

            if (proboy)
            {
                // proboy
                log.info("P."+letter+" -> " + trade.symbol + " " + upStr + " LEVEL " + formatPriceForSignalLog(levelPrice) + "  (level x " + power + " "
                        + "0ms LL " + levelLiq + extraString);
                if (futures)
                    makeProboyTrade(trade.symbol, up, levelPrice, power, 0, true,true, false, true, evntId);
            } else {
                // otskok
                log.info("O."+letter+" -> " + trade.symbol + " " + upStr + " LEVEL " + formatPriceForSignalLog(levelPrice) + "  (level x " + power + " "
                        + "0ms LL " + levelLiq + extraString);
                if (futures)
                    makeProboyTrade(trade.symbol, up, levelPrice, power, 0, false,true, false, true, evntId);
            }
        }
    }

    // торгуем на отскок или на пробой
    public static boolean MODE_OTSKOK = true;

    // Во сколько раз ближайший объем должен быть больше среднего чтобы считать это тригером - для сигнала чтобы сравнить что как..
    public double SIGNALING_BIG_RATIO_F = 1.5; // 6,  5  - is not bad!
    public double SIGNALING_BIG_RATIO_S = 1.5; // 6,  5  - is not bad!
    // Во сколько раз должен упасть объем на уровне чтобы считать его "разъеденным"
    public double ALERTING_DECREASE_KOOF = 3;
    // Максимальное растояние до стопа по сигнальной монете (т.е. при таком вот ПЛЕЧЕ и таком СТОПе мы обнулим депозит)
    public double MAX_DISTANCE_TO_SL_TRADING_COIN = 40.0;
    // Максимальное растояние до стопа по BTCUSDT (т.е. при таком вот ПЛЕЧЕ и таком СТОПе мы обнулим депозит)
    public double MAX_DISTANCE_TO_SL_BTCUSDT = 40.0;

    public int MIN_DELTA_PRETENDENT = 5;
    public int GREAT_DELTA_PRETENDENT = 5;  // 5 was good!

    // symbol -> levelQty appearTime
    private Map<String, Long> levelAppearTime = new HashMap<>();
    
    public boolean openNewDeals = true;
    
    public static volatile int MIN_CANDLES_FOR_LEVEL = 10;
    
    // symbol -> perehi_koof
    private Map<String, PretendentSymbol> pretendentSymbols = new HashMap<>();
    /*
        Функция анализирует ближайшией к спреду объемы - если они очень большие то алертим.
        а вообще они должны быть не просто большими, а еще и стремительно уменьшаться! 
        и при этом в противоположной стороне не должно быть такой ситуации
    */
    
    private Map<String, LevelDesc> levelDescBySymbol = new HashMap<>();
    private void saveLevelDesc(String symbol, Integer barsTillCrossUp, Integer barsTillCrossDown)
    {
        LevelDesc ld = levelDescBySymbol.get( symbol );
        if (ld == null)
            ld = new LevelDesc( symbol );
        if (barsTillCrossUp != null)
        {
            ld.barsTillCrossUp = barsTillCrossUp;
            ld.barsTillCrossUpTime = System.currentTimeMillis();
        }
        if (barsTillCrossDown != null)
        {
            ld.barsTillCrossDown = barsTillCrossDown;
            ld.barsTillCrossDownTime = System.currentTimeMillis();
        }
        levelDescBySymbol.put(symbol, ld);
    }
    /**  */
    private LevelDesc getLevelDesc(String symbol)
    {
        LevelDesc ld = levelDescBySymbol.get( symbol );
        if (ld == null)
            ld = new LevelDesc( symbol);
        return ld;
    }

    
    /**  */
    private void runSituation(SituationResult sr, String side, int deltaPretendent)
    {
        int addedCnt = 0;
        for (int i=0; i < sr.triggeredSymbols.size() && addedCnt < 1; i++)
        {
            String symbol = sr.triggeredSymbols.get(i);
            LevelDesc ld = this.getLevelDesc(symbol);

            if ("BTCUSDT".equals(symbol))  // ok
            {
                continue;
            }
            
            if (sr.triggeredBuy || sr.triggeredSell)
            {
                // ТОЛЬКО ШОРТиМ!
                //if (sr.triggeredSell)
                {
                    createMarketOrder(symbol, side, deltaPretendent, null, 100,
                            100, 100, 1, 1,10.0,
                            true, true, false, false, true);
                    addedCnt++;
                }
            }
        }
    }
    
    private Situation lastSituation = null;

    public Double getAlternativePrice(String symbol)
    {
        StakanContainer sc = getStakanContainerBySymbol( symbol, true);
        if (sc == null)
        {
            log.info("Stakan NULL for symbol " + symbol);
            return null;
        }
        return (sc.bstakan.getBestBuy().price + sc.bstakan.getBestSell().price) / 2.0;
    }

    /**  */
    private int getPretendents(boolean up)
    {
        int cnt = 0;
        for (PretendentSymbol ps : this.pretendentSymbols.values())
        {
            if (ps.up == up)
                cnt++;
        }
        return cnt;
    }
    
    private Long firstStatTime = null;
    private boolean wasInSignalZone = true;
    
    private final double SIGNAL_DELTA_PERCENT = 3.0;
    private final double SIGNAL_STOP_DELTA_PERCENT = 1.0;
    
    /**  */
    private void analyzeMarketStat()
    {
        if (firstStatTime == null)            
            firstStatTime = System.currentTimeMillis();
        if (System.currentTimeMillis() - firstStatTime < 180_000L)
            return;       
        /*
        MarketStat marketStat = getMarketStat( null );
        Deal _curDeal = this.currentDeal;
        
        if (marketStat.barsUpPercent < 15.0 &&
                marketStat.upPercent > 50.0 + SIGNAL_DELTA_PERCENT && marketStat.twoMinuteUpPercent > 50.0 + SIGNAL_DELTA_PERCENT  && !wasInSignalZone)
        {   // BUY SIG
            wasInSignalZone = true;
            if (_curDeal != null && !_curDeal.up)
                this.closeDeal(_curDeal);
            prontMarketStat( marketStat );
            log.info(" - B - ");
            createMarketOrder("BTCUSDT", BinOrder.SIDE_BUY, false, null, null, 30);
            
        } else if (marketStat.barsUpPercent > 85.0 &&
                marketStat.upPercent < 50.0 - SIGNAL_DELTA_PERCENT && marketStat.twoMinuteUpPercent < 50.0 - SIGNAL_DELTA_PERCENT && !wasInSignalZone  )
        {   // SELL SIG
            wasInSignalZone = true;
            if (_curDeal != null && _curDeal.up)
                this.closeDeal(_curDeal);
            prontMarketStat( marketStat );
            log.info(" - S - ");
            createMarketOrder("BTCUSDT", BinOrder.SIDE_SELL, false, null, null, 30);            
        } else
        {
            wasInSignalZone = false;
            
            if (_curDeal != null )
            {
                if (!_curDeal.up && (marketStat.upPercent > 50.0 + SIGNAL_STOP_DELTA_PERCENT && marketStat.twoMinuteUpPercent > 50.0 + SIGNAL_STOP_DELTA_PERCENT))
                {
                    prontMarketStat( marketStat );
                    log.info("Closing deal..");
                    this.closeDeal(_curDeal);
                } else
                if (_curDeal.up && (marketStat.upPercent < 50.0 - SIGNAL_STOP_DELTA_PERCENT && marketStat.twoMinuteUpPercent < 50.0 - SIGNAL_STOP_DELTA_PERCENT))
                {
                    prontMarketStat( marketStat );
                    log.info("Closing deal..");
                    this.closeDeal(_curDeal);
                }
            }
        }*/
    }

    
    /**  */
    private double getOurTrendPercent(boolean up, String symbol)
    {
        MarketStat marketStat = getMarketStat( symbol );
        if (up)
            return marketStat.upPercent;
        return 100.0 - marketStat.upPercent;
    }
    
    /**  */
    private double getOurTrendPercentByBars(boolean up, String symbol)
    {
        MarketStat marketStat = getMarketStat( symbol );
        if (up)
            return marketStat.barsUpPercent;
        return 100.0 - marketStat.barsUpPercent;
    }
    
    Map<String, MarketStatElem> marketStatMap = new HashMap<>();
    /**  */
    public void putMarketStat(MarketStatElem elem)
    {
        MarketStatElem oldElem = marketStatMap.get( elem.symbol );
        if (oldElem != null)
            elem.addOldHist( oldElem.levelHist );
        marketStatMap.put(elem.symbol, elem);        
        // analyzeMarketStat();
    }
    private long lastMarketStatRequest = 0;
    private MarketStat _lastMarketStat;
    /**  */
    public MarketStat getMarketStat(String symbol)
    {
        //if (System.currentTimeMillis() - lastMarketStatRequest < 2000L)
          //  return _lastMarketStat;
        MarketStat ms = new MarketStat();
        
        int upCoins = 0;
        int downCoins = 0;
        int totalUpBars = 0;
        int totalDownBars = 0;
        int total2minUps = 0;
        int total2minDowns = 0;
        
        for (MarketStatElem elem : marketStatMap.values())
        {
            if (symbol != null && !symbol.equals( elem.symbol ))
                continue;
            ms.totalCoins++;
            if (elem.up)
            {
                upCoins++;
                totalUpBars += elem.barsToWall;
            } else
            {
                downCoins++;
                totalDownBars += elem.barsToWall;
            }
            total2minUps += elem.getTotalUps();
            total2minDowns += elem.getTotalDowns();
        }
        ms.upPercent = (double) upCoins * 100.0 / (double)(upCoins + downCoins);
        ms.twoMinuteUpPercent = (double) total2minUps * 100.0 / (double) (total2minUps + total2minDowns);
        double avgUpBars = upCoins > 0 ? (double)totalUpBars / (double)upCoins : 0;
        double avgDownBars = downCoins > 0 ? (double)totalDownBars / (double)downCoins : 0;
        ms.barsUpPercent = (double) avgUpBars * 100.0 / (double)(avgUpBars + avgDownBars);
        _lastMarketStat = ms;
        lastMarketStatRequest = System.currentTimeMillis();
        return ms;
    }
    
    private final double MIN_DISTANCE_TO_SL_IN_COMISSIONS = 4.0;
    /** */
    private LevelGridElem getBestSL(List<LevelGridElem> grid, double enterPrice)
    {
        if (grid == null || grid.size() < 1)
            return null;
        
        LevelGridElem level = grid.get(0);
        if (level.distanceInComissions < MIN_DISTANCE_TO_SL_IN_COMISSIONS)
        {
            if (grid.size() > 1)
            {
                log.info("Nearest SL is too near. selecting next SL.");
                LevelGridElem nextlevel = grid.get( 1 );                
                double distanceToSl = Math.abs(enterPrice - nextlevel.thePrice);
                if (distanceToSl * MAX_DISTANCE_TO_SL_TRADING_COIN > enterPrice)
                {
                    log.info("SL level is too far away (more then "+String.format("%.2f", MAX_DISTANCE_TO_SL_TRADING_COIN) +" leverage) at " + nextlevel.thePrice + " --> SKIP deal");                    
                } else
                    level = nextlevel;
            }
        }
        if (level.distanceInComissions < MIN_DISTANCE_TO_SL_IN_COMISSIONS)
        {
            log.info("SL is too near - lets set it to " + MIN_DISTANCE_TO_SL_IN_COMISSIONS);
            level.distanceInComissions = MIN_DISTANCE_TO_SL_IN_COMISSIONS;
        }
        return level;
    }
    
    // Сколько времени храним инфо о пробитых или отскоченных уровнях
    private static final long MAX_TTL_LOST_LEVELS_INFO = 2000L;
    private List<LostPretendentInfo> lostPretendents = new ArrayList<>();
    /**  */
    private void addLostPretendent(String symbol, boolean up, boolean probilo)
    {
        LostPretendentInfo lpi = new LostPretendentInfo(up, symbol, probilo);
        lostPretendents.add(lpi);
        long curTime = System.currentTimeMillis();
        while (lostPretendents.size() > 0 && curTime - lostPretendents.get(0).time > MAX_TTL_LOST_LEVELS_INFO)
        {
            lostPretendents.remove( 0 );
        }
    }
    
    /** сколько монет только что пробило уровень  */
    private ProbiloInfo getProbiloInfo()
    {
        ProbiloInfo pi = new ProbiloInfo();
        for (int i=0; i < lostPretendents.size(); i++)
        {
            LostPretendentInfo lpi = lostPretendents.get(i);
            if (lpi.probilo)
                pi.probilo++;
            else
                pi.neProbilo++;
        }
        if (pi.neProbilo == 0)
            pi.probiloRation = 100.0;
        else
            pi.probiloRation = new Double(pi.probilo) / new Double(pi.neProbilo);
        
        return pi;
    }

    //////////////////////////////////
    private class ProbiloInfo {
        public int probilo = 0;
        public int neProbilo = 0;
        // Если число пробитий больше числа НЕ_пробитий, то этот кооф-т будет больше 1. Если больше отскоков - то кооф-т меньше 1
        public double probiloRation = 0;
        public ProbiloInfo() {}

        @Override
        public String toString() {
            return "ProbiloInfo{" + "probilo=" + probilo + ", neProbilo=" + neProbilo + ", probiloRation=" + String.format("%.2f",probiloRation) + '}';
        }        
    }
    //////////////////////////////////
    //////////////////////////////////
    private class MarketStat {
        // сколько всего монет в анапизе
        public int totalCoins;
        // сколько монет сигнализировали вверх
        public double upPercent;
        // на сколько блиайшая вершиа у тех кто сигнализировал вверх дальше чем у тех кто сигнализировал вниз
        public double barsUpPercent;
        
        // процент упираний вверх за 2 минуты а не только последнее
        public double twoMinuteUpPercent;
        
        public MarketStat() {}

        @Override
        public String toString() {
            return "MarketStat{" + "totalCoins=" + totalCoins + ", upPercent=" + String.format("%.2f",upPercent) + ", barsUpPercent=" + String.format("%.2f",barsUpPercent) + '}';
        }        
    }
    //////////////////////////////////

    
    /**  */
    private Deal getDeal(SymbolInfo sInfo)
    {
        return getDeal(sInfo.symbol);
    }
    
    /**  */
    private synchronized Deal getDeal(String symbol)
    {
        for (int i=0; i<dealPacks.size(); i++)
        {
            DealPack dp = dealPacks.get(i);
            synchronized (dp.deals) {
                Set<Deal> _currDeals = dp.deals;
                for (Deal d : _currDeals) {
                    if (d.symbol.equals( symbol )) {
                        return d;
                    }
                }
            }
        }
        return null;
    }
    
    long startTime = 0;
   
    volatile boolean ordersBusy = false;    
    /* Получает сообщения с ордерами и их изменениями */
    public synchronized void updateOrders(OrderContainer orders)
    {
        if (ordersBusy)
        {
            log.info("         SYNC ORDERS ");
            return;
        }
        ordersBusy = true;
        try {
            for (OrderResp order : orders.getData())
            {
                log.info("Received order [" + orders.getAction() + "] " + order);
            }
        } catch (Exception e)
        {
            log.error("Exception processing orders update!", e);
        }
        ordersBusy = false;
    }
    
    volatile boolean marginBusy = false;
    
    /* Получает сообщения с обновлением инфы о марже */
    public synchronized void updateMargin(MarginContainer margins)
    {
        if (marginBusy)
        {
            log.info("         SYNC ORDERS ");
            return;
        }
        marginBusy = true;
        try {
            for (MarginRecord m : margins.getData())
            {
                log.info("Received margin [" + margins.getAction() + "] " + m);
                if (m.getGrossOpenCost() != null)
                    this.usedOpenCnt = m.getGrossOpenCost();
            }
        } catch (Exception e)
        {
            log.error("Exception processing margin update!", e);
        }
        marginBusy = false;
    }
    
    volatile boolean stakanBusy = false;    
    
 
    // Доля от общего объема которую надо накопить чтобы закрыть наборчик самых ТОП уровней по объему
    final double TOP_SIZE_PERCENT = 0.2;
    // во сколько раз должно быть больше лотов на одной стороне стакана, чтобы обратить на это внимание
    final double CRITICAL_TOTAL_LOT_SIZE_MULT = 1.8;
    
    double TP = 0;

    
    public static final DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    private String goodFloat(double d)
    {
        return (""+d).replace('.', ',');
    }
    
    // --------------------------------------------------- .... BUY   SELLL ... -------------------------------------------------------------
    
    // Переменные для того, чтобы повторять сигнал даже если реального сигнала нет, НО мы еще не вошли в позицию по прошлому сигналу
    boolean REPEAT_onlyClose = false;
    double REPEAT_buy_price = -1;
    double REPEAT_sell_price = -1;
    double REPEAT_DELTA_PRICE = 4.0;    // макс. отклонение текущей цены от цены сигнала при котором мы повторим сигнал     Цифра д.б. в пределах комиссии тейкера.
    double REPEATED_price = -1;         // запоминаем последнюю цену при которой мы повторили сигнал - чтобы несколько раз не повторять по одной цене
    
    int tradeCnt = 0;
    double profit = 0;
    
    // Цены п которым купили или продали. 0 если не в сделке
    double buyPrice = 0;
    double sellPrice = 0;
 
    long lastNegativePNLtime = 0L;
    
    /* Обновлдение позиции от Binance Futures */
    public synchronized void updateBinancePosition(Set<Position> ps) {
        try {
            
            Set<Deal> allDeals = new HashSet<>();
            if (this.currentDeal != null)
            { // вариант с одной сделкой
                allDeals.add(currentDeal);
            } else
            { // вариант с пакетом сделок
                for (int i=0; i < dealPacks.size(); i++)
                {
                    DealPack dp = dealPacks.get(i);
                    synchronized (dp.deals)
                    {
                        for (Deal d: dp.deals)
                            allDeals.add(d);
                    }
                }
            }
            
            if (allDeals.isEmpty())
                return;
            
            for (Position p : ps)
            {
                log.info("Received position " + p);                    
                for (Deal d : allDeals)
                {
                    if (p.symbol.equals( d.symbol))
                    {
                        if ("BOTH".equals( p.positionSide ) )
                        {
                            if (p.positionAmt == 0)
                            {
                                // ВАЖНО! Если торгуем с реверсом,  то эту строку надо ПЕРЕНЕСТИ ОТСЮДА!
                                // т.к. с реверсом может прилететь 0 а позиция не ноль на самом деле
                                //d.currentLotSize = 0;

                                //d.zeroQtySetTime = System.currentTimeMillis();

                                final int MIN_DELAY_AFTER_OPEN = 15_000;

                                if (System.currentTimeMillis() - d.openTime < MIN_DELAY_AFTER_OPEN && REVERSE_AFTER_CLOSE)
                                {
                                    log.info("Got 0 amount but after less then "+(MIN_DELAY_AFTER_OPEN/1000)+" sec after open deal :: skip close " + d.symbol);
                                } else
                                {
                                    log.info("Got 0 Amount after more then "+(MIN_DELAY_AFTER_OPEN/1000)+" sec from Open :: closing " + d.symbol);
                                    d.currentLotSize = 0;
                                    delOrders(d.slOrders);
                                    delOrders(d.tpOrders);
                                    closeDeal(d, false);
                                    removeDealFromPack( d );
                                    saveTradeState("updateBinancePosition_close");
                                }
                            } else
                            {
                                double newAmnt = Math.abs( p.positionAmt );

                                    // for MARKET trades
                                    log.info("Position is just set up or should be partly decreased...");

                                    Long openDealTime = symbolOpenDealTime.get(d.symbol);
                                    if (openDealTime == null)
                                    {
                                        log.info("!!! STRANGE SITUATION !!! openDealTime = null for " + d.symbol);
                                        return;
                                    }
                                    if (System.currentTimeMillis() - openDealTime < 20_000)
                                    {
                                        log.info(" Too early to fix position amnt becoz of TP SL. Posibly it is just opening pos...");
                                        return;
                                    }

                                    d.currentLotSize = newAmnt;
                                    // TODO: нужно изменить размер позиции
                                    // м.б. частичное уменьшение если например ТП цепанулся не полностью
                                    // сюда зашли...

                                    if (d.quants.size() > 0 && d.getTotalQty().doubleValue() != newAmnt)
                                    {
                                        double newQuantQty = newAmnt / d.quants.size();
                                        for (int i=0; i < d.quants.size(); i++)
                                        {
                                            PriceQtyB pq = d.quants.get(i);
                                            pq.qty = new BigDecimal(newQuantQty);
                                        }
                                    }
                                    saveTradeState("updateBinancePosition_resize");

                                /*
                                    while (d.getTotalQty().doubleValue() > newAmnt)
                                    {
                                        PriceQtyB pq = d.getLastQuant();
                                        if (d.getTotalQty().doubleValue() - newAmnt >= pq.qty.doubleValue()) {
                                            // Разница в новом и сохраненном обьеме больше чем размер этого кванта
                                            // удаляем его совсем.
                                            d.removeLastQuant();
                                            log.info("Removing 1 quant of size " + pq.qty);
                                        } else {
                                            // разница в новом и сохраненном обьеме меньше этого кванта
                                            // уменьшим его размер просто
                                            pq.qty = pq.qty.subtract(new BigDecimal(d.getTotalQty().doubleValue() - newAmnt));
                                            log.info("partly decreased last quant.");
                                            break;
                                        }
                                    } */
                                    /*
                                    if (d.getTotalQty().doubleValue() < newAmnt)
                                    { // общий обьем меньше того что прилетел.
                                        // Это значит что закупка по рынку на самом деле прошла порциями
                                        // и надо доувеличить последний квант - разницей.
                                        double extra = newAmnt - d.getTotalQty().doubleValue();
                                        log.info("Got real amount bigger than inited in deal. Lets add " + extra);
                                        PriceQtyB pq = d.getLastQuant();
                                        pq.qty = pq.qty.add(new BigDecimal( extra ));
                                        log.info("Last quant qty set to " + pq.qty);
                                    } */

                            }
                        } 
                        /*
                        else if ("SHORT".equals( p.positionSide ) && !d.up )
                        {
                            if (p.positionAmt == 0)
                            {
                                log.info("NO POS -> ...let's kill all open orders..");                                
                                delOrders(d.slOrders);
                                delOrders(d.tpOrders);
                                currentDeals.remove( d );
                            } else
                            {
                                d.currentLotSize = p.positionAmt;
                            }
                        }*/
                    }
                }
            }
        } catch (Exception e) {
        }
    }

    /**  */
    public synchronized void delTpOrSlOrder(String symbol, String cliOrderId)
    {
        Deal deal = getDeal(symbol);
        if (deal == null)
        {
            log.info("WARNING! No deal found for " + symbol);
            return;
        }
        if (deal.slOrders != null && deal.slOrders.size() > 0)
        {
            for (int i=0; i < deal.slOrders.size(); i++)
            {
                BinOrder bo = deal.slOrders.get(i);
                if (cliOrderId.equals(bo.getNewClientOrderId()))
                {
                    deal.slOrders.remove(i);
                    log.info("SL Order " + cliOrderId + " deleted from deal.");
                    if (deal.slOrdersDelayed.size() > 0)
                    {
                        BinOrder newO = deal.slOrdersDelayed.get(0);
                        log.info("Creating delayed SL order @ price " + newO.getPrice());
                        SymbolInfo sInfo = symbolInfos.get(newO.getSymbol());
                        newO = createLimitOrder(newO.getNewClientOrderId(), newO.getSide(), newO.getQuantity(),
                                newO.getStopPrice(), newO.getSymbol(), sInfo.getDigitsAfterDotPrice(), sInfo.digitsAfterDotLots, newO.getType());
                        if (newO == null)
                            log.info("Error creating :(");
                        else {
                            deal.slOrdersDelayed.remove(0);
                            deal.slOrders.add(newO);
                        }
                    }
                    saveTradeState("delTpOrSlOrder_sl");
                    return;
                }
            }
        }
        if (deal.tpOrders != null && deal.tpOrders.size() > 0)
        {
            for (int i=0; i < deal.tpOrders.size(); i++)
            {
                BinOrder bo = deal.tpOrders.get(i);
                if (cliOrderId.equals(bo.getNewClientOrderId()))
                {
                    deal.tpOrders.remove(i);
                    // TODO: Отправить ТГ сообщение о части профита
                    try {

                        double partPercent = bo.getQuantity() * 100.0 / deal.initialLotSize;
                        double priceMove = Math.abs(bo.getStopPrice() - deal.price);
                        double priceMovePercent = priceMove * 100.0 / deal.price;
                        // Получим процент прибыли от движения (без учета плеч - просто цена * закрытую часть)
                        deal.collectedProfit += priceMovePercent * bo.getQuantity() / deal.initialLotSize;

                        /*
                        tgSender.sendText("\uD83C\uDFAF Profit close " + ((int)partPercent) + "% of <b>" + deal.symbol + "</b> at price "
                                + String.format("%.6f", bo.getStopPrice())
                                + "\r\n\r\n+" + String.format("%.1f", priceMovePercent) + "% price move");
                         */
                    } catch (Exception e) {}

                    log.info("TP Order " + cliOrderId + " deleted from deal.");
                    // TODO: Переместить SL ордер если стоит TRAILING_SL_GRID
                    if (TRAILING_SL_GRID)
                    {
                        if (deal.slOrders.size() > 1)
                        {
                            BinOrder sl0 = deal.slOrders.get(0);
                            BinOrder sl1 = deal.slOrders.get(1);
                            double slGap = Math.abs(sl1.getStopPrice() - sl0.getStopPrice());
                            String side = "";
                            double newSlPrice = sl0.getStopPrice();
                            if (deal.up) {
                                side = "BUY";
                                newSlPrice += slGap;
                            } else {
                                side = "SELL";
                                newSlPrice -= slGap;
                            }
                            String orderId = "SLM_" + side + "_" + ORDER_NUM;
                            ORDER_NUM++;
                            log.info("Moving SL grid of " + symbol + " to price " + newSlPrice);
                            BinOrder newSl = createStopLossOrder(orderId, side, Math.abs(sl0.getQuantity()), newSlPrice, symbol);
                            if (newSl != null)
                            {
                                deal.slOrders.add(0, newSl);
                                // удалим самый дальний ордер вместо которого добавили новый
                                delOrder(deal.slOrders.get(deal.slOrders.size()-1));
                                deal.slOrders.remove(deal.slOrders.size()-1);
                            } else {
                                log.info("WARNING: couldn't add new SL of grid.");
                            }
                            // удалим самый дальний ордер, потому что соотв-й кусочек сделки уже закрыт TP-ордером
                            delOrder(deal.slOrders.get(deal.slOrders.size()-1));
                            deal.slOrders.remove(deal.slOrders.size()-1);
                        }
                    }
                    if (deal.tpOrdersDelayed.size() > 0)
                    {
                        BinOrder newO = deal.tpOrdersDelayed.get(0);
                        log.info("Creating delayed TP order @ price " + newO.getPrice());
                        SymbolInfo sInfo = symbolInfos.get(newO.getSymbol());
                        newO = createLimitOrder(newO.getNewClientOrderId(), newO.getSide(), newO.getQuantity(),
                                newO.getStopPrice(), newO.getSymbol(), sInfo.getDigitsAfterDotPrice(), sInfo.digitsAfterDotLots, newO.getType());
                        if (newO == null)
                            log.info("Error creating :(");
                        else {
                            deal.tpOrdersDelayed.remove(0);
                            deal.tpOrders.add(newO);
                        }
                    }
                    saveTradeState("delTpOrSlOrder_tp");
                    return;
                }
            }
        }
    }
    
    /**  */
    private boolean createDelayedOrderTasks(Deal d, Set<DelayedOrderTaks> delayedOrderTasks) {
        for (DelayedOrderTaks dt : delayedOrderTasks) 
        {
            String type = "TP";
            if (dt.clientId.startsWith("SL"))
                type = "SL";
            String newOrderId = type + "_" + ORDER_NUM;
            BinOrder o = createLimitOrder(newOrderId, dt.mainOrderSide, dt.qty, dt.price, dt.symbol,
                    dt.getDigitsAfterDotPrice, dt.getDigitsAfterDotLots, dt.type);

            if (o == null) {
                log.error("XXX " + dt.symbol + " ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ SL TP DIDn't PLACE! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            } else if (o.getCreateErrCode() == BinOrder.CREATE_ERROR_WOULD_TRIGGER_IMIDETLY)
            {
                log.info("Closing deal as price gone out of TP or SL limits...");
                // TODO: нет! переделать - на самом деле цена не хорошая!
                return false;
            } else
            {
                log.info("ORDER ERR CODE: " + o.getCreateErrCode());
                if (dt.clientId.startsWith("SL")) {
                    d.slOrders.add(o);
                } else if (dt.clientId.startsWith("TP")) {
                    d.tpOrders.add(o);
                }
            }
            ORDER_NUM++;
        }
        return true;
    }
    
    /**  */
    private synchronized void removeDealFromPack(Deal d) {
        try {
            for (int i = 0; i < dealPacks.size(); i++) {
                DealPack dp = dealPacks.get(i);
                synchronized (dp.deals) {
                    dp.deals.remove(d);
                }
            }
        } catch (Throwable e)
        {
            log.info("ERROR! ", e);
        }
    }  
    
    /**  */
    public synchronized boolean delOrder(BinOrder bo)
    {
        try {
            binancer.delOrder( bo );
            return true;
        } catch (Exception e) {
            log.error("can't kill sl order", e);
            return false;
        }
    }
    
    public boolean closeDeal(SymbolInfo si)
    {
        Deal d = this.getDeal(si);
        if (d != null)
            return closeDeal(d, false);
        else
        {
            log.info("Can't find deal to close with symbol: " + si.symbol);
            return false;
        }
    }
    
    public boolean closeDeal(String symbol)
    {
        Deal d = this.getDeal(symbol);
        if (d != null)
            return closeDeal(d, false);
        else
        {
            log.info("Can't find deal to close with symbol: " + symbol);
            return false;
        }
    }

    public void closeDealIfDirection(String symbol, boolean up)
    {
        DealPack dealPack = getAnyDealPack();

        if (dealPack == null)
        {
            log.info("No dealPack");
            return;
        }

        Deal deal = getDeal(symbol);
        if (deal != null && deal.up == up) {
            closeDeal( deal, false );
        }
    }

    List<BinOrder> suborders2close = new ArrayList<>();
    public void killSubOrders()
    {
        if (suborders2close.size() > 0)
            delOrders(suborders2close);
    }

    /** Закрывает позицию открывая маркет-оредр reduceOnly в противоположном направлении */    
    public synchronized boolean closeDeal(Deal deal, boolean closeSubOrdersLater)
    {
        try {
            try {
                double absoluteProfit = deal.getAbsoluteProfit();
                String buyStr = "buy";
                if (!deal.up)
                    buyStr = "sell";
                String okMark = "\uD83C\uDFC6";
                String profitStr = "Profit";
                if (absoluteProfit < 0)
                {
                    okMark = "\uD83D\uDCDD";
                    profitStr = "Loss";
                }
                /*
                DecimalFormat df = new DecimalFormat("0.######");
                String message = "Closing " + buyStr + " <b>" + deal.symbol + "</b> trade @ "
                        + df.format(deal.latestPrice)
                        + "\r\n\r\n" + okMark + " " + profitStr + ": <b>" + String.format("%.1f", absoluteProfit) + "%</b> (leverage 1:1)";
                tgSender.sendText(message); */
            } catch (Throwable t) {t.printStackTrace();}

            if (deal.currentLotSize == 0)
                return false;
            SymbolInfo sInfo = symbolInfos.get( deal.symbol );
            String side = !deal.up ? BinOrder.SIDE_BUY : BinOrder.SIDE_SELL;            
            BinOrder bo = new BinOrder(side, BinOrder.TYPE_MARKET);
            bo.setSymbol(deal.symbol);
            bo.setQuantity( Math.abs(deal.currentLotSize) );
            //bo.setPositionSide(BinOrder.SIDE_BUY.equals(side) ? "LONG" : "SHORT");
            bo.setReduceOnly( true );
            
            boolean needClose = true;
            /*
            if (bo.getQuantity() < 0.000000001)
            { // реально не было ничего куплено - ждем заполнения лимитного..
                needClose = false;
            }*/
            
            BinOrder mainOrder = needClose ? this.binancer.sendOrder(bo, sInfo.getDigitsAfterDotPrice(), sInfo.digitsAfterDotLots)
                    : null;
            if (needClose && (mainOrder == null || !"FILLED".equals(mainOrder.getStatus())))
            {
                log.info("Order was'nt filled :( " + mainOrder);                
            } else
            {
                log.info("DEAL should be closed!");
                
                // Мы уже сделали все что нужно в связи с закрытием сделки - запомним это, чтобы не удалять 
                // сделку еще раз, когда прилетит апдейт позиции.
                

                if (!closeSubOrdersLater) {
                    delOrders(deal.slOrders);
                    delOrders(deal.tpOrders);
                    delOrders(deal.openOrders);
                } else
                {
                    if (deal.slOrders != null)
                        suborders2close.addAll(deal.slOrders);
                    if (deal.tpOrders != null)
                        suborders2close.addAll(deal.tpOrders);
                    if (deal.openOrders != null)
                        suborders2close.addAll(deal.openOrders);
                }
                removeDealFromPack( deal );
                saveTradeState("closeDeal");
                return true;
            }
        } catch (Exception e)
        {
            log.error("Can't FORCE close cuu deal", e);
        }
        return false;
    }
    
    /** Закрывает позицию открывая маркет-оредр reduceOnly в противоположном направлении */    
    public boolean closePartOfDeal(Deal deal, double qty)
    {
        try {
            SymbolInfo sInfo = symbolInfos.get( deal.symbol );
            String side = !deal.up ? BinOrder.SIDE_BUY : BinOrder.SIDE_SELL;            
            BinOrder bo = new BinOrder(side, BinOrder.TYPE_MARKET);
            bo.setSymbol(deal.symbol);
            bo.setQuantity( Math.abs( qty ) );
            //bo.setPositionSide(BinOrder.SIDE_BUY.equals(side) ? "LONG" : "SHORT");
            bo.setReduceOnly( true ); // CAn't be used in HEDGE mode.
            
            BinOrder mainOrder = this.binancer.sendOrder(bo, sInfo.getDigitsAfterDotPrice(), sInfo.digitsAfterDotLots);
            if (mainOrder == null || !"FILLED".equals(mainOrder.getStatus()))
            {
                log.info("Order was'nt filled :( " + mainOrder);                
            } else            
                return true;            
        } catch (Exception e)
        {
            log.error("Can't closePartOfDeal", e);
        }
        return false;
    }
    
    /** 
     Возвращает код ошибки удаления ордера либо 0 - если ордера вообще не было
     */
    private synchronized BinOrder delLimitOrder(List<BinOrder> orders) {
        if (orders != null)
            for (BinOrder o : orders)
            {
                try {
                    return binancer.delOrder( o );
                } catch (Exception e) {
                    log.error("Can't del order " + o, e);
                }
            }
        return null;
    }
    
    /**  */
    public synchronized void delOrders(List<BinOrder> orders) {
        if (orders != null)
            for (BinOrder o : orders)
            {
                try {
                    binancer.delOrder( o );
                } catch (Exception e) {
                    log.error("Can't del order " + o, e);
                }
            }
    }

    public final boolean PROBOY = true;
    public final boolean OTSKOK = false;
    
    // Какой суммой торгуем
    public volatile double MAX_TRADE_AMOUNT_USD = 10_000.0;

    // текущая открытая сделка!
    public volatile Vector<DealPack> dealPacks = new Vector<>();

    public int SB = 0;
    public volatile double MULT_DECREMENTER = 1.0;
    public volatile double TP_COMMS_DECREMENTER = 1.0;
    public static double INITIAL_PROFIT_KOOF = 5.0;
    public volatile long MIN_DELAY_TO_DEAL = 0;
    
    int ORDER_NUM = 0;

    Deal currentDeal = null;
    long lastDealStart = 0;
    
    /**  */
    public double getMaxTradeAmntForSymbol(String symbol)
    {
        SymbolInfo si = symbolInfos.get(symbol);
        if (si == null || si.leverages.size() < 1)
        {
            log.info("Empty leverage info for " + symbol + ", using pos size = " + MAX_TRADE_AMOUNT_USD);
            return MAX_TRADE_AMOUNT_USD;
        }
        return si.leverages.get(0).maxUSDTtrade;
    }
    
    /**  */
    private double getTradeAmount(String symbol, double pnl, double leverage)
    {
        // Если нет сложного процента то всегда торгуем фикс. суммой MAX_TRADE_AMOUNT_USD
        if (!autoChangeAmount)
            return MAX_TRADE_AMOUNT_USD / MAX_QUANTS;

        /*  Размер сделки одного кванта - это
            - либо BALANCE * leverage      ЕСЛИ   (BALANCE * leverage) * MAX_QUANTS <= макс возможная позиция по монете
            - либо макс. возможная позиция по монете / MAX_QUANTS
         */
        double maxTotalAmnt = getMaxTradeAmntForSymbol( symbol );
        double amnt = BALANCE * leverage;
        if (amnt * MAX_QUANTS < maxTotalAmnt * 0.95)
            return amnt;
        return maxTotalAmnt * 0.95 / MAX_QUANTS;
    }

    /**  */
    private Deal getDealToFreePack(boolean megaSignal)
    {
        if (!megaSignal)
            return null;
        DealPack dealPack = getAnyDealPack();
        for (Deal d : dealPack.deals)
        {
            if (d.getPnlPercent() >= (double) PROFIT_PERCENT_TO_CLOSE)
            {
                log.info("Closing Deal " + d.symbol + " with PNLPecent " + d.getPnlPercent() + " for new Deal..");
                return d;
            }
        }
        return null;
    }

    private int upCnt = 0;
    private int downCnt = 0;

    Map<String, Long> symbolOpenDealTime = new HashMap<>();

    public double BALANCE = 10.0;
    public final boolean FIXED_BALANCE = false;  // Если true, то торгуем всегда BALANCE - т.е. без сложного процента.
    public double currentFullBalance = BALANCE;
    public double CAP = 19.0;     // Close All Profit
    public double CAL = 30000.0;     // Close All Loss
    // сколько максимум кусочков можно докупить
    public int MAX_QUANTS = 2;          // Сколько накопительных закупок в одну сторону можно делать
    public volatile int MAX_DEALS = 14;
    public volatile int MAX_HOURS = 96;  // Макс. время жизни сделки
    public volatile int BUY_SELL_DELTA = 14;
    public boolean REDUCE_ONLY_WITH_PROFIT = false;
    public final long MIN_DEAL_DELAY = 0; // секунд.  - через сколько сек. можно докидывать квантов или закрывать
    public volatile int PROBOY_OTSKOK_DELTA = 30;
    public volatile int PROFIT_PERCENT_TO_CLOSE = 200;  // Это для лимита на общее число сделок
    public volatile int PROFIT_PERCENT_TO_CLOSE_BS_DELTA = 200;   // Это абсолютный проф процент для закрытия по лимиту на отношение B/S
    public boolean TRAILING_SL_GRID = false;
    boolean REVERSE_AFTER_CLOSE = true;
    boolean PLUS_PNL_MODE_PROD = false;
    boolean CLOSE_ALL_PARTS = true;
    public double martingaleMult = 1.0;
    public volatile double TRADE_AMOUNT_USD = BALANCE * this.oneDealLeverage;
    long initTime = System.currentTimeMillis();
    /**  */
    public synchronized Deal createMarketOrder(String symbol, String side, 
            int deltaPretendent, Double latestPrice, int _TP_COMISSIONS, double SL_COMISSIONS, int NO_LOSS_COMISSIONS,
                                               int tpCnt, int tpCntS, double leverage, boolean useTP, boolean useSL,
                                               boolean reduceOnly, boolean megaSignal, boolean proboy)
    {
        int TP_COMISSIONS = (int)((double)(_TP_COMISSIONS) * TP_COMMS_DECREMENTER);

        log.info(" createMarketOrder " + symbol);
        if (!openNewDeals) return null;

        if (System.currentTimeMillis() - initTime < 3 * 60 * 1000L)
        {
            log.info("...slishkom rano");
            return null;
        }
        
        DealPack dealPack = getAnyDealPack();

        if (dealPack == null)
        {
            log.info("No dealPack");
            return null;
        }

        Deal existingDeal = this.getDeal(symbol);
        double pnl = 0;
        if (existingDeal != null) {
            pnl = existingDeal.getPNL();
            if (reduceOnly && REDUCE_ONLY_WITH_PROFIT && pnl < 0)
            {
                log.info("Close " + symbol + " canceled becouse pnl < 0 (" + pnl + ")");
                return null;
            }
            if ((System.currentTimeMillis() - existingDeal.openTime) < MIN_DEAL_DELAY * 1_000L)
            {
                log.info("Deal " + symbol + " change canceled becouse too early");
                return null;
            }
        }

        SymbolInfo sInfo = symbolInfos.get(symbol);
        Double price = latestPrice != null ? latestPrice : sInfo.priceExample;

        boolean decrese = false;
        Double prevQty = null;

        double qtyToAddWhileReverse = 0;
        Deal deal2removeAfterReverse = null;

        if (existingDeal != null) {
            log.info("Found existing deal.");

            /*
            if (existingDeal.proboy != proboy)
            {
                log.info("Existing deal proboy = " + existingDeal.proboy + " buy new signal is proboy = " + proboy + ". Skip...");
                return null;
            }*/

            log.info("Already opened " + existingDeal.quants.size() + " (of MAX " + MAX_QUANTS + ") quants (" + existingDeal.getTotalQty() + "). buy: " + existingDeal.up);

            if (existingDeal.up)
            {
                if (BinOrder.SIDE_BUY.equals(side)) {
                    if (reduceOnly)
                        return null;
                    if (existingDeal.quants.size() >= MAX_QUANTS) {
                        log.info("Max quants, skip");
                        return null;
                    }
                    // ENCREASE POSITION!
                    PriceQtyB lastQuant = existingDeal.getLastQuant();
                    /*if (price > lastQuant.price - lastQuant.price * (double) SB / 1000.0)
                    {
                        log.info("Dont buy as price not lower then lastPrice - SB ("+SB+")");
                        return null;
                    }*/
                    prevQty = existingDeal.getLastQuant().qty.doubleValue();
                } else {
                    // DECREASE or CLOSE pos
                    if (existingDeal.quants.size() == 1 || CLOSE_ALL_PARTS)
                    {
                        log.info("closing last quant (and all deal)");
                        if (REVERSE_AFTER_CLOSE) {
                            if (reduceOnly) {
                                closeDeal(existingDeal, false);
                                return null;
                            }

                            qtyToAddWhileReverse = Math.abs(existingDeal.currentLotSize);
                            deal2removeAfterReverse = existingDeal;
                            deal2removeAfterReverse.openTime = System.currentTimeMillis();
                            // closeDeal(existingDeal, !reduceOnly);
                            existingDeal = null;
                            // Если это не уменьшительная сделака - то мы сразу откроем новую на реверс
                            log.info(" -- REVERSE --");
                        } else {
                            closeDeal(existingDeal, false);
                            return null;
                        }
                    } else
                        decrese = true;
                }
            }
            else if (!existingDeal.up)
            {
                if (BinOrder.SIDE_SELL.equals(side)) {
                    if (reduceOnly)
                        return null;
                    if (existingDeal.quants.size() >= MAX_QUANTS) {
                        log.info("Max quants, skip");
                        return null;
                    }
                    // ENCREASE POSITION!
                    PriceQtyB lastQuant = existingDeal.getLastQuant();
                    /*if (price < lastQuant.price + lastQuant.price * (double) SB / 1000.0)
                    {
                        log.info("Dont sell as price not higher then lastPrice + SB ("+SB+")");
                        return null;
                    }*/
                    prevQty = existingDeal.getLastQuant().qty.doubleValue();
                } else {
                    // DECREASE or CLOSE pos
                    if (existingDeal.quants.size() == 1 || CLOSE_ALL_PARTS) {
                        log.info("closing last quant (and all deal)");
                        if (REVERSE_AFTER_CLOSE) {
                            if (reduceOnly) {
                                closeDeal(existingDeal, false);
                                return null;
                            }
                            qtyToAddWhileReverse = Math.abs(existingDeal.currentLotSize);
                            deal2removeAfterReverse = existingDeal;
                            deal2removeAfterReverse.openTime = System.currentTimeMillis();
                            // closeDeal(existingDeal, !reduceOnly);
                            existingDeal = null;
                            // Если это не уменьшительная сделака - то мы сразу откроем новую на реверс
                            log.info(" -- REVERSE --");
                        } else {
                            closeDeal(existingDeal, false);
                            return null;
                        }
                    } else
                        decrese = true;
                }
            }
        } else {
            // Никакой сделки еще нет
            if (reduceOnly)
                return null;
        }

        double quantity = getTradeAmount( symbol, pnl, leverage) / price;

        // Мартингейл при увеличении позиции!!
        if (prevQty != null) {
            log.info("Martingale: change " + quantity + " to " + (prevQty * martingaleMult));
            quantity = prevQty * martingaleMult;
        }

        double qty2initDeal = quantity; // Это запишем как размер позиции в Deal даже если ордер больше из-за реверса

        if (qtyToAddWhileReverse > 0)
        {
            log.info(" :: In Reverse: adding Qty (" + qtyToAddWhileReverse + ") to calculated new qty ("+quantity+")");
            quantity = quantity + qtyToAddWhileReverse;
        }

        // уберем лишние знаки после запятой
        quantity = new BigDecimal("" + quantity).setScale(sInfo.digitsAfterDotLots, BigDecimal.ROUND_HALF_DOWN).doubleValue();
        qty2initDeal = new BigDecimal("" + qty2initDeal).setScale(sInfo.digitsAfterDotLots, BigDecimal.ROUND_HALF_DOWN).doubleValue();
        double bothSideQty = quantity;
        
        log.info("- ! - " + side + " " + quantity + " of " + symbol + " for " + (quantity * price) + " USD");

        Deal mayCloseDeal = null; // Сделка которую закрыть после открытия (чтобы не было больше MAX_DEALS сделок)
        /////////////////////////////////////////////////////////
        if (existingDeal == null && deal2removeAfterReverse == null)
        {
            // Вначале проверим - стоит ли открывать новую сделку в ЭТОМ направлении, не нарушит ли это баланс (Хедж)

            int bCnt = 0, sCnt = 0;
            int proboyCnt = 0, otskokCnt = 0;
            double PNL = 0;
            Deal upDeal2Close = null;
            Deal downDeal2Close = null;
            for (Deal d : dealPack.deals)
            {
                if (d.up) {
                    bCnt++;
                    if (d.getAbsoluteProfit() > (double) PROFIT_PERCENT_TO_CLOSE_BS_DELTA)
                        upDeal2Close = d;
                } else {
                    sCnt++;
                    if (d.getAbsoluteProfit() > (double) PROFIT_PERCENT_TO_CLOSE_BS_DELTA)
                        downDeal2Close = d;
                }
                if (d.proboy)
                    proboyCnt ++;
                else
                    otskokCnt ++;
                PNL = PNL + d.getPNL();
            }

            if (PLUS_PNL_MODE_PROD) {
                if (PNL < 0) {
                    if (bCnt > sCnt) {
                        if (BinOrder.SIDE_BUY.equals(side)) {
                            log.info("PNL < 0 with buy cnt > sCnt -> skip buy deal");
                            return null;
                        }
                    } else if (bCnt < sCnt) {
                        if (!BinOrder.SIDE_BUY.equals(side)) {
                            log.info("PNL < 0 with buy cnt < sCnt -> skip sell deal");
                            return null;
                        }
                    }
                } else {
                    if (bCnt > sCnt) {
                        if (!BinOrder.SIDE_BUY.equals(side)) {
                            log.info("PNL > 0 with buy cnt > sCnt -> skip sell deal");
                            return null;
                        }
                    } else if (bCnt < sCnt) {
                        if (BinOrder.SIDE_BUY.equals(side)) {
                            log.info("PNL > 0 with buy cnt < sCnt -> skip buy deal");
                            return null;
                        }
                    }
                }
            } else {
                if (BinOrder.SIDE_BUY.equals(side) && bCnt - sCnt > BUY_SELL_DELTA)
                {
                    if (upDeal2Close == null) {
                        log.info("Skip Deal as BUY_SELL DELTA (" + BUY_SELL_DELTA + ") reached.");
                        return null;
                    }
                    mayCloseDeal = upDeal2Close;
                    log.info("Close deal " + mayCloseDeal.symbol + " to free buy-sell delta limit.");
                }
                if (BinOrder.SIDE_SELL.equals(side) && sCnt - bCnt > BUY_SELL_DELTA)
                {
                    if (downDeal2Close == null) {
                        log.info("Skip Deal as BUY_SELL DELTA (" + BUY_SELL_DELTA + ") reached.");
                        return null;
                    }
                    mayCloseDeal = downDeal2Close;
                    log.info("Close deal " + mayCloseDeal.symbol + " to free buy-sell delta limit.");
                }
                // Мы тут - значит Хедж - BUY_SELL_DELTA не нарушен - можно открыть сделку
                if (proboy) {
                    if (proboyCnt - otskokCnt > PROBOY_OTSKOK_DELTA)
                        return null;
                } else if (otskokCnt - proboyCnt > PROBOY_OTSKOK_DELTA)
                    return null;
            }
            // check MAX_DEALS
            if (dealPack.deals.size() >= MAX_DEALS)
            {
                mayCloseDeal = getDealToFreePack(megaSignal);
                if (mayCloseDeal == null) {
                    log.info("Already opened " + dealPack.deals.size() + " deals. Skip new one...");
                    return null;
                } else {
                    // Можно открыть новую сделку взамен mayCloseDeal которую надо будет закррыть сращу после успешного открытия новой
                }
            }
        }
        /////////////////////////////////////////////////////////
        
        try {
            if (decrese)
                bothSideQty = existingDeal.getLastQuant().qty.doubleValue();

            BinOrder bo = LIMIT_TRADES  ? new BinOrder(side, BinOrder.TYPE_LIMIT) 
                                        : new BinOrder(side, BinOrder.TYPE_MARKET);
            
            bo.setSymbol(symbol);
            bo.setQuantity( bothSideQty );                    
            if (LIMIT_TRADES)
            {
                bo.setTimeInForce( BinOrder.TIM_IN_FRC_GOOD_TILL_CANCEL);
                bo.setPrice( latestPrice );
                bo.setNewClientOrderId( "OPEN_" + ORDER_NUM );
                ORDER_NUM++;
            }
            if (decrese)
                bo.setReduceOnly( true );

            symbolOpenDealTime.put(symbol,new Long(System.currentTimeMillis()));
            BinOrder mainOrder = binancer.sendOrder(bo, sInfo.getDigitsAfterDotPrice(), sInfo.digitsAfterDotLots);
            if (mainOrder == null || mainOrder.getStatus() == null 
                    || (!"FILLED".equals(mainOrder.getStatus()) && !LIMIT_TRADES))
            {
                log.info("Order wasn't filled :( " + mainOrder);
                return null;
            }
            // Раз мы здесь - ордер исполнен и можно удалить лимитки к старой позиции
            if (deal2removeAfterReverse != null)
            {
                removeDealFromPack( deal2removeAfterReverse );
                delOrders( deal2removeAfterReverse.slOrders );
                delOrders( deal2removeAfterReverse.tpOrders );
                delOrders( deal2removeAfterReverse.openOrders );
            }
            killSubOrders();

            if (mayCloseDeal != null)
                closeDeal(mayCloseDeal, false);

            boolean filled = "FILLED".equalsIgnoreCase(mainOrder.getStatus());
            log.info("Order executed " + mainOrder);
            
            lastDealStart = System.currentTimeMillis();
            
            double realDealPrice = LIMIT_TRADES ? latestPrice : mainOrder.getPrice();   // она поменялась на реальную цену сделки..
            log.info("realDealPrice: " + realDealPrice);

            double TP_SL_GRANULA = realDealPrice * 0.001;

            double qty4TP = quantity;
            if (existingDeal != null && !decrese)
            {
                existingDeal.openTime = System.currentTimeMillis();
                qty4TP = qty4TP + existingDeal.getTotalQty().doubleValue();
                qty4TP = new BigDecimal("" + qty4TP).setScale(sInfo.digitsAfterDotLots, BigDecimal.ROUND_HALF_DOWN).doubleValue();
                log.info("Encreasing exising deal: new PT qty: " + qty4TP);
            }
            
            /* 
            // старый вариант до круглых уровней
            double TP_MIN_OFFSET = _distanceToSlInComissions * 1.0; // 1.7 was ok 
            double TP_MAX_OFFSET = _distanceToSlInComissions * 1.0; // 2.7 was ok
            double SL_MIN_OFFSET = _distanceToSlInComissions * 1.0;
            double SL_MAX_OFFSET = _distanceToSlInComissions * 1.0;
            if (!proboy)
            { // торгуем отскок
                TP_COUNT = 1;
                SL_COUNT = 1;
                TP_MIN_OFFSET = _distanceToSlInComissions * 1.0;
                TP_MAX_OFFSET = _distanceToSlInComissions * 1.0;
                SL_MIN_OFFSET = _distanceToSlInComissions * 1.0;
                SL_MAX_OFFSET = _distanceToSlInComissions * 1.0;
            }
            */
            
            int TP_COUNT = tpCnt;
            int SL_COUNT = 1; //TP_COUNT; // 1

      //      double SL_MIN_OFFSET = SL_COMISSIONS;
        //    double SL_MAX_OFFSET = SL_COMISSIONS;

            double tpQuantity = qty4TP / TP_COUNT;
            double slQuantity = qty4TP / SL_COUNT;

            Double SL_NO_LOSS_TRIGGER_PRICE = null;
            
            if (tpQuantity < sInfo.minQty)
            {
                log.info(" -- ONLY 1 TP as this coin has large min lot");
                tpQuantity = qty4TP;
                TP_COUNT = 1;
            }
            if (slQuantity < sInfo.minQty)
            {
                log.info(" -- ONLY 1 SL as this coin has large min lot");
                slQuantity = qty4TP;
                SL_COUNT = 1;                
            }
                
            if (tpQuantity < sInfo.minQty)
            {
                tpQuantity = sInfo.minQty;
            } else
            {
                tpQuantity = new BigDecimal("" + tpQuantity).setScale(sInfo.digitsAfterDotLots, BigDecimal.ROUND_HALF_DOWN).doubleValue();
            }
            if (slQuantity < sInfo.minQty)
            {
                slQuantity = sInfo.minQty;
            } else
            {
                slQuantity = new BigDecimal("" + slQuantity).setScale(sInfo.digitsAfterDotLots, BigDecimal.ROUND_HALF_DOWN).doubleValue();
            }
            double lastTpQuantity = qty4TP - tpQuantity * (TP_COUNT-1);
            lastTpQuantity = new BigDecimal("" + lastTpQuantity).setScale(sInfo.digitsAfterDotLots, BigDecimal.ROUND_HALF_DOWN).doubleValue();
            
            double lastSlQuantity = qty4TP - slQuantity * (SL_COUNT-1);
            lastSlQuantity = new BigDecimal("" + lastSlQuantity).setScale(sInfo.digitsAfterDotLots, BigDecimal.ROUND_HALF_DOWN).doubleValue();

            
            // ============================================= ВЫЧ  TP  ВЫЧ  TP   ВЫЧ  TP   ВЫЧ  TP   ВЫЧ  TP  ВЫЧ  TP ======================================
            List<PriceQty> tps = new ArrayList<>();
            List<PriceQty> sls = new ArrayList<>();

            double _realDealPrice = realDealPrice;
            if (existingDeal != null)
                _realDealPrice = existingDeal.getAvgPrice(realDealPrice);

            if (SL_COUNT < 2)
            {
                log.info("-- prepare 1 SL");
                double slPrice;
                if (BinOrder.SIDE_BUY.equals(side)) {

                    if (SL_COMISSIONS > 0) {
                        slPrice = /* _realDealPrice */ latestPrice - TP_SL_GRANULA * SL_COMISSIONS;
                    } else
                        slPrice = latestPrice - sInfo.tickSize;

                    if (NO_LOSS_COMISSIONS > 0)
                        SL_NO_LOSS_TRIGGER_PRICE = _realDealPrice + TP_SL_GRANULA * NO_LOSS_COMISSIONS;
                    else
                        SL_NO_LOSS_TRIGGER_PRICE = null;
                } else {
                    if (SL_COMISSIONS > 0)
                        slPrice = /* _realDealPrice */ latestPrice + TP_SL_GRANULA * SL_COMISSIONS;
                    else
                        slPrice = latestPrice + sInfo.tickSize;
                    if (NO_LOSS_COMISSIONS > 0)
                        SL_NO_LOSS_TRIGGER_PRICE = _realDealPrice - TP_SL_GRANULA * NO_LOSS_COMISSIONS;
                    else
                        SL_NO_LOSS_TRIGGER_PRICE = null;
                }
                PriceQty pq = new PriceQty(slPrice, lastSlQuantity);
                sls.add(pq);
            } else
            { // можно наделать много SL
                double startPos = (double)SL_COMISSIONS / (double) tpCntS;
                double slStep = ((double)SL_COMISSIONS - startPos) / (double) (SL_COUNT - 1);
                double _slPart = 0;

                log.info("-- prepare " + SL_COUNT + " SL");
                if (BinOrder.SIDE_SELL.equals(side)) {
                    for (int i = 0; i < SL_COUNT; i++) {
                        double _slLevel = _realDealPrice + TP_SL_GRANULA * (startPos + _slPart);
                        double qty = (i == SL_COUNT - 1) ? lastSlQuantity : slQuantity;
                        PriceQty pq = new PriceQty( _slLevel, qty );
                        sls.add(pq);
                        _slPart += slStep;
                    }
                } else {
                    for (int i = 0; i < SL_COUNT; i++) {
                        double _slLevel = _realDealPrice - TP_SL_GRANULA * (startPos + _slPart);
                        double qty = (i == SL_COUNT - 1) ? lastSlQuantity : slQuantity;
                        PriceQty pq = new PriceQty( _slLevel, qty );
                        sls.add(pq);
                        _slPart += slStep;
                    }
                }
            }
            
            if (TP_COUNT < 2)
            {
                log.info("-- prepare 1 TP");
                double tpPrice;                
                if (BinOrder.SIDE_BUY.equals(side)) {
                    tpPrice = _realDealPrice /* latestPrice */ + TP_SL_GRANULA * TP_COMISSIONS;
                } else {
                    tpPrice = _realDealPrice /* latestPrice */- TP_SL_GRANULA * TP_COMISSIONS;
                }
                PriceQty pq = new PriceQty(tpPrice, lastTpQuantity);
                tps.add(pq);
            } else
            { // можно наделать много TP
                double startPos = (double)TP_COMISSIONS / (double) tpCntS;
                double tpStep = ((double)TP_COMISSIONS - startPos) / (double) (TP_COUNT - 1);
                double _tpPart = 0;

                log.info("-- prepare " + TP_COUNT + " TP");
                if (BinOrder.SIDE_BUY.equals(side)) {
                    for (int i = 0; i < TP_COUNT; i++) {
                        double _tpLevel = _realDealPrice + TP_SL_GRANULA * (startPos + _tpPart);
                        double qty = (i == TP_COUNT - 1) ? lastTpQuantity : tpQuantity;
                        PriceQty pq = new PriceQty( _tpLevel, qty );
                        tps.add(pq);
                        _tpPart += tpStep;
                    }                    
                } else {
                    for (int i = 0; i < TP_COUNT; i++) {
                        double _tpLevel = _realDealPrice - TP_SL_GRANULA * (startPos + _tpPart);
                        double qty = (i == TP_COUNT - 1) ? lastTpQuantity : tpQuantity;
                        PriceQty pq = new PriceQty( _tpLevel, qty );
                        tps.add(pq);
                        _tpPart += tpStep;
                    }                    
                }
            }
            
            // Удаляем старые ТП и СЛ в случае если это не уменьшение позиции. Чтобы создать новые
            // в соответствии с новой средней ценой
            if (existingDeal != null && !decrese)
            {
                log.info("XXX " + symbol + " CORRECT POSITION kill all old orders...");
                delOrders(existingDeal.slOrders);
                delOrders(existingDeal.tpOrders);
                existingDeal.noLossSet = false;
            }
            
            // ============================================= ВЫЧ  TP  ВЫЧ  TP   ВЫЧ  TP   ВЫЧ  TP   ВЫЧ  TP  ВЫЧ  TP ======================================
            List<BinOrder> slOrders = new ArrayList<>();
            Set<DelayedOrderTaks> delayedOrderTasks = new HashSet<>();
            Set<DelayedOrderTaks> oldDelayedOrderTasks = new HashSet<>();
            List<BinOrder> slOrdersDelayed = new ArrayList<>();
            List<BinOrder> tpOrdersDelayed = new ArrayList<>();
            if (useSL && !decrese) {
                for (int i = 0; i < sls.size(); i++) {
                    PriceQty pq = sls.get(i);
                    if (!LIMIT_TRADES   // если рыночные ордера
                            || filled       // если лимитные орддера и нужен обычный СЛ в обратную сторону при закупке  
                    ) {
                        BinOrder slOrder = null;
                        if (i < 5)
                            slOrder = createLimitOrder("SL_" + side + "_" + ORDER_NUM, side, pq.qty, pq.price, symbol, sInfo.getDigitsAfterDotPrice(), sInfo.digitsAfterDotLots, BinOrder.STOP_LOSS);
                        if (slOrder == null) {
                            slOrder = new BinOrder();
                            slOrder.setSymbol(symbol);
                            slOrder.setSide(side);
                            slOrder.setQuantity( pq.qty);
                            slOrder.setNewClientOrderId("SLX_" + side + "_" + ORDER_NUM);
                            slOrder.setStopPrice( pq.price );
                            slOrder.setType(BinOrder.STOP_LOSS);
                            slOrdersDelayed.add(slOrder);
                            log.error("XXX " + symbol + " ~ SL " + i + " DIDn't PLACE! ~~~");
                        } else
                            slOrders.add(slOrder);
                        ORDER_NUM++;
                    } else {
                        DelayedOrderTaks dot = new DelayedOrderTaks("SL_" + side + "_" + ORDER_NUM, mainOrder.getSide(), pq.qty, pq.price, symbol, sInfo.getDigitsAfterDotPrice(), sInfo.digitsAfterDotLots, BinOrder.STOP_LOSS);
                        delayedOrderTasks.add(dot);
                        ORDER_NUM++;
                    }
                }
            } else if (useSL && existingDeal != null)
            {
                slOrders = existingDeal.slOrders;
                slOrdersDelayed = existingDeal.slOrdersDelayed;
                log.info("SL Orders where saved from old existing deal without changes as this is decreasing..");
            }
            
            List<BinOrder> tpOrders = new ArrayList<>();
            if (useTP && !decrese) {
                // Увеличение позиции + мы вообще используем ТП - надо все пересоздать
                for (int i = 0; i < tps.size(); i++) {
                    PriceQty pq = tps.get(i);


                    if (!LIMIT_TRADES || filled) {
                        BinOrder tpOrder = null;
                        if (i < 9)
                            tpOrder = createLimitOrder("TP_" + side + "_" + ORDER_NUM, side, pq.qty, pq.price, symbol, sInfo.getDigitsAfterDotPrice(), sInfo.digitsAfterDotLots, BinOrder.TAKE_PROFIT);
                        if (tpOrder == null) {
                            tpOrder = new BinOrder();
                            tpOrder.setSymbol(symbol);
                            tpOrder.setSide(side);
                            tpOrder.setQuantity( pq.qty);
                            tpOrder.setNewClientOrderId("TPX_" + side + "_" + ORDER_NUM);
                            tpOrder.setStopPrice( pq.price );
                            tpOrder.setType(BinOrder.TAKE_PROFIT);
                            tpOrdersDelayed.add(tpOrder);
                            log.error("XXX " + symbol + " ~ TP " + i + " DIDn't PLACE! ~~~");
                        } else
                            tpOrders.add(tpOrder);
                        ORDER_NUM++;
                    } else {
                        DelayedOrderTaks dot = new DelayedOrderTaks("TP_" + side + "_" + ORDER_NUM, mainOrder.getSide(), pq.qty, pq.price, symbol, sInfo.getDigitsAfterDotPrice(), sInfo.digitsAfterDotLots, BinOrder.TAKE_PROFIT);
                        delayedOrderTasks.add(dot);
                        ORDER_NUM++;
                    }
                }
            } else if (useTP && existingDeal != null)
            {
                tpOrders = existingDeal.tpOrders;
                tpOrdersDelayed = existingDeal.tpOrdersDelayed;
                log.info("TP Orders where saved from old existing deal without changes as this is decreasing..");
            }
            
            Deal d = LIMIT_TRADES ?
                    new Deal(this, symbol, realDealPrice, BinOrder.SIDE_BUY.equals(side), 
                            !filled ? 0 : quantity, mainOrder, slOrders, tpOrders, TP_SL_GRANULA,
                    false // убрать этот false чтобы работал отлов быстрых разворотов при негативном сценарии 
                    , TP_SL_GRANULA * SL_COMISSIONS, 0, sInfo.getKrugloe().doubleValue(), SL_NO_LOSS_TRIGGER_PRICE, quantity,
                    BinOrder.SIDE_BUY.equals(side))
                    :
                    new Deal(this, symbol, realDealPrice, BinOrder.SIDE_BUY.equals(side),
                            qty2initDeal, mainOrder, slOrders, tpOrders, TP_SL_GRANULA,
                    false // убрать этот false чтобы работал отлов быстрых разворотов при негативном сценарии 
                    , TP_SL_GRANULA * SL_COMISSIONS, 0, sInfo.getKrugloe().doubleValue(), SL_NO_LOSS_TRIGGER_PRICE, qty2initDeal,
                    BinOrder.SIDE_BUY.equals(side));
            /// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ВСЕГДА МЕТИМ КАК ПРОБОй - т.о. ограничиваем число пробоев, но не ограничиваем число отскоков
            d.proboy = true; // proboy;
            
            if (LIMIT_TRADES && !filled)
                d.openOrders.add( mainOrder );
                
            d.delayedOrderTasks = delayedOrderTasks;
            if (filled && oldDelayedOrderTasks.size() > 0)
                d.oldDelayedOrderTasks = oldDelayedOrderTasks;
            
            if (existingDeal == null)
                existingDeal = d;

            existingDeal.openTime = System.currentTimeMillis();

            { // подправить existingDeal информацией о том, что мы хотим перевернуться...
                existingDeal.zeroQtySetTime = Long.MAX_VALUE;
                if (!decrese) {
                    log.info("TP SL orders list replaced or set for new trade");
                    // заменяем список ТП ордеров только в случае увеличения позиции т.к. ТП был пересоздан
                    existingDeal.tpOrders = tpOrders;
                    existingDeal.slOrders = slOrders;
                    existingDeal.slOrdersDelayed = slOrdersDelayed;
                    existingDeal.tpOrdersDelayed = tpOrdersDelayed;
                } else {
                    log.info("TP SL orders wasn't replaced as this trade just decreased");
                }
                //existingDeal.slOrders = slOrders;
                existingDeal.delayedOrderTasks = delayedOrderTasks;
                if (filled && oldDelayedOrderTasks.size() > 0)
                    existingDeal.oldDelayedOrderTasks = oldDelayedOrderTasks;
                //   existingDeal.targetUp = BinOrder.SIDE_BUY.equals(side);
                if (decrese)
                {
                    existingDeal.removeLastQuant();
                } else
                {
                    existingDeal.addQuant(new PriceQtyB(realDealPrice, new BigDecimal(qty2initDeal)));
                }

                // existingDeal.price = realDealPrice;             // TODO: цену надо высчитать как среднее по кускам !
                // операции с квантами пересчитывают цену автоматом

              //  existingDeal.noLossSet = false;
                existingDeal.slNoLossTriggerPrice = SL_NO_LOSS_TRIGGER_PRICE;
                if (filled)
                {
                    existingDeal.up = existingDeal.targetUp;
                    existingDeal.currentLotSize = existingDeal.getTotalQty().doubleValue();
           //         existingDeal.targetQty = sumQty;
                } else
                {
                    existingDeal.openOrders.add( mainOrder );
             //       existingDeal.targetQty = sumQty;
                }
            }
      
            if (false && slOrders.size() < 1)
            {
                log.error("XXX SL DIDn't PLACE! " + symbol);
                this.closeDeal(existingDeal, false);
                return null;
            } else
            {
                synchronized (dealPack.deals)
                {
                    dealPack.deals.remove( existingDeal );
                    dealPack.deals.add( existingDeal );
                    if (dealPack.isEmpty())
                    {
                        dealPack.setProfitKoof( this.INITIAL_PROFIT_KOOF );
                    }        
                    if (dealPack.isFull())
                    {
                        dealPack.setFinishCollectTime();
                        dealPack.chargeNextProfKoofDecrementTime();
                        OPEN_NEXT_DEAL_PACK_AFTER_TIME = System.currentTimeMillis() + 60_000L;
                    }
                }
                saveTradeState("createMarketOrder");
                return existingDeal;
            }
        } catch (Exception e)
        {
            log.error("Can't " + side + " " + symbol, e);
            return null;
        }        
    }
    
    private long OPEN_NEXT_DEAL_PACK_AFTER_TIME = 0;
    
    /**  */
    private synchronized DealPack getActiveDealPack()
    {
        DealPack newPack = null;
        for (int i=0; i < dealPacks.size(); i++)
        {
            DealPack dp = dealPacks.get(i);
            if (!dp.isFull() && !dp.isEmpty())
                return dp;
            if (dp.isEmpty() && System.currentTimeMillis() > OPEN_NEXT_DEAL_PACK_AFTER_TIME)
                newPack = dp;
        }
        return newPack;
    }
    
    /**  */
    private synchronized DealPack getAnyDealPack()
    {
        if (dealPacks.size() > 0)
            return dealPacks.get(0);
        return null;
    }
    
    /**  */
    public synchronized void setMaxDealsInPackage(int maxDeals)
    {
        this.MAX_DEALS = maxDeals; 
        for (int i=0; i < dealPacks.size(); i++)
        {
            DealPack dp = dealPacks.get(i);
            dp.MAX_DEALS = maxDeals;
        }
    }
    
    /**  */
    public synchronized void setProfitKoof(double profitKoof)
    {
        this.INITIAL_PROFIT_KOOF = profitKoof;
        for (int i=0; i < dealPacks.size(); i++)
        {
            DealPack dp = dealPacks.get(i);
            dp.setProfitKoof(profitKoof);
        }
    }
    
    public double getProfitKoof()
    {
        return INITIAL_PROFIT_KOOF;
    }
    
    /*
        Вернет либо тот обхем что на входе дан, но убрав лишние знаки после запятой.
        Либо вернет минимально-возможный объем по инструменту - если тот на входе был меньше
    */
    public double getNormalizedQty(String symbol, double qty)
    {
        SymbolInfo sInfo = symbolInfos.get(symbol);
        if (qty < sInfo.minQty)
            return sInfo.minQty;
        return new BigDecimal("" + qty).setScale(sInfo.digitsAfterDotLots, BigDecimal.ROUND_HALF_DOWN).doubleValue();
    }
    
    /**  */
    public BinOrder createStopLossOrder(String cliOrderId, String mainPosSdie, double lots, double stopPrice, String symbol)
    {
        SymbolInfo sInfo = symbolInfos.get( symbol );
        return createLimitOrder(cliOrderId, mainPosSdie, lots, stopPrice, symbol, sInfo.getDigitsAfterDotPrice(), sInfo.digitsAfterDotLots, BinOrder.STOP_LOSS);
    }
    
    /**  */
    public BinOrder createTPOrder(String cliOrderId, String mainPosSdie, double lots, double stopPrice, String symbol)
    {
        SymbolInfo sInfo = symbolInfos.get( symbol );
        return createLimitOrder(cliOrderId, mainPosSdie, lots, stopPrice, symbol, sInfo.getDigitsAfterDotPrice(), sInfo.digitsAfterDotLots, BinOrder.TAKE_PROFIT);
    }
    
    /**  */
    private BinOrder createLimitOrder(String cliOrderId, String mainPosSdie, double lots, double stopPrice, String symbol, int digitsAfterDotPrice, int digitsAfterDotLots, String type)
    {
        String side = BinOrder.SIDE_BUY.equals( mainPosSdie ) ? BinOrder.SIDE_SELL : BinOrder.SIDE_BUY;        
        BinOrder bo = new BinOrder(side, type);
        bo.setSymbol( symbol );
        bo.setTimeInForce( BinOrder.TIM_IN_FRC_GOOD_TILL_CANCEL);
        //bo.setPositionSide( /*"BOTH"*/  BinOrder.SIDE_BUY.equals( mainPosSdie )?"LONG":"SHORT"  );
        
        bo.setPrice( stopPrice );
        bo.setStopPrice( stopPrice );
        bo.setQuantity( Math.abs(lots) );
        bo.setNewClientOrderId( cliOrderId );        
        bo.setReduceOnly( true );
        
        log.info("Composed order " + bo);
        return this.binancer.sendOrder(bo, digitsAfterDotPrice, digitsAfterDotLots);
    }

    
    /**   */
    public Stakan getStakan(String symbol)
    {
        Stakan s = stakanBySymbol.get( symbol );
        if (s == null)
        {
            s = new Stakan(symbol);
            stakanBySymbol.put(symbol, s);
        }
        return s;
    }
    
    // =========================================================================
    //  EXPERIMENTAL METHODS
    // =========================================================================
    
    
    /*
        Возвращает процент реально проторгованного уровня по всем претендентам.. от 0 до 1
    */
    /*private DoublePair getAllPretendentsRealTradePercent()
    {
        long curTime = System.currentTimeMillis();
        
        double etalonQty = -1.0;
        
        double allLevelsQty = 0.0;
        double allLevelsTradedQtyB = 0.0;
        double allLevelsTradedQtyS = 0.0;
        
        for (String marketSymbol : pretendentSymbols.keySet())
        {
            // marketSymbol это пара типа F_ETHUSDT { market + "_" + symbol }
            String symbol = marketSymbol.substring(2);
            boolean futures = marketSymbol.startsWith("F") ? true : false;            
            long levelLifetime = curTime - levelAppearTime.get( marketSymbol );
            StakanContainer stakanCont = getStakanContainerBySymbol(symbol, futures);
            double levelQty = stakanCont.maxQtyAtLevel;
            DoublePair tradedVolumesBS = getLastNsecVolumes(stakanCont.symbol, levelLifetime, futures);
            if (etalonQty < 0)
                etalonQty = levelQty;
            
            double proportion = levelQty / etalonQty;
            
            allLevelsQty = allLevelsQty + levelQty * proportion;
            allLevelsTradedQtyB = allLevelsTradedQtyB + tradedVolumesBS.buy * proportion;
            allLevelsTradedQtyS = allLevelsTradedQtyS + tradedVolumesBS.sell * proportion;
        }        
        DoublePair rv = new DoublePair();        
        if (allLevelsQty > 0.0)
        {
            rv.buy = allLevelsTradedQtyB / allLevelsQty;
            rv.sell = allLevelsTradedQtyS / allLevelsQty;
        }        
        return rv;
    }*/
    
    public static boolean comparePrices(Double p1, Double p2)
    {
        if (p1 == null && p2 == null)
            return true;
        if (p1 == null || p2 == null)
            return false;
        if (Math.abs(p1.doubleValue() - p2.doubleValue()) < 0.00000001)
            return true;
        return false;
    }
}

class RSI {
    
    public double RSI_PER;
    private double EMA_ALPHA;
    
    double prevD_AVG = 0;
    double prevU_AVG = 0;
    
    double prev_prev_D_AVG = 0;
    double prev_prev_U_AVG = 0;
    
    double RSI = 50;
    double prevRSI = 50;
    
    double prevPrice = -1;
    double prev_prev_price = -1;

    int addCnt = 0;

    public RSI(int per)
    {
        this.RSI_PER = per;
        this.EMA_ALPHA = 2d / (1d + RSI_PER);
    }
    
    public double getAvg()
    {
        return RSI;
    }
    
    public double getPrevAvg()
    {
        return prevRSI;
    }
    
    private boolean bar1 = true;
    public void addStat(double val)
    {
        if (addCnt < RSI_PER)
            addCnt++;

        double price = val;
        
        if (bar1)
        {
            prevPrice = prev_prev_price;
            prevD_AVG = prev_prev_D_AVG;
            prevU_AVG = prev_prev_U_AVG;
            bar1 = false;
        } else
        {
            prevRSI = RSI;
        }
        
        if (!bar1)
        { // Это не самая первая свеча
            double RSI_U = 0;
            double RSI_D = 0;
            
            if (price > prevPrice) {
                RSI_U = price - prevPrice;
            } else if (price < prevPrice) {
                RSI_D = prevPrice - price;
            }
            double RSI_D_AVG = prevD_AVG - EMA_ALPHA * (prevD_AVG - RSI_D);
            double RSI_U_AVG = prevU_AVG - EMA_ALPHA * (prevU_AVG - RSI_U);
            if (RSI_D_AVG == 0) {
                RSI = 50;
            } else {
                double RS = RSI_U_AVG / RSI_D_AVG;
                RSI = 100 - 100 / (1 + RS);
            }
            prev_prev_D_AVG = prevD_AVG;
            prev_prev_U_AVG = prevU_AVG;
            prevD_AVG = RSI_D_AVG;
            prevU_AVG = RSI_U_AVG;
        }
        prev_prev_price = prevPrice;
        prevPrice = price;
    }
}

class MMA {
    List<Double> stat;
    int period;
    double totalAmount = 0.0;

    double prevAvg = 0;

    public MMA(int period) {
        this.period = period;
        stat = new ArrayList<>();
    }

    public void reset()
    {
        stat = new ArrayList<>();
        totalAmount = 0.0;
        prevAvg = 0;
    }

    public int getCnt()
    {
        return stat.size();
    }

    public void addStat(double d) {
        prevAvg = getAvg();
        stat.add(d);
        totalAmount += d;
        while (stat.size() > period) {
            Double first = stat.get(0);
            totalAmount = totalAmount - first;
            stat.remove(0);
        }
    }

    public double removeOldData()
    {
        double old = stat.remove(0);
        totalAmount -= old;
        return old;
    }

    public double getPrevAvg() {
        return prevAvg;
    }

    public double getAvg() {
        if (stat.size() > 0)
            return totalAmount / (double) stat.size();
        return 0.0;
    }
}

class PriceCty {
    public double price;
    public double cty;
    public PriceCty(double p, double c)
    {
        this.price = p;
        this.cty = c;
    }
}

class PriceQtyTime {
    public double price;
    public double qty;
    public long time;
    public PriceQtyTime(double p, double q, long time)
    {
        this.price = p;
        this.qty = q;
        this.time = time;
    }
}

class MMAT {
    // MMA с ограничением не менее N и не менее timeframe

    List<DoubleLongPair> stat;
    int period;
    long timeframe;
    double totalAmount = 0.0;

    double prevAvg = 0;

    public MMAT(int period, long timeframe) {
        this.timeframe = timeframe;
        this.period = period;
        stat = new ArrayList<>();
    }

    public void addStat(double d, long time) {
        prevAvg = getAvg();
        stat.add(new DoubleLongPair(d, time));
        totalAmount += d;

        DoubleLongPair first = null;
        if (stat.size() > 0)
            first = stat.get(0);

        while ( stat.size() > period
                && time - first.l > timeframe)
        {
            totalAmount = totalAmount - first.d;
            stat.remove(0);
            first = stat.get(0);
        }
    }

    public double getPrevAvg() {
        return prevAvg;
    }

    public double getAvg() {
        if (stat.size() > 0)
            return totalAmount / (double) stat.size();
        return 0.0;
    }
    public double getSum() {
        return totalAmount ;
    }
}

class MMATema {
    // - Собирает данные за указанный таймфрейм
    // - делит весь таймфрей на число (период)
    // - возвращает EMA(n) , где каждый квант это сумма за часть периода

    public final Deque<DoubleLongSymbPrice> stat;
    int period;
    long timeframe;
    double totalAmount = 0.0;

    double prevAvg = 0;

    public MMATema(int period, long timeframe) {
        this.timeframe = timeframe;
        this.period = period;
        this.stat = new ArrayDeque<>(100);
    }

    public void addStat(double d, long time) {
        stat.addLast(new DoubleLongSymbPrice(d, time));

        totalAmount += d;

        while (!stat.isEmpty()) {
            DoubleLongSymbPrice first = stat.peekFirst();

            if (time - first.l > timeframe)
            {
                totalAmount = totalAmount - first.d;
                stat.removeFirst(); // O(1)
            } else
                break;
        }
    }

    public void addStat(double d, long time, String symb, Double price) {
        stat.addLast(new DoubleLongSymbPrice(d, time, symb, price));

        totalAmount += d;

        while (!stat.isEmpty()) {
            DoubleLongSymbPrice first = stat.peekFirst();

            if (time - first.l > timeframe)
            {
                totalAmount = totalAmount - first.d;
                stat.removeFirst(); // O(1)
            } else
                break;
        }
    }

    public double getPriceTravel()
    {
        if (stat.isEmpty())
            return 0.0;

        // Находим последнюю по времени запись для каждой монеты (финальная цена).
        Map<String, DoubleLongSymbPrice> lastBySymbol = new HashMap<>();
        for (DoubleLongSymbPrice rec : stat)
        {
            if (rec == null || rec.symbol == null || rec.price == null)
                continue;
            DoubleLongSymbPrice prevLast = lastBySymbol.get(rec.symbol);
            if (prevLast == null || rec.l >= prevLast.l)
                lastBySymbol.put(rec.symbol, rec);
        }

        double weightedDeltaSum = 0.0;
        double weightSum = 0.0;

        for (DoubleLongSymbPrice start : stat)
        {
            if (start == null || start.symbol == null || start.price == null || start.price == 0.0)
                continue;

            DoubleLongSymbPrice last = lastBySymbol.get(start.symbol);
            if (last == null || last == start || last.price == null)
                continue; // Последняя запись не участвует как стартовая.

            // d — это объем события; чем больше объем, тем сильнее влияние на итог.
            double weight = start.d;
            if (weight <= 0.0)
                continue;

            double priceDeltaPercent = (last.price - start.price) * 100.0 / start.price;
            weightedDeltaSum += priceDeltaPercent * weight;
            weightSum += weight;
        }

        if (weightSum <= 0.0)
            return 0.0;
        return weightedDeltaSum / weightSum;
    }

    /**  */
    public double getFirst()
    {
        if (stat.isEmpty())
            return 0.0;
        return stat.peekFirst().d;
    }

    public double getMax() {
        double max = 0.0;
        for (DoubleLongSymbPrice dlp : stat)
        {
            if (dlp.d > max)
                max = dlp.d;
        }
        return max;
    }

    /**  Возвращает какой процент событий в статистике имеет значение выше заданного  */
    public double getEventsPercentsMoreThan(double overN)
    {
        if (stat.size() < 1)
            return 0.0;
        int moreThanCnt = 0;
        for (DoubleLongSymbPrice dlp : stat)
        {
            if (dlp.d > overN)
                moreThanCnt++;
        }
        return moreThanCnt * 100.0 / stat.size();
    }

    /** Возвращает true, если процент значений выше X больше либо равен порогу P. */
    public boolean isEventsPercentsMoreThanOrEqual(double x, double p)
    {
        double moreThanPercent = getEventsPercentsMoreThan(x);
        return moreThanPercent >= p;
    }

    public double getMax(long quantFrame) {
        double max = 0.0;
        MMATema mt = new MMATema(1, quantFrame);
        for (DoubleLongSymbPrice dlp : stat)
        {
            mt.addStat(dlp.d, dlp.l);
            if (mt.getSum() > max)
                max = mt.getSum();
        }
        return max;
    }

    public double getMin(long quantFrame) {
        double min = 0.0;
        MMATema mt = new MMATema(1, quantFrame);
        for (DoubleLongSymbPrice dlp : stat)
        {
            mt.addStat(dlp.d, dlp.l);
            if (mt.getSum() < min)
                min = mt.getSum();
        }
        return min;
    }

    public double getSum(long time) {
        while (!stat.isEmpty()) {
            DoubleLongSymbPrice first = stat.peekFirst();

            if (time - first.l > timeframe)
            {
                totalAmount = totalAmount - first.d;
                stat.removeFirst(); // O(1)
            } else
                break;
        }
        return totalAmount ;
    }

    public double getSum() {
        return totalAmount ;
    }

    public double getAvg()
    {
        if (stat.size() > 0)
            return totalAmount / (double) stat.size();
        return 0.0;
    }
}

class MMATema2 {
    /*
        Тут хранятся только уникальные (1 запись на 1 символ) записи за временное окно
    */

    // - Собирает данные за указанный таймфрейм
    // - делит весь таймфрей на число (период)
    // - возвращает EMA(n) , где каждый квант это сумма за часть периода

    private final Set<SymbolDataTime> stat;
    long timeframe;
    double totalAmount = 0.0;

    public MMATema2(long timeframe) {
        this.timeframe = timeframe;
        this.stat = new HashSet<>();
    }

    /**  */
    public void addStat(String symbol, double d, long time) {
        SymbolDataTime sdt = new SymbolDataTime(symbol, d, time);
        Set<SymbolDataTime> toRemove = new HashSet<>();
        for (SymbolDataTime s : stat) {
            if (time - s.time > timeframe || s.symbol.equals(sdt.symbol))
            {
                totalAmount = totalAmount - s.data;
                toRemove.add(s);
            }
        }
        stat.removeAll(toRemove);
        totalAmount += d;
        stat.add(sdt);
    }

    public double getAvg()
    {
        if (stat.size() < 1)
            return 0.0;
        return totalAmount / (double) stat.size();
    }
}

// дает среднее по цене, но так же хранит объем
class MMAVT {

    private final Deque<PriceQtyTime> stat;

    private int period;
    long timeframe;
    double totalAmount = 0.0;


    public MMAVT(int period, long timeframe)
    {
        this.period = period;
        this.timeframe = timeframe;
        // Инициализируем массив заданного размера
        this.stat = new ArrayDeque<>(100);
    }

    /**
     * Возвращает количество по цене в диапазоне.
     * Обход Deque происходит с помощью итератора.
     */
    public double getQtyInPriceRange(int comissions)
    {
        double sumQty = 0;
        Double lastQty = null;
        double lastPrice = 0;
        double rabge = 0;

        // Обход Deque в обратном порядке: от самого нового к самому старому (как в оригинале)
        for (PriceQtyTime pqt : stat)
        {
            // Здесь обход идет от нового к старому (ArrayDequeIterator)
            // Если нужен был бы строгий обратный порядок,
            // использовали бы descendingIterator(), но для Deque стандартный итератор
            // обычно идет от head (старый) к tail (новый).
            // В ArrayDeque это не гарантировано, поэтому безопаснее обходить через массив/List
            // или использовать descendingIterator.
            // Оставим итератор по умолчанию, но будем помнить, что он может идти
            // от старого к новому (First -> Last).

            // ПРЕДПОЛАГАЕМ, что стандартный итератор Deque идет от старого к новому (от First к Last)
            // Если в вашем оригинальном коде логика `stat.get(stat.size() - i - 1)`
            // предполагала обход от НОВОГО к СТАРОМУ,
            // нам нужно использовать descendingIterator().

            // Используем descendingIterator для обхода от НОВОГО к СТАРОМУ
            if (pqt == null) continue;

            if (lastQty == null) {
                lastQty = pqt.qty;
                lastPrice = pqt.price;
                rabge = lastPrice * 0.0001 * (double) comissions;
            }

            if (lastPrice != 0 && Math.abs(pqt.price - lastPrice) < rabge)
                sumQty += pqt.qty;
        }

        return sumQty;
    }

    /**  */
    public double getQtyInPriceRange(int comissions, boolean up)
    {
        double sumQty = 0;
        Double lastQty = null;
        double lastPrice = 0;
        double range = 0;

        // Обход Deque в обратном порядке: от самого нового к самому старому (как в оригинале)
        for (PriceQtyTime pqt : stat)
        {
            // Здесь обход идет от нового к старому (ArrayDequeIterator)
            // Если нужен был бы строгий обратный порядок,
            // использовали бы descendingIterator(), но для Deque стандартный итератор
            // обычно идет от head (старый) к tail (новый).
            // В ArrayDeque это не гарантировано, поэтому безопаснее обходить через массив/List
            // или использовать descendingIterator.
            // Оставим итератор по умолчанию, но будем помнить, что он может идти
            // от старого к новому (First -> Last).

            // ПРЕДПОЛАГАЕМ, что стандартный итератор Deque идет от старого к новому (от First к Last)
            // Если в вашем оригинальном коде логика `stat.get(stat.size() - i - 1)`
            // предполагала обход от НОВОГО к СТАРОМУ,
            // нам нужно использовать descendingIterator().

            // Используем descendingIterator для обхода от НОВОГО к СТАРОМУ
            if (pqt == null) continue;

            if (lastQty == null) {
                lastQty = pqt.qty;
                lastPrice = pqt.price;
                range = lastPrice * 0.0001 * (double) comissions;
            }

            if (lastPrice != 0 &&
                    (
                    (up && pqt.price - lastPrice < range && pqt.price >= lastPrice)
                    ||
                    (!up && lastPrice - pqt.price < range && pqt.price <= lastPrice)
                    )
                )
                sumQty += pqt.qty;
        }

        return sumQty;
    }

    /**
     * Добавляет элемент, удаляя старые по timeframe.
     * Удаление: O(1) за элемент. Общая сложность: O(k + 1), где k - количество удаляемых элементов.
     */
    public void addStat(double price, double qty, long time)
    {
        PriceQtyTime newElement = new PriceQtyTime(price, qty, time);

        // 1. Добавление нового элемента (O(1))
        stat.addLast(newElement);
        totalAmount += qty;

        // 2. Ограничение по timeframe (основное ограничение)
        // Удаляем старые элементы с начала (head) Deque, пока они выходят за timeframe.
        // Это O(1) за каждый удаленный элемент.
        while (!stat.isEmpty()) {
            PriceQtyTime first = stat.peekFirst();
            if (first == null || time - first.time <= timeframe) {
                break; // Самый старый элемент в порядке, останавливаемся
            }

            // Элемент слишком старый, удаляем его
            stat.removeFirst(); // O(1)
            totalAmount -= first.qty;
        }

        // 3. Ограничение по maxPeriod (дополнительное ограничение)
        // Удаляем элементы, если мы превысили максимальный размер (maxPeriod = 100)
        while (stat.size() > 100) {
            PriceQtyTime first = stat.removeFirst(); // O(1)
            totalAmount -= first.qty;
        }

        // ВНИМАНИЕ: Если `period` всегда будет 1 (как вы упомянули),
        // ограничение по `maxPeriod` (100) становится просто страховкой.
        // Основная работа выполняется ограничением по `timeframe`.
    }
}

// дает среднее по цене, но так же хранит объем
class MMAV {

    List<PriceCty> stat;
    private int period;

    public MMAV(int period) {
        this.period = period;
        stat = new ArrayList<>();
    }

    public void reset()
    {
        stat = new ArrayList<>();
    }

    public int getCnt()
    {
        return stat.size();
    }

    // добавляет с сортировкой.. чем ближе к КОНЦУ списка, тем ВЫГОДНЕЕ цена
    public void addStat(PriceCty pc, boolean buy)
    {
        stat.add(pc);
    }

    public PriceCty removeLastRecord()
    {
        PriceCty old = stat.remove(stat.size() - 1);
        return old;
    }

    public PriceCty getLastRecord()
    {
        PriceCty old = stat.get(stat.size() - 1);
        return old;
    }

    // вернет общее кол-во
    public double getTotalQty()
    {
        double sumQty = 0;
        for (int i=0; i < stat.size(); i++)
        {
            PriceCty pc = stat.get(i);
            sumQty += pc.cty;
        }
        return sumQty;
    }

    /*
    Удалит часть обеьма - т.е. или уменьшит последний квант или удалит нужные кванты пока
    общий обьем не станет равным целевому
     */
    public void minusQtyPart(double part)
    {
        double targetQty = getTotalQty() * (1.0 - part);
        while (getTotalQty() > targetQty)
        {
            double delta = getTotalQty() - targetQty;
            PriceCty lastRecord = getLastRecord();
            if (delta >= lastRecord.cty)
            {
                removeLastRecord();
            } else {
                lastRecord.cty = lastRecord.cty - delta;
            }
        }
    }

    // Вернет - нет ли закупки на конкретной линии цены
    public boolean isGridSlotAvailable(double price, double priceStep)
    {
        if (stat.size() < 1)
            return true;
        if (stat.size() == 1)
        {
            PriceCty pc = stat.get(0);
            if (Math.abs(price - pc.price) > priceStep / 2.0)
                // единственная запись , которая отстоит "далеко" - значит можно добавить новую запись
                return true;
        } else {
            // есть более 1 записи уже
            double minPrice = Double.MAX_VALUE;
            double maxPrice = Double.MIN_VALUE;
            for (int i = 0; i < stat.size() - 1; i++)
            {
                PriceCty pc1 = stat.get(i);
                PriceCty pc2 = stat.get(i + 1);
                double min = Math.min(pc1.price, pc2.price);
                double max = Math.max(pc1.price, pc2.price);
                if (price > min + priceStep / 2.0 && price < max - priceStep / 2.0)
                    // можно вписать в серединку
                    return true;
                minPrice = Math.min(minPrice, min);
                maxPrice = Math.max(maxPrice, max);
            }
            if (price < minPrice - priceStep / 2.0 || price > maxPrice + priceStep / 2.0)
                // можно добавить снизу или сверху
                return true;
        }
        return false;
    }

    // вернет среднюю ЦЕНУ
    public double getAvg() {
        if (stat.size() < 1)
            return 0.0;

        double sumQty = 0;
        double sumPrice = 0;
        for (int i=0; i < stat.size(); i++)
        {
            PriceCty pc = stat.get(i);
            sumQty += pc.cty;
            sumPrice += pc.price * pc.cty;
        }
        return sumPrice / sumQty;
    }
}

// Double EMA
class DEMMA {

    private int period;
    private Double prevEma = null;
    private Double avg = null;

    EMMA emaBase;
    EMMA emaEma;

    public DEMMA(int period) {
        emaBase = new EMMA(period);
        emaEma = new EMMA(period);
        this.period = period;
    }

    public void addStat(double d) {
        emaBase.addStat(d);
        emaEma.addStat(emaBase.getAvg());
        double res = 2.0 * emaBase.getAvg() - emaEma.getAvg();
        prevEma = avg;
        avg = res;
    }

    public double getAvg() {
        if (avg != null)
            return avg;
        return 0.0;
    }
}

class EMMA {
    
    private int period;
    private Double prevEma = null;
    private Double avg = null;
    
    public EMMA(int period) {
        this.period = period;        
    }

    public void addStat(double d) {
        double formula = d;
        if (prevEma != null)
        {
            formula = (d - prevEma) * 2.0 / (1.0 + (double)period) + prevEma;
        }
        prevEma = avg;
        avg = formula;
    }

    public double getAvg() {
        if (avg != null)
            return avg;
        return 0.0;
    }
}

class EVO2MMA {

    MMA mma1;
    MMA mma2;

    double prevAvg = 0;

    public EVO2MMA(MMA mma1, MMA mma2) {
        this.mma1 = mma1;
        this.mma2 = mma2;
    }

    public void addStat(double d) {
    }

    public double getPrevAvg() {
        return prevAvg;
    }

    private List<Double> memory1 = new ArrayList<>();
    private List<Double> memory2 = new ArrayList<>();

    public double getAvg() {
        try {
            memory1.add( mma1.getPrevAvg() );
            memory2.add( mma2.getPrevAvg() );

            while (memory1.size() > 5)
            {
                memory1.remove(0);
                memory2.remove(0);
            }

            double deltaMMA1 = mma1.getAvg() - memory1.get(0);
            double deltaMMA2 = mma2.getAvg() - memory2.get(0);

            double precentGrowMMA1 = deltaMMA1 / mma1.getAvg();
            double precentGrowMMA2 = deltaMMA2 / mma2.getAvg();

            double corelateRatio = precentGrowMMA2 == 0 ? 100.0 : precentGrowMMA1 * 300.0/ precentGrowMMA2;

            if (corelateRatio > 100) corelateRatio = 100.0;
            if (corelateRatio < -100) corelateRatio = -100.0;

            return corelateRatio;
        } catch (Exception e)
        {
            return 0;
        }
    }
}

class EVO {

    int n;
    int nSmooth;

    MMA buyM;
    MMA sellM;
    MMA smoothM;

    public EVO(int n, int nSmooth)
    {
        this.n = n;
        this.nSmooth = nSmooth;
        buyM = new MMA(n);
        sellM = new MMA(n);
        smoothM = new MMA(nSmooth);
    }

    public void addStat(double o, double h, double l, double c, double v) {
        double deltaPrice = c - o;      // тело свечи
        double deltaPriceMM = h - l;
        double directedVolume = 0;
        double indirectedVolume = 0;
        if (deltaPriceMM > 0) {
            directedVolume = v / deltaPriceMM * Math.abs(deltaPrice);
            indirectedVolume = v / deltaPriceMM * (deltaPriceMM - Math.abs(deltaPrice));
        }

        if (deltaPrice > 0) {
            buyM.addStat(directedVolume + indirectedVolume / 2.0);
            sellM.addStat(indirectedVolume / 2.0);
        } else {
            sellM.addStat(directedVolume + indirectedVolume / 2.0);
            buyM.addStat(indirectedVolume / 2.0);
        }
        double z = buyM.getAvg() - sellM.getAvg();
        smoothM.addStat(z);
    }

    public double getPrevAvg() {
        return smoothM.getPrevAvg();
    }

    public double getAvg() {
        return smoothM.getAvg();
    }
}

class EVO2 {

    int n;
    int nSmooth;

    RSI r;
    MMA smoothM;
    boolean invert;

    public EVO2(int n, int nSmooth, boolean invert)
    {
        this.invert = invert;
        this.n = n;
        this.nSmooth = nSmooth;
        r = new RSI(n);
        smoothM = new MMA(nSmooth);
    }

    double prevDeltaPrice = 0;
    public void addStat(double o, double h, double l, double c, double v) {
        double deltaPrice = invert ? o - c : c - o;

        if (deltaPrice < 0)
            deltaPrice = prevDeltaPrice;
        else
            prevDeltaPrice = deltaPrice;

        double ratio = deltaPrice / v;
        r.addStat(ratio);
        smoothM.addStat(r.getAvg());
    }

    public double getPrevAvg() {
        return smoothM.getPrevAvg();
    }

    public double getAvg() {
        return smoothM.getAvg();
    }
}
