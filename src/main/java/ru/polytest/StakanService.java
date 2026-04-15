package ru.polytest;


import jakarta.annotation.Nonnull;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/** Test Task / Quant Researcher
 *
 * @author ypabl
 */
@Service
public class StakanService {
    public static final Logger log = LoggerFactory.getLogger(StakanService.class);
    private static final double DEFAULT_VOLUME_NUM_MIN = 100_000d;
    private static final double DEFAULT_LIQUIDITY_NUM_MIN = 5_000d;

    /*  --- напоминалка ---
        Bids - заявки на покупку
        Asks - заявки на продажу
        side BUY -> правим Bids / SELL - правим Asks
     */

    @Autowired
    PolyHandler polyHandler;

    @Autowired
    PolyService polyService;

    // Это мапа инструментов , где ключ - это токен одного из исходов которые мы отслеживаем по websocket
    private Map<String, Instrument> instrumentsByTokens = new HashMap<>();
    /**  */
    private void addInstrument(String outcomeToken, MarketWraper marketWraper,
                               String outcomename, double feeRate, Instrument instrument)
    {
        if (instrument.addOutcome(marketWraper, outcomename, feeRate, outcomeToken))
            instrumentsByTokens.put(outcomeToken, instrument);
    }

    @PostConstruct
    private void init()
    {
        log.info("Starting ...");
        polyHandler.setStakanService(this);

        List<Event> events = getFilteredEvents();
        if (events.size() < 1)
        {
            log.info("No events found.");
            return;
        }

        int cnt = 0;
        for (Event event : events) {
            Instrument instrument = new Instrument(event);

            log.info(" -- event["+cnt+"]: " + event.getId() + " " + event.getTitle()
                    + "; markets = " + event.getMarkets().size() + " 24hourvolume = " + event.getVolume24hr());

            Set<String> uniqMarketNames = new HashSet<>();
            for (Market market : event.getMarkets())
            {
                if (uniqMarketNames.contains(market.getQuestion())) {
                    log.info("=============== dubl ");
                    continue;
                } else
                    uniqMarketNames.add(market.getQuestion());

                String outs = "";
                MarketWraper marketWraper = new MarketWraper(market);
                for (int i=0; i < market.getOutcomes().size(); i++)
                {
                    if (outs.length() > 0) outs += "  --  ";
                    String outDesc = market.getOutcomes().get(i);
                    String outPrice = "-";
                    if (market.getOutcomePrices() != null && market.getOutcomePrices().size() > 1)
                        outPrice = market.getOutcomePrices().get(i);
                    else if (!market.getNegRiskOther()) {
                        // Когда нет цены - это флаг того, что данный результат просто зарезервирован типа "Person X" etc.
                        // т.е. этот вариант результата надо просто удалить - отслеживать его нет смысла
                        continue;
                    }
                    String outToken = market.getClobTokenIds().get(i);
                    double fee = 0;

                    /* // SWITCH ON WHILE PROD
                    if (market.getFeesEnabled())
                        fee = polyService.getFeeRate(outToken);
                    */
                    outs += outDesc + " / " + outPrice + " / " + fee;
                    addInstrument(outToken, marketWraper, outDesc, fee, instrument);
                }

                log.info(" -> Mark " + market.getQuestion() + " OUT: [" +outs+ "]");
            }
            cnt++;
        }
        log.info("Total chasing instruments: " + instrumentsByTokens.values().size());

        polyHandler.setSubscriptions(instrumentsByTokens.keySet());
        polyHandler.init();
    }

    /** Полностью инициализирует или переинициализирует стакан по snapshot-сообщению book. */
    public void handleBookEvent(String assetId,
                                String marketId,
                                List<Book.LevelUpdate> bids,
                                List<Book.LevelUpdate> asks,
                                String hash,
                                String timestamp) {
        Instrument instrument = instrumentsByTokens.get(assetId);
        if (instrument == null) {
            log.warn("Skip book event: token {} is not tracked", assetId);
            return;
        }

        Book book = instrument.getBook(assetId);
        if (book == null) {
            log.warn("Skip book event: no book found for token {}", assetId);
            return;
        }

        long eventTimestamp = parseTimestamp(timestamp);

        // Диагностический лог: иногда snapshot приходит с пустой одной стороной стакана.
        // Это важно видеть, чтобы отличать «книга не инициализирована» от «на стороне реально нет заявок».
        int bidCount = bids == null ? 0 : bids.size();
        int askCount = asks == null ? 0 : asks.size();
        if (bidCount == 0 || askCount == 0) {
            log.warn(
                    "Book snapshot has empty side: assetId={}, marketId={}, bids={}, asks={}, hash={}, ts={}",
                    assetId,
                    marketId,
                    bidCount,
                    askCount,
                    hash,
                    eventTimestamp
            );
        }

        book.replace(assetId, marketId, bids, asks, hash, eventTimestamp);
        instrument.checkMarketOpportunity(assetId);
        //log.info("Book inited for " + marketId);
    }

    long lastDebugTime = 0;
    private void debugStat()
    {
        double totalYesNoProfit = 0;
        double totalYesNoProfit_imposible = 0;
        double totalUnreleasedYesNoQty = 0;
        int totalYesNoSignals = 0;
        long totalYesNoLifetime = 0;

        double totalAllYesProfit = 0;
        double totalAllYesProfit_imposible = 0;
        double totalUnreleasedAllYesQty = 0;
        int totalAllYesSignals = 0;
        long totalAllYesLifetime = 0;

        int totalInstruments = 0;
        int totalMarkets = 0;
        for (Instrument instrument : new HashSet<>(instrumentsByTokens.values()))
        {
            totalInstruments++;
            totalYesNoProfit += instrument.accumelatedYesNoProfit;
            totalYesNoProfit_imposible += instrument.accumelatedYesNoProfit_imposible;
            totalYesNoSignals += instrument.signalYesNoLifetimes.size();

            totalAllYesProfit += instrument.accumelatedAllYesProfit;
            totalAllYesProfit_imposible += instrument.accumelatedAllYesProfit_imposible;
            totalAllYesSignals += instrument.signalAllYesLifetimes.size();

            if (instrument.signal != null)
            {
                SimpleSizePrice deal = instrument.signal.theDeal;
                totalUnreleasedAllYesQty += deal.price * deal.size;
            }

            for (Long lt : instrument.signalYesNoLifetimes)
                totalYesNoLifetime += lt;
            for (MarketWraper marketWraper : new HashSet<>(instrument.marketsByOutcomeToken.values()))
            {
                totalMarkets++;
                if (marketWraper.signal != null)
                {
                    SimpleSizePrice deal = marketWraper.signal.theDeal;
                    totalUnreleasedYesNoQty += deal.price * deal.size;
                }
            }
        }
        long avgYesNoSigLifetime = 0;
        long avgAllYesSigLifetime = 0;

        if (totalYesNoSignals > 0)
            avgYesNoSigLifetime = totalYesNoLifetime / totalYesNoSignals;
        if (totalAllYesSignals > 0)
            avgAllYesSigLifetime = totalAllYesLifetime / totalAllYesSignals;

        log.info("======== total instruments: " + totalInstruments);
        log.info("======== total markets: " + totalMarkets);
        log.info("======== total YES-NO profit: " + totalYesNoProfit);
        log.info("======== total YES-NO profit (HIGH RISK <50ms): " + totalYesNoProfit_imposible);
        log.info("======== total unreleased YES-NO qty: " + totalUnreleasedYesNoQty + " USD");
        log.info("======== total unreleased YES-NO profit: " + (totalUnreleasedYesNoQty * Instrument.MIN_PERCENT_DELTA_YES_NO / 100.0) + " USD");
        log.info("======== total YES-NO sig cnt: " + totalYesNoSignals);
        log.info("======== total YES-NO sig lifetime: " + avgYesNoSigLifetime);
        log.info(" -- ALL YES");
        log.info("======== total ALL YES profit: " + totalAllYesProfit);
        log.info("======== total ALL YES profit (HIGH RISK <50ms): " + totalAllYesProfit_imposible);
        log.info("======== total unreleased ALL YES qty: " + totalUnreleasedAllYesQty + " USD");
        log.info("======== total unreleased ALL YES profit: " + (totalUnreleasedAllYesQty * Instrument.MIN_PERCENT_DELTA_ALL_YES / 100.0) + " USD");
        log.info("======== total ALL YES sig cnt: " + totalAllYesSignals);
        log.info("======== total ALL YES sig lifetime: " + avgAllYesSigLifetime);
    }


    /** Применяет точечное обновление одного ценового уровня из price_change. */
    public void handlePriceChangeEvent(String assetId,
                                       String marketId,
                                       String side,
                                       String price,
                                       String size,
                                       String hash,
                                       String timestamp) {
        Instrument instrument = instrumentsByTokens.get(assetId);
        if (instrument == null) {
            log.warn("Skip price_change event: token {} is not tracked", assetId);
            return;
        }

        Book book = instrument.getBook(assetId);
        if (book == null) {
            log.warn("Skip price_change event: no book found for token {}", assetId);
            return;
        }

        long eventTimestamp = parseTimestamp(timestamp);
        /* if (!book.isInitializedBySnapshot()) {
            log.warn(
                    "Price change arrived before snapshot init: assetId={}, marketId={}, side={}, price={}, size={}, hash={}, ts={}",
                    assetId,
                    marketId,
                    side,
                    price,
                    size,
                    hash,
                    eventTimestamp
            );
        } */
        book.applyPriceChange(assetId, marketId, side, price, size, hash, eventTimestamp);
        instrument.checkMarketOpportunity(assetId);

        if (System.currentTimeMillis() - lastDebugTime > 60_000)
        {
            lastDebugTime = System.currentTimeMillis();
            debugStat();
        }
    }

    /**  */
    private List<Event> getFilteredEvents()
    {
        List<Event> events = polyService.getAllEvents(DEFAULT_VOLUME_NUM_MIN, DEFAULT_LIQUIDITY_NUM_MIN);

        List<Event> filtered = new ArrayList<>();
        for (Event event : events)
        {
            if (!event.getEnableOrderBook()) {
                //log.info(" order book not enabled -> skip");
                continue;
            }
            if (!event.getActive() || event.getArchived()
                    || event.getClosed()) {
                //log.info(" event not active");
                continue;
            }
            if (!event.getEnableNegRisk() || !event.getNegRisk())
            {
                //log.info("NegRisk not enabled");
                continue;
            }
            if (event.getVolume24hr() == null || event.getVolume24hr() < DEFAULT_VOLUME_NUM_MIN)
            {
                //log.info("snall 24 volume: " + event.getVolume24hr());
                continue;
            } else
                //log.info("24 volume: " + event.getVolume24hr());

            if (event.getMarkets().size() < 3)
            {
                //log.info("Less than 3 outcomes.");
                continue;
            }
            try {
                boolean negRiskOtherExists = false;
                for (Market market : event.getMarkets()) {
                    if (market.getNegRiskOther() /*&& market.getOutcomePrices() != null*/)
                        negRiskOtherExists = true;

                    if (market.getFeesEnabled())
                        throw new Exception("Skip event with charged markets..");
                    if (market.getOutcomes() == null || market.getOutcomes().size() != 2
                            //|| market.getOutcomePrices() == null || market.getOutcomePrices().size() != 2
                            || market.getClobTokenIds() == null || market.getClobTokenIds().size() != 2)
                        throw new Exception("Wrong enet settings - no all needed outcome data");
                }
                if (!negRiskOtherExists)
                {
                    //log.info("Filtering event " + event.getTitle() + " as no negRiskOther exists.");
                    //continue;
                }
            } catch (Exception e)
            {
                continue;
            }
            filtered.add(event);
        }
        log.info("Got " + events.size() + " events. negRisk cnt: " + filtered.size());
        return filtered;
    }

    /**  */
    public List<Market> loadMarkets(double volumeNumMin, double liquidityNumMin) {
        log.info(
                "Loading markets with filters: volume_num_min={}, liquidity_num_min={}",
                volumeNumMin,
                liquidityNumMin
        );
        return polyService.getAllMarkets(volumeNumMin, liquidityNumMin);
    }

    /**  */
    private long parseTimestamp(String timestamp) {
        if (timestamp == null || timestamp.isBlank()) {
            return 0L;
        }
        return Long.parseLong(timestamp);
    }

}
