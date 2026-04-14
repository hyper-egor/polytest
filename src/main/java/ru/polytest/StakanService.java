package ru.polytest;


import jakarta.annotation.Nonnull;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Test Task / Quant Researcher
 *
 * @author ypabl
 */
@Service
public class StakanService {
    public static final Logger log = LoggerFactory.getLogger(StakanService.class);
    private static final double DEFAULT_VOLUME_NUM_MIN = 150_000d;
    private static final double DEFAULT_LIQUIDITY_NUM_MIN = 10_000d;

    /*  --- напоминалка ---
        Bids - заявки на покупку
        Asks - заявки на продажу
        side BUY -> правим Bids / SELL - правим Asks
     */

    @Autowired
    PolyHandler polyHandler;

    @Autowired
    PolyService polyService;

    @PostConstruct
    private void init()
    {
        log.info("Starting ...");
        List<Event> events = getFilteredEvents();
        if (events.size() < 1)
        {
            log.info("No events found.");
            return;
        }

        Set<String> tokens2subscribe = new HashSet<>();

        int cnt = 0;
        for (Event event : events) {
            log.info(" -- event["+cnt+"]: " + event.getId() + " " + event.getTitle()
                    + "; markets = " + event.getMarkets().size() + " 24hourvolume = " + event.getVolume24hr());

            for (Market market : event.getMarkets())
            {
                String outs = "";
                for (int i=0; i < market.getOutcomes().size(); i++)
                {
                    if (outs.length() > 0) outs += "  --  ";
                    String outDesc = market.getOutcomes().get(i);
                    String outPrice = "-";
                    if (market.getOutcomePrices() != null && market.getOutcomePrices().size() > 1)
                        outPrice = market.getOutcomePrices().get(i);
                    else {
                        // Когда нет цены - это флаг того, что данный результат просто зарезервирован типа "Person X" etc.
                        // т.е. этот вариант результата надо просто удалить - отслеживать его нет смысла
                        continue;
                    }
                    String outToken = market.getClobTokenIds().get(i);
                    tokens2subscribe.add(outToken);
                    double fee = 0;
                    if (market.getFeesEnabled())
                        fee = polyService.getFeeRate(outToken);
                    outs += outDesc + " / " + outPrice + " / " + fee;
                }

                log.info(" -> Mark " + market.getQuestion() + " OUT: [" +outs+ "]");
            }
            cnt++;
        }

        polyHandler.setSubscriptions(tokens2subscribe);
        polyHandler.init();
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
                for (Market market : event.getMarkets()) {
                    //if (market.getFeesEnabled())
                      //  throw new Exception("Skip event with charged markets..");
                    if (market.getOutcomes() == null || market.getOutcomes().size() != 2
                            //|| market.getOutcomePrices() == null || market.getOutcomePrices().size() != 2
                            || market.getClobTokenIds() == null || market.getClobTokenIds().size() != 2)
                        throw new Exception("Wrong enet settings - no all needed outcome data");
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

}
