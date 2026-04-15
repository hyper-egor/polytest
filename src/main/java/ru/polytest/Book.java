package ru.polytest;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class Book {
    private String assetId;
    private String marketId;
    private String lastHash;
    private long lastTimestamp;
    private boolean initializedBySnapshot;

    private final NavigableMap<BigDecimal, BigDecimal> bids;
    private final NavigableMap<BigDecimal, BigDecimal> asks;

    public Book() {
        bids = new TreeMap<>(Comparator.reverseOrder());
        asks = new TreeMap<>();
        initializedBySnapshot = false;
    }

    /** Полностью заменяет текущее состояние стакана новым snapshot-сообщением. */
    public synchronized void replace(String assetId,
                                     String marketId,
                                     List<LevelUpdate> bidLevels,
                                     List<LevelUpdate> askLevels,
                                     String hash,
                                     long timestamp) {
        this.assetId = assetId;
        this.marketId = marketId;
        this.lastHash = hash;
        this.lastTimestamp = timestamp;

        bids.clear();
        asks.clear();

        applyLevels(bids, bidLevels);
        applyLevels(asks, askLevels);
        initializedBySnapshot = true;
    }

    /** Применяет одно точечное обновление уровня цены к нужной стороне стакана. */
    public synchronized void applyPriceChange(String assetId,
                                              String marketId,
                                              String side,
                                              String price,
                                              String size,
                                              String hash,
                                              long timestamp) {
        this.assetId = assetId;
        this.marketId = marketId;
        this.lastHash = hash;
        this.lastTimestamp = timestamp;

        NavigableMap<BigDecimal, BigDecimal> sideLevels;
        if ("BUY".equalsIgnoreCase(side)) {
            sideLevels = bids;
        } else if ("SELL".equalsIgnoreCase(side)) {
            sideLevels = asks;
        } else {
            throw new IllegalArgumentException("Unknown side: " + side);
        }

        updateLevel(sideLevels, price, size);
    }

    /** Возвращает копию текущих bid-уровней для безопасного чтения извне. */
    public synchronized List<LevelSnapshot> getBidLevels() {
        return snapshotOf(bids);
    }

    /** Возвращает копию текущих ask-уровней для безопасного чтения извне. */
    public synchronized List<LevelSnapshot> getAskLevels() {
        return snapshotOf(asks);
    }

    /** Возвращает лучшую цену покупки, если она есть. */
    public synchronized BigDecimal getBestBidPrice() {
        if (bids.isEmpty()) {
            return null;
        }
        return bids.firstKey();
    }

    /** Возвращает лучшую цену продажи, если она есть. */
    public synchronized BigDecimal getBestAskPrice() {
        if (asks.isEmpty()) {
            return null;
        }
        return asks.firstKey();
    }

    /** Возвращает id токена, к которому относится этот стакан. */
    public synchronized String getAssetId() {
        return assetId;
    }

    /** Возвращает market id, из которого пришел последний апдейт стакана. */
    public synchronized String getMarketId() {
        return marketId;
    }

    /** Возвращает hash последнего примененного websocket-сообщения. */
    public synchronized String getLastHash() {
        return lastHash;
    }

    /** Возвращает timestamp последнего примененного websocket-сообщения. */
    public synchronized long getLastTimestamp() {
        return lastTimestamp;
    }

    /** Возвращает количество ценовых уровней в bid-стороне. */
    public synchronized int getBidLevelsCount() {
        return bids.size();
    }

    /** Возвращает количество ценовых уровней в ask-стороне. */
    public synchronized int getAskLevelsCount() {
        return asks.size();
    }

    /** Возвращает true, если стакан хотя бы раз был инициализирован snapshot-сообщением book. */
    public synchronized boolean isInitializedBySnapshot() {
        return initializedBySnapshot;
    }

    /**  */
    private void applyLevels(NavigableMap<BigDecimal, BigDecimal> levels, List<LevelUpdate> updates) {
        if (updates == null || updates.isEmpty()) {
            return;
        }

        for (LevelUpdate update : updates) {
            updateLevel(levels, update.price, update.size);
        }
    }

    /**  */
    private void updateLevel(NavigableMap<BigDecimal, BigDecimal> levels, String priceStr, String sizeStr) {
        BigDecimal price = new BigDecimal(priceStr);
        BigDecimal size = new BigDecimal(sizeStr);

        if (size.signum() == 0) {
            levels.remove(price);
            return;
        }
        levels.put(price, size);
    }

    /**  */
    private List<LevelSnapshot> snapshotOf(NavigableMap<BigDecimal, BigDecimal> levels) {
        if (levels.isEmpty()) {
            return Collections.emptyList();
        }

        List<LevelSnapshot> snapshot = new ArrayList<>(levels.size());
        for (BigDecimal price : levels.keySet()) {
            snapshot.add(new LevelSnapshot(price, levels.get(price)));
        }
        return snapshot;
    }

    public static class LevelUpdate {
        public final String price;
        public final String size;

        public LevelUpdate(String price, String size) {
            this.price = price;
            this.size = size;
        }
    }
}
