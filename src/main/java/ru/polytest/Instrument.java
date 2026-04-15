package ru.polytest;

/*  Структура для хранения отслеживаемой "связки" или "инструмента".
    Это набор сущностей в рамках которых мы ловим арбитражные возможности

 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;

public class Instrument {
    public static final Logger log = LoggerFactory.getLogger(Instrument.class);

    public static final double MIN_PERCENT_DELTA_YES_NO = .9;
    public static final double MIN_PERCENT_DELTA_ALL_YES = 1.5;

    public double accumelatedYesNoProfit = 0;
    public double accumelatedAllYesProfit = 0;

    // это профиты, которые невозможны по реальнмоу таймингку - сделка закрывается до того, как отмечен конец входного сигнала
    public double accumelatedYesNoProfit_imposible = 0;

    public double accumelatedAllYesProfit_imposible = 0;

    public List<Long> signalYesNoLifetimes = new ArrayList<>();
    public List<Long> signalAllYesLifetimes = new ArrayList<>();

    public Event event;
    public String id;
    public String name;

    public Signal signal = null;

    public Map<String, MarketWraper> marketsByOutcomeToken;
    public Map<String, OutcomeWrapper> outcomesByToken;

    public Instrument(Event event) {
        marketsByOutcomeToken = new HashMap<>();
        outcomesByToken = new HashMap<>();
        this.event = event;
        this.name = event.getTitle();
        this.id = event.getId();
    }

    /** добавить вариант результата в этом событие и добавить вариант ответа */
    public boolean addOutcome(MarketWraper marketWraper, String outcomeName, double feeRate, String token)
    {
        marketsByOutcomeToken.put(token, marketWraper);

        OutcomeWrapper outcomeWrapper = new OutcomeWrapper(outcomeName, feeRate);
        outcomesByToken.put(token, outcomeWrapper);
        marketWraper.addOutcome(token, outcomeWrapper);
        return true;
    }

    /**  */
    public Book getBook(String token)
    {
        OutcomeWrapper outcomeWrapper = outcomesByToken.get(token);
        if (outcomeWrapper == null)
            return null;
        return outcomeWrapper.book;
    }

    long lastLogTime = 0;
    int counter = 0;

    /** проверим возможность арбитража внутри одного "Market"
     *  Суть тут в том, что оба варианта результата (outcome) должны в сумме
     *  стоить 1 USDT - это веростяность - либо одно либо другой точно случится.
     *
     *  Если по стакану их цена меньше чем 1 то это потенциальная возможность
     *
     * */
    public void checkMarketOpportunity(String token)
    {
        MarketWraper marketWraper = marketsByOutcomeToken.get(token);
        if (marketWraper == null) {
            log.info("Logic ERROR: marlet not found by token " + token);
            return;
        }

        // Проверяем сумму цен-вероятностей разных исходов одного "вопроса" (маркета). Должно быть 1.
        // если меньше чем 1 - можно купить оба и продать когда будет 1 или выше. либо дождаться результата и получить 1
        // YES + NO

        if (marketWraper.signal == null)
        { // мы не в сделке по этому маркеты - попробуем открыть
            SimpleSizePrice available2buy = findAbilutyToBuyYesNo(marketWraper, MIN_PERCENT_DELTA_YES_NO);
            if (available2buy.size > 0)
            {
                log.info("1 OUTCOME (YES+NO): " + name + " -> " + marketWraper.name + " -> avgPrice = "
                        + available2buy.price
                        + " Amount USD: " + (available2buy.price * available2buy.size));
                marketWraper.signal = new Signal(Signal.STRATEGY_SINGLE_OUTCOME);
                marketWraper.signal.theDeal = available2buy;
            }
        } else {
            // если сигнал еще не ушёл, то проверим - не ушел ли он сейчас
            if (marketWraper.signal.disappearTime == 0)
            { // пока не устанавливали, что сигнала больше нет.
                SimpleSizePrice available2buy = findAbilutyToBuyYesNo(marketWraper, MIN_PERCENT_DELTA_YES_NO);
                if (available2buy.size == 0)
                { // сигнала больше нет
                    marketWraper.signal.disappearTime = System.currentTimeMillis();
                    signalYesNoLifetimes.add(marketWraper.signal.disappearTime - marketWraper.signal.createTime);
                }
            }

            // мы в сделке - попробуем закрыться
            SimpleSizePrice available2sell = findAbilutyToSellYesNo(marketWraper, marketWraper.signal.theDeal, MIN_PERCENT_DELTA_YES_NO);
            if (available2sell.size > 0)
            {
                log.info("CLOSE 1 OUTCOME (YES+NO): " + name + " -> " + marketWraper.name + " -> avgPrice = "
                        + available2sell.price
                        + " Amount USD: " + (available2sell.price * available2sell.size));
                double profit = ((available2sell.price - marketWraper.signal.theDeal.price) * available2sell.size);
                log.info("   Profit: " + profit);
                accumelatedYesNoProfit += profit;
                if (marketWraper.signal.disappearTime == 0 || marketWraper.signal.disappearTime - marketWraper.signal.createTime < 50) {
                    // закрываемся слишком рано - вреальном мире мы не успели бы зайти и закрыться
                    accumelatedYesNoProfit_imposible += profit;
                }
                marketWraper.signal.theDeal.size -= available2sell.size;
                if (marketWraper.signal.theDeal.size <= 0.000000001)
                {
                    if (marketWraper.signal.disappearTime == 0)
                        signalYesNoLifetimes.add(0L);
                    marketWraper.signal = null;
                    log.info("Signal closed.");
                }
            }
        }

        ///////////////////// ALL YES

        if (signal == null)
        { // мы не в сделке по ALL YES этого инструмента
            SimpleSizePrice available2buy = findAbilutyToBuyAllYes(MIN_PERCENT_DELTA_ALL_YES);
            if (available2buy.size > 0)
            {
                log.info("ALL YES: " + name + " -> avgPrice = "
                        + available2buy.price
                        + " Amount USD: " + (available2buy.price * available2buy.size));
                signal = new Signal(Signal.STRATEGY_ALL_YES);
                signal.theDeal = available2buy;
            }
        } else {
            // если сигнал еще не ушёл, то проверим - не ушел ли он сейчас
            if (signal.disappearTime == 0)
            { // пока не устанавливали, что сигнала больше нет.
                SimpleSizePrice available2buy = findAbilutyToBuyAllYes(MIN_PERCENT_DELTA_ALL_YES);
                if (available2buy.size == 0)
                { // сигнала больше нет
                    signal.disappearTime = System.currentTimeMillis();
                    signalAllYesLifetimes.add(signal.disappearTime - signal.createTime);
                }
            }

            // мы в сделке - попробуем закрыться
            SimpleSizePrice available2sell = findAbilutyToSellAllYes(signal.theDeal, MIN_PERCENT_DELTA_ALL_YES);
            if (available2sell.size > 0)
            {
                log.info("CLOSE ALL YES: " + name + " -> avgPrice = "
                        + available2sell.price
                        + " Amount USD: " + (available2sell.price * available2sell.size));
                double profit = ((available2sell.price - signal.theDeal.price) * available2sell.size);
                log.info("   Profit: " + profit);
                accumelatedAllYesProfit += profit;
                if (signal.disappearTime == 0 || signal.disappearTime - signal.createTime < 50) {
                    // закрываемся слишком рано - вреальном мире мы не успели бы зайти и закрыться
                    accumelatedAllYesProfit_imposible += profit;
                }
                signal.theDeal.size -= available2sell.size;
                if (signal.theDeal.size <= 0.000000001)
                {
                    if (signal.disappearTime == 0)
                        signalYesNoLifetimes.add(0L);
                    signal = null;
                    log.info("Signal closed.");
                }
            }
        }
    }

    /**
     *  Находит возможность купить YES + NO
     *  1. динаковым size
     *  2. Таким образом, чтобы дельта цены была более minPercentDelta
     * */
    private SimpleSizePrice findAbilutyToBuyYesNo(MarketWraper marketWraper, double MIN_PERCENT_DELTA)
    {
        BigDecimal one = BigDecimal.ONE;
        BigDecimal hundred = new BigDecimal("100");
        BigDecimal minPercentDelta = BigDecimal.valueOf(MIN_PERCENT_DELTA);

        OutcomeWrapper yes = marketWraper.getYesOutCome();
        OutcomeWrapper no = marketWraper.getNoOutCome();
        if (yes == null || no == null
                || !yes.book.isInitializedBySnapshot()
                || !no.book.isInitializedBySnapshot())
        {
            // нет outcome или их стаканы еще не инициализированы
            return new SimpleSizePrice(0,0);
        }

        // создадим изменяемый массив цен для NO
        List<LevelSnapshotWraper> noLevels = new ArrayList<>();
        for (LevelSnapshot ls : no.book.getAskLevels())
            noLevels.add(new LevelSnapshotWraper(ls));

        boolean pricesAreGood = true;

        List<SimpleSizePrice> allSizes = new ArrayList<>();

        for (LevelSnapshot _level : yes.book.getAskLevels())
        {
            LevelSnapshotWraper levelYes = new LevelSnapshotWraper(_level);
            while (noLevels.size() > 0)
            {
                LevelSnapshotWraper levelNo = noLevels.get(0);
                BigDecimal sumPrice = levelYes.price.add(levelNo.price);
                BigDecimal percentDelta = one.subtract(sumPrice).multiply(hundred);
                if (percentDelta.compareTo(minPercentDelta) > 0) {
                    // эти уровни подходят для совместного выкупа
                    BigDecimal posibleSize = levelYes.size.min(levelNo.size);
                    // добавили выкуп этой части стакана
                    allSizes.add(new SimpleSizePrice(posibleSize.doubleValue(), sumPrice.doubleValue()));

                    if (posibleSize.compareTo(levelYes.size) == 0)
                    { // Взяли полностью size от YES - этот уровень затираем полностью
                        if (posibleSize.compareTo(levelNo.size) == 0)
                        { // в NO тот же самый size , что нам нужен -  значит этот уровнь тоже можно грохнуть
                            noLevels.removeFirst();
                        } else {
                            // Этот NO уровень еще не исчерпан.
                            // просто отметим, что он стал меньше
                            levelNo.size = levelNo.size.subtract(posibleSize);
                        }
                        // Вылетаем из перебора NO совсем
                        // - т.е. возьмем следующий YES level
                        break;
                    } else {
                        // взяли полностью size от NO - этот уровень затираем полностью
                        noLevels.removeFirst();
                        // Этот уровень NO исчерпан, а вот YES - еще нет,
                        // поэтому листанем следующий NO
                        levelYes.size = levelYes.size.subtract(posibleSize);
                        continue;
                    }
                } else {
                    // цены не интересные - на этом компоновку size завершаем.
                    pricesAreGood = false;
                    break;
                }
            }
            if (!pricesAreGood)
                break;
        }
        // тут компоновка size завершена.

        // Посчитаем среднюю цену и общий size
        if (allSizes.size() > 0) {
            double totalSize = 0;
            double totalPricedSize = 0;
            for (SimpleSizePrice sp : allSizes) {
                totalSize += sp.size;
                totalPricedSize += sp.price * sp.size;
            }
            double avgPrice = totalPricedSize / totalSize;
            return new SimpleSizePrice(totalSize, avgPrice);
        }
        return new SimpleSizePrice(0, 0);
    }

    /**
     *  Находит возможность купить корзину из всех YES по всем market текущего Instrument.
     *  1. Покупка идет одинаковым size сразу по всем market.
     *  2. По ask-уровням каждого YES-стакана.
     *  3. Суммарная цена корзины должна давать дельту выше minPercentDelta.
     * */
    private SimpleSizePrice findAbilutyToBuyAllYes(double MIN_PERCENT_DELTA)
    {
        BigDecimal one = BigDecimal.ONE;
        BigDecimal hundred = new BigDecimal("100");
        BigDecimal minPercentDelta = BigDecimal.valueOf(MIN_PERCENT_DELTA);

        Set<MarketWraper> uniqMarkets = new HashSet<>(marketsByOutcomeToken.values());
        if (uniqMarkets.isEmpty()) {
            return new SimpleSizePrice(0, 0);
        }

        // Рабочие изменяемые ask-уровни YES по каждому market.
        List<List<LevelSnapshotWraper>> yesLevelsByMarket = new ArrayList<>();
        for (MarketWraper market : uniqMarkets)
        {
            OutcomeWrapper yes = market.getYesOutCome();
            if (yes == null || !yes.book.isInitializedBySnapshot()) {
                // Нет нужного outcome или стакан еще не инициализирован.
                return new SimpleSizePrice(0, 0);
            }

            List<LevelSnapshotWraper> yesLevels = new ArrayList<>();
            for (LevelSnapshot ls : yes.book.getAskLevels()) {
                yesLevels.add(new LevelSnapshotWraper(ls));
            }

            // Для корзины all-YES нужен доступный ask в каждом market.
            if (yesLevels.isEmpty()) {
                return new SimpleSizePrice(0, 0);
            }

            yesLevelsByMarket.add(yesLevels);
        }

        List<SimpleSizePrice> allSizes = new ArrayList<>();

        while (true)
        {
            BigDecimal sumPrice = BigDecimal.ZERO;
            BigDecimal possibleSize = null;

            // Берем лучший ask из каждого market и считаем цену корзины и общий доступный size.
            for (List<LevelSnapshotWraper> marketYesLevels : yesLevelsByMarket)
            {
                if (marketYesLevels.isEmpty()) {
                    possibleSize = null;
                    break;
                }

                LevelSnapshotWraper bestAsk = marketYesLevels.get(0);
                sumPrice = sumPrice.add(bestAsk.price);

                if (possibleSize == null || bestAsk.size.compareTo(possibleSize) < 0) {
                    possibleSize = bestAsk.size;
                }
            }

            if (possibleSize == null || possibleSize.signum() <= 0) {
                break;
            }

            // Проверяем, что корзина all-YES достаточно дешевая относительно 1.
            BigDecimal percentDelta = one.subtract(sumPrice).multiply(hundred);
            if (percentDelta.compareTo(minPercentDelta) <= 0) {
                // Дальше будет только хуже, так как мы движемся по более дорогим уровням.
                break;
            }

            allSizes.add(new SimpleSizePrice(possibleSize.doubleValue(), sumPrice.doubleValue()));

            // Списываем одинаковый size во всех market; исчерпанные уровни удаляем.
            for (List<LevelSnapshotWraper> marketYesLevels : yesLevelsByMarket)
            {
                LevelSnapshotWraper bestAsk = marketYesLevels.get(0);
                bestAsk.size = bestAsk.size.subtract(possibleSize);
                if (bestAsk.size.signum() <= 0) {
                    marketYesLevels.removeFirst();
                }
            }
        }

        // Посчитаем итоговые size и среднюю цену корзины.
        if (allSizes.size() > 0) {
            double totalSize = 0;
            double totalPricedSize = 0;
            for (SimpleSizePrice sp : allSizes) {
                totalSize += sp.size;
                totalPricedSize += sp.price * sp.size;
            }
            double avgPrice = totalPricedSize / totalSize;
            return new SimpleSizePrice(totalSize, avgPrice);
        }
        return new SimpleSizePrice(0, 0);
    }

    /**
     *  Находит возможность продать корзину из всех YES по всем market текущего Instrument.
     *  1. Продажа идет одинаковым size сразу по всем market.
     *  2. По bid-уровням каждого YES-стакана.
     *  3. Объем продажи ограничен уже купленным объемом позиции.
     *  4. Суммарная цена корзины должна быть выше цены входа на minPercentDelta.
     * */
    private SimpleSizePrice findAbilutyToSellAllYes(SimpleSizePrice buyInfo, double MIN_PERCENT_DELTA)
    {
        BigDecimal hundred = new BigDecimal("100");
        BigDecimal minPercentDelta = BigDecimal.valueOf(MIN_PERCENT_DELTA);

        if (buyInfo == null || buyInfo.size <= 0 || buyInfo.price <= 0) {
            return new SimpleSizePrice(0, 0);
        }

        Set<MarketWraper> uniqMarkets = new HashSet<>(marketsByOutcomeToken.values());
        if (uniqMarkets.isEmpty()) {
            return new SimpleSizePrice(0, 0);
        }

        BigDecimal remaining = BigDecimal.valueOf(buyInfo.size);
        BigDecimal buyAvgPrice = BigDecimal.valueOf(buyInfo.price);
        BigDecimal requiredSellPrice = buyAvgPrice.multiply(
                BigDecimal.ONE.add(minPercentDelta.divide(hundred))
        );

        // Рабочие изменяемые bid-уровни YES по каждому market.
        List<List<LevelSnapshotWraper>> yesLevelsByMarket = new ArrayList<>();
        for (MarketWraper market : uniqMarkets)
        {
            OutcomeWrapper yes = market.getYesOutCome();
            if (yes == null || !yes.book.isInitializedBySnapshot()) {
                // Нет нужного outcome или стакан еще не инициализирован.
                return new SimpleSizePrice(0, 0);
            }

            List<LevelSnapshotWraper> yesLevels = new ArrayList<>();
            for (LevelSnapshot ls : yes.book.getBidLevels()) {
                yesLevels.add(new LevelSnapshotWraper(ls));
            }

            // Для корзины all-YES нужен доступный bid в каждом market.
            if (yesLevels.isEmpty()) {
                return new SimpleSizePrice(0, 0);
            }

            yesLevelsByMarket.add(yesLevels);
        }

        BigDecimal totalSize = BigDecimal.ZERO;
        BigDecimal totalPricedSize = BigDecimal.ZERO;

        while (remaining.signum() > 0)
        {
            BigDecimal sumPrice = BigDecimal.ZERO;
            BigDecimal possibleSize = null;

            // Берем лучший bid из каждого market и считаем цену корзины и общий доступный size.
            for (List<LevelSnapshotWraper> marketYesLevels : yesLevelsByMarket)
            {
                if (marketYesLevels.isEmpty()) {
                    possibleSize = null;
                    break;
                }

                LevelSnapshotWraper bestBid = marketYesLevels.get(0);
                sumPrice = sumPrice.add(bestBid.price);

                if (possibleSize == null || bestBid.size.compareTo(possibleSize) < 0) {
                    possibleSize = bestBid.size;
                }
            }

            if (possibleSize == null || possibleSize.signum() <= 0) {
                break;
            }

            if (sumPrice.compareTo(requiredSellPrice) < 0) {
                // Дальше будет только хуже, так как мы движемся по более дешевым bid-уровням.
                break;
            }

            if (possibleSize.compareTo(remaining) > 0) {
                possibleSize = remaining;
            }

            totalSize = totalSize.add(possibleSize);
            totalPricedSize = totalPricedSize.add(sumPrice.multiply(possibleSize));
            remaining = remaining.subtract(possibleSize);

            // Списываем одинаковый size во всех market; исчерпанные уровни удаляем.
            for (List<LevelSnapshotWraper> marketYesLevels : yesLevelsByMarket)
            {
                LevelSnapshotWraper bestBid = marketYesLevels.get(0);
                bestBid.size = bestBid.size.subtract(possibleSize);
                if (bestBid.size.signum() <= 0) {
                    marketYesLevels.removeFirst();
                }
            }
        }

        if (totalSize.signum() <= 0) {
            return new SimpleSizePrice(0, 0);
        }

        BigDecimal avgSellPrice = totalPricedSize.divide(totalSize, java.math.MathContext.DECIMAL64);
        return new SimpleSizePrice(totalSize.doubleValue(), avgSellPrice.doubleValue());
    }

    /**
     *  Находит возможность продать YES + NO
     *  1. одинаковым size
     *  2. По bid-уровням
     *  3. С ограничением по объему уже купленной позиции
     *  4. Итоговая цена продажи должна быть выше средней цены покупки на minPercentDelta
     * */
    private SimpleSizePrice findAbilutyToSellYesNo(MarketWraper marketWraper,
                                                   SimpleSizePrice buyInfo,
                                                   double MIN_PERCENT_DELTA)
    {
        BigDecimal hundred = new BigDecimal("100");
        BigDecimal minPercentDelta = BigDecimal.valueOf(MIN_PERCENT_DELTA);

        if (buyInfo == null || buyInfo.size <= 0 || buyInfo.price <= 0) {
            return new SimpleSizePrice(0, 0);
        }

        OutcomeWrapper yes = marketWraper.getYesOutCome();
        OutcomeWrapper no = marketWraper.getNoOutCome();
        if (yes == null || no == null
                || !yes.book.isInitializedBySnapshot()
                || !no.book.isInitializedBySnapshot())
        {
            // нет outcome или их стаканы еще не инициализированы
            return new SimpleSizePrice(0,0);
        }

        BigDecimal maxSellSize = BigDecimal.valueOf(buyInfo.size);
        BigDecimal remaining = maxSellSize;
        BigDecimal buyAvgPrice = BigDecimal.valueOf(buyInfo.price);
        BigDecimal requiredSellPrice = buyAvgPrice.multiply(
                BigDecimal.ONE.add(minPercentDelta.divide(hundred))
        );

        // создадим изменяемый массив bid-уровней для NO
        List<LevelSnapshotWraper> noLevels = new ArrayList<>();
        for (LevelSnapshot ls : no.book.getBidLevels()) {
            noLevels.add(new LevelSnapshotWraper(ls));
        }

        BigDecimal totalSize = BigDecimal.ZERO;
        BigDecimal totalPricedSize = BigDecimal.ZERO;
        boolean pricesAreGood = true;

        for (LevelSnapshot yesBidLevel : yes.book.getBidLevels())
        {
            if (remaining.signum() <= 0) {
                break;
            }

            LevelSnapshotWraper levelYes = new LevelSnapshotWraper(yesBidLevel);
            while (noLevels.size() > 0)
            {
                if (remaining.signum() <= 0) {
                    break;
                }

                LevelSnapshotWraper levelNo = noLevels.get(0);
                BigDecimal sumPrice = levelYes.price.add(levelNo.price);
                if (sumPrice.compareTo(requiredSellPrice) >= 0) {
                    // Эти уровни подходят для продажи, но объем ограничен купленной позицией.
                    BigDecimal possibleSize = levelYes.size.min(levelNo.size).min(remaining);
                    if (possibleSize.signum() <= 0) {
                        break;
                    }

                    totalSize = totalSize.add(possibleSize);
                    totalPricedSize = totalPricedSize.add(sumPrice.multiply(possibleSize));
                    remaining = remaining.subtract(possibleSize);

                    if (possibleSize.compareTo(levelYes.size) == 0)
                    {
                        // YES уровень исчерпан полностью
                        if (possibleSize.compareTo(levelNo.size) == 0)
                        {
                            // NO уровень тоже исчерпан
                            noLevels.removeFirst();
                        } else {
                            // NO уровень частично остался
                            levelNo.size = levelNo.size.subtract(possibleSize);
                        }
                        // Переходим к следующему YES уровню
                        break;
                    } else {
                        // NO уровень исчерпан, YES еще остался
                        noLevels.removeFirst();
                        levelYes.size = levelYes.size.subtract(possibleSize);
                    }
                } else {
                    // Цены дальше только хуже, завершаем компоновку.
                    pricesAreGood = false;
                    break;
                }
            }

            if (!pricesAreGood) {
                break;
            }
        }

        if (totalSize.signum() <= 0) {
            return new SimpleSizePrice(0, 0);
        }

        BigDecimal avgSellPrice = totalPricedSize.divide(totalSize, java.math.MathContext.DECIMAL64);
        return new SimpleSizePrice(totalSize.doubleValue(), avgSellPrice.doubleValue());
    }
}
