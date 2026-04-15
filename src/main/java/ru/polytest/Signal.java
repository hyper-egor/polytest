package ru.polytest;

public class Signal {

    // стратегия покупать YES + NO если их сумма меньше 1
    public static int STRATEGY_SINGLE_OUTCOME = 0;

    // стратегия покупать все YES одного события если их сумма меньше 1
    public static int STRATEGY_ALL_YES = 1;

    public int strategyId;

    public long createTime;
    // время когда по стакану определено, что сигнала больше нет
    public long disappearTime = 0;

    public SimpleSizePrice theDeal = null;

    public Signal(int strategyId)
    {
        this.strategyId = strategyId;
        createTime = System.currentTimeMillis();
    }
}
