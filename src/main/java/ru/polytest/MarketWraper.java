package ru.polytest;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/*
    Это структура для хранения варианта (предсказания) в рамках одного события ("инструмента").

    тут хранится, например "ТРАМП" для "Инструмента" = "кто победит на выборах".
        и у этого варианта есть пара резульататов (outcomes): YES / NO

*/
public class MarketWraper {

    public Market market;
    public String id;
    public String name;
    public Map<String, OutcomeWrapper> outcomesByToken;

    // живой сигнал (сделка) в рамках этого маркета
    public Signal signal = null;

    public MarketWraper(Market m) {
        this.id = m.getId();
        this.name = m.getQuestion();
        this.market = m;
        outcomesByToken = new HashMap<>();
    }

    /**  */
    public void addOutcome(String name, double feeRate, String token)
    {
        OutcomeWrapper wrapper = new OutcomeWrapper(name, feeRate);
        addOutcome(token, wrapper);
    }

    /** */
    public OutcomeWrapper getYesOutCome()
    {
        for (OutcomeWrapper ow : outcomesByToken.values())
        {
            if (ow.name.equalsIgnoreCase("YES"))
                return ow;
        }
        return null;
    }

    /** */
    public OutcomeWrapper getNoOutCome()
    {
        for (OutcomeWrapper ow : outcomesByToken.values())
        {
            if (ow.name.equalsIgnoreCase("NO"))
                return ow;
        }
        return null;
    }

    /**  */
    public void addOutcome(String token, OutcomeWrapper wrapper)
    {
        outcomesByToken.put(token, wrapper);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof MarketWraper that)) return false;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }
}
