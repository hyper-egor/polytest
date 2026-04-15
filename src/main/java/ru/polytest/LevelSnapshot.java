package ru.polytest;

import java.math.BigDecimal;

/** Снимок одного ценового уровня стакана: цена и доступный размер. */
public class LevelSnapshot {
    public final BigDecimal price;
    public final BigDecimal size;

    public LevelSnapshot(BigDecimal price, BigDecimal size) {
        this.price = price;
        this.size = size;
    }
}
