package ru.polytest;

import java.math.BigDecimal;

public class LevelSnapshotWraper {

    public BigDecimal price;
    public BigDecimal size;

    public LevelSnapshotWraper(LevelSnapshot ls)
    {
        this.price = ls.price;
        this.size = ls.size;
    }
}
