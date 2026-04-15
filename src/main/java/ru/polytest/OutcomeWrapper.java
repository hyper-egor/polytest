package ru.polytest;

/*
    Структура для хранения одного исхода для предсказания.
    у этого исхода есть
     - имя (например YES / NO)
     - feeRate который взымается с тейкера по определенной формуле при сделке
     - book - список Bid/Asks который мы и мониторим
 */

public class OutcomeWrapper {

    public String name;
    public double feeRate;

    public Book book;

    public OutcomeWrapper(String name, double feeRate) {
        this.name = name;
        this.feeRate = feeRate;
        book = new Book();
    }
}
