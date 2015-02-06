package com.betssontech.realtime;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 * Returns the first value for
 */
class FirstInstanceAggregator implements CombinerAggregator<String> {
    private final String field;
    private String value;

    public FirstInstanceAggregator(String field) {
        this.field = field;
    }

    @Override
    public String init(TridentTuple tuple) {
        value = tuple.getStringByField(field);
        return value;
    }

    @Override
    public String combine(String val1, String val2) {
        return value;
    }

    @Override
    public String zero() {
        return null;
    }
}
