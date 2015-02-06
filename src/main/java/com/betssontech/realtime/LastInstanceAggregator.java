package com.betssontech.realtime;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 * Returns the last timestamp by "aggregating" and throwing away all values except the last
 */
class LastInstanceAggregator implements CombinerAggregator<String> {
    private final String field;

    public LastInstanceAggregator(String field){
        this.field = field;
    }

    @Override
    public String init(TridentTuple tuple) {
        return tuple.getStringByField(TridentTest.TIMESTAMP);
    }

    @Override
    public String combine(String val1, String val2) {
        return val2;
    }

    @Override
    public String zero() {
        return null;
    }
}
