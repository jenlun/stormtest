package com.betssontech.realtime;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
* Created by jelu on 2015-02-06.
*/
class Divide extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Long a = tuple.getLong(0);
        Long b = tuple.getLong(1);
        Double d = a.doubleValue() / b.doubleValue();
        collector.emit(new Values(d));
    }
}
