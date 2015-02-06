package com.betssontech.realtime;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
* Created by jelu on 2015-02-06.
*/
class BetFilter extends BaseFilter {

    @Override
    public boolean isKeep(TridentTuple tuple) {
        return tuple.getString(0).equals(TridentTest.BET);
    }
}
