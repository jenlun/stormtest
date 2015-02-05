package com.betssontech.realtime;

import backtype.storm.tuple.Tuple;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;

public class StormTestCase {
    protected Mockery context = new Mockery() {
        {
            setImposteriser(ClassImposteriser.INSTANCE);
        }
    };

    protected Tuple getTuple() {
        return context.mock(Tuple.class);
    }
}
