package com.betssontech.realtime;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import org.jmock.Expectations;
import org.junit.Test;
import storm.trident.operation.TridentCollector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TestFakeTweetsSpout extends StormTestCase {

    @Test
    public void testFakeSpoutEmitsTweet() throws IOException {
        FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout(10);
        final TridentCollector collector = context.mock(TridentCollector.class);
        context.checking(new Expectations() {{
            atLeast(1).of(collector).emit(with(any(Values.class)));
        }});

        final TopologyContext topology = context.mock(TopologyContext.class);
        Map config = new HashMap();
        spout.open(config, topology);

        spout.emitBatch(0, collector);
        context.assertIsSatisfied();
    }

}
