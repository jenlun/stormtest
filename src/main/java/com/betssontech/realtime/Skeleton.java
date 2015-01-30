package com.betssontech.realtime;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.tuple.Values;
import org.apache.commons.collections.MapUtils;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.operation.*;
import storm.trident.operation.builtin.Count;
import storm.trident.tuple.TridentTuple;

/**
 * Use this skeleton for starting your own topology that uses the Fake tweets generator as data source.
 *
 * @author pere
 */
public class Skeleton {

    public static final String UPPERCASE_TEXT = "uppercase_text";
    public static final String ACTOR = "actor";
    public static final String TEXT = "text";
    public static final String TOPOLOGY = "hackaton";
    public static final String LOCATION_COUNT = "location_count";
    public static final String LOCATION = "location";
    private static final String COUNT = "count";

    public static StormTopology buildTopology(LocalDRPC drpc) throws IOException {
        FakeTweetsBatchSpout spout = new FakeTweetsBatchSpout();

        TridentTopology topology = new TridentTopology();
        topology.newStream("spout", spout)
                .each(new Fields(ACTOR, TEXT), new PerActorTweetsFilter("dave"))
                .each(new Fields(TEXT, ACTOR), new UppercaseFunction(), new Fields(UPPERCASE_TEXT))
                .each(new Fields(ACTOR, UPPERCASE_TEXT), new Utils.PrintFilter());

        topology.newStream("stream2", spout)
                .groupBy(new Fields(LOCATION))
                .aggregate(new Fields(LOCATION), new LocationAggregator(), new Fields(COUNT))
                .each(new Fields(LOCATION, COUNT), new Utils.PrintFilter());

        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();

        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY, conf, buildTopology(drpc));

        backtype.storm.utils.Utils.sleep(20000);
        cluster.killTopology(TOPOLOGY);
        cluster.shutdown();
    }

    private static class PerActorTweetsFilter extends BaseFilter {
        private final String actor;

        public PerActorTweetsFilter(String actor) {
            this.actor = actor;
        }

        @Override
        public boolean isKeep(TridentTuple tridentTuple) {
            return tridentTuple.getString(0).equals(actor);
        }
    }

    private static class UppercaseFunction extends BaseFunction {
        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            tridentCollector.emit(new Values(tridentTuple.getString(0).toUpperCase()));
        }
    }

    private static class LocationAggregator extends BaseAggregator<Map<String, Integer>> {

        static HashMap<String, Integer> map = new HashMap<String, Integer>();

        @Override
        public Map<String, Integer> init(Object o, TridentCollector tridentCollector) {
            return map;
        }

        @Override
        public void aggregate(Map<String, Integer> stringIntegerMap, TridentTuple tridentTuple, TridentCollector tridentCollector) {
            String location = tridentTuple.getString(0);
            stringIntegerMap.put(location, MapUtils.getInteger(stringIntegerMap, location, 0) + 1);
        }

        @Override
        public void complete(Map<String, Integer> stringIntegerMap, TridentCollector tridentCollector) {
            tridentCollector.emit(new Values(stringIntegerMap));
        }
    }
}