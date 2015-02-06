package com.betssontech.realtime;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.joda.time.DateTime;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by jelu on 2015-01-30.
 */
public class TridentTest {

    public static final String JOE = "joe";
    public static final String JANE = "jane";
    public static final String JEN = "jen";
    public static final String JIM = "jim";
    public static final String BET = "bet";
    public static final String WIN = "win";
    public static final String DEPOSIT = "deposit";
    public static final String REGISTER = "register";
    public static final String LIVE = "live";
    public static final String TABLE = "table";

    public static void main(String[] args) {

        final String T0 = "2015-01-02T14:00:00";
        final String T1 = "2015-01-02T14:05:00";
        final String T2 = "2015-01-02T14:10:00";
        final String T3 = "2015-01-02T14:15:00";
        final String T4 = "2015-01-02T14:20:00";


        FixedBatchSpout registrations = new FixedBatchSpout(new Fields("op", "user", "timestamp"), 3,
                new Values(REGISTER, JOE, nextTime()),
                new Values(REGISTER, JANE, nextTime()),
                new Values(REGISTER, JIM, nextTime()),
                new Values(REGISTER, JEN, nextTime())
        );

        FixedBatchSpout deposits = new FixedBatchSpout(new Fields("op", "user", "timestamp", "amount"), 30,
                new Values(DEPOSIT, JOE, nextTime(), 100),
                new Values(DEPOSIT, JANE, nextTime(), 10),
                new Values(DEPOSIT, JEN, nextTime(), 50),
                new Values(DEPOSIT, JEN, nextTime(), 100),
                new Values(DEPOSIT, JEN, nextTime(), 30)
        );

        FixedBatchSpout transactionsSpout = new FixedBatchSpout(new Fields("op", "user", "game", "timestamp", "amount"), 30,
                new Values(BET, JOE, LIVE, nextTime(), 1),
                new Values(BET, JOE, TABLE, nextTime(), 2),
                new Values(WIN, JOE, TABLE, nextTime(), 2),
                new Values(BET, JOE, TABLE, nextTime(), 1),
                new Values(BET, JOE, TABLE, nextTime(), 1),
                new Values(BET, JOE, TABLE, nextTime(), 1),
                new Values(BET, JOE, TABLE, nextTime(), 2),
                new Values(BET, JOE, LIVE, nextTime(), 2),
                new Values(WIN, JOE, LIVE, nextTime(), 5),
                new Values(BET, JOE, LIVE, nextTime(), 2),
                new Values(BET, JOE, LIVE, nextTime(), 2),
                new Values(BET, JANE, LIVE, nextTime(), 2),
                new Values(BET, JANE, LIVE, nextTime(), 2),
                new Values(BET, JANE, LIVE, nextTime(), 2),
                new Values(BET, JANE, LIVE, nextTime(), 2),
                new Values(BET, JEN, LIVE, nextTime(), 2),
                new Values(BET, JOE, LIVE, nextTime(), 2)
        );

        TridentTopology topology = new TridentTopology();


        Stream depositStream = topology.newStream("deposits", deposits)
                .partitionBy(new Fields("user"))
                .groupBy(new Fields("user"))
                .aggregate(new Fields("amount"), new Sum(), new Fields("sum_deposits"));

        // All transactions, filter only bet (not win)
        Stream transactionStream = topology.newStream("transactions", transactionsSpout)
                .partitionBy(new Fields("user"))
                .each(new Fields("op", "user", "amount"), new BetFilter());

        // Calculate avg bet
        Stream transAggStream = transactionStream
                .groupBy(new Fields("user"))
                .chainedAgg()
                .partitionAggregate(new Fields("user"), new Count(), new Fields("count"))
                .partitionAggregate(new Fields("amount"), new Sum(), new Fields("sum"))
                .chainEnd()
                .each(new Fields("sum", "count"), new Divide(), new Fields("avgbet"));

        // Get first game type
        Stream firstGameStream = transactionStream
                .groupBy(new Fields("user"))
                .aggregate(new Fields("game"), new FirstGameAgg(), new Fields("firstgame"));

        // Join avg and first game
        final Stream stream = topology.join(transAggStream, new Fields("user"), firstGameStream, new Fields("user"), new Fields("user", "count", "sum", "avgbet", "firstgame"));

        // Output all users, game and avg bet
        stream.each(new Fields("user", "firstgame", "avgbet"), new com.betssontech.realtime.Utils.PrintFilter());


        // Add deposits
        final Stream transAndDeposits = topology.join(stream, new Fields("user"), depositStream, new Fields("user"), new Fields("user", "count", "sum", "avgbet", "firstgame", "sum_deposits"));

        transAndDeposits.each(new Fields("user", "firstgame", "avgbet", "sum_deposits"), new com.betssontech.realtime.Utils.PrintFilter());



        // Set up and run a local cluster for testing

        Config conf = new Config();
        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, topology.build());
        Utils.sleep(5000);
        cluster.killTopology("test");
        cluster.shutdown();
    }

    static DateTime T0 = DateTime.now();

    private static String nextTime() {
        T0 = T0.plusSeconds((int) (Math.random() * 20));
        return T0.toDateTimeISO().toString();
    }


    private static class BetFilter extends BaseFilter {
        @Override
        public boolean isKeep(TridentTuple tuple) {
            return tuple.getString(0).equals(BET);
        }
    }

    private static class Divide extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            Long a = tuple.getLong(0);
            Long b = tuple.getLong(1);
            Double d = a.doubleValue() / b.doubleValue();
            collector.emit(new Values(d));
        }
    }

    private static class FirstGameAgg implements CombinerAggregator<String> {
        private String value;

        @Override
        public String init(TridentTuple tuple) {
            value = tuple.getStringByField("game");
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
}
