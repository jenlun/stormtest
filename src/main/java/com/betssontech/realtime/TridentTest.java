package com.betssontech.realtime;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.joda.time.DateTime;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;

import java.io.Serializable;

public class TridentTest implements Serializable{

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
    public static final String OTHER = "other";
    public static final String FIRST_DEPOSIT_TIME = "first_deposit_time";
    public static final String TIMESTAMP = "timestamp";
    public static final String USER = "user";
    public static final String LAST_TRANS_TIME = "last_transaction_time";
    public static final String NUMBER_OF_BETS = "number_of_bets";
    public static final String AVERAGE_BET = "average_bet";
    public static final String FIRST_TYPE_OF_GAME_PLAYED = "first_type_of_game_played";
    public static final String SUM_DEPOSITS = "sum_deposits";
    public static final String SUM_TRANS = "sum_trans";
    public static final String GAME = "game";
    public static final String AMOUNT = "amount";
    public static final String EVENT_TYPE = "event_type";
    private DateTime T0 = DateTime.now();

    public static void main(String[] args) {

        // Set up and run a local cluster for testing
        Config conf = new Config();
        conf.setDebug(false);

        TridentTest tt = new TridentTest();

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, tt.getTopology().build());
        Utils.sleep(5000);
        cluster.killTopology("test");
        cluster.shutdown();
    }

    public TridentTopology getTopology(){
        FixedBatchSpout registrations = new FixedBatchSpout(new Fields(EVENT_TYPE, USER, TIMESTAMP), 3,
                new Values(REGISTER, JOE, nextTime()),
                new Values(REGISTER, JANE, nextTime()),
                new Values(REGISTER, JIM, nextTime()),
                new Values(REGISTER, JEN, nextTime())
        );

        FixedBatchSpout deposits = new FixedBatchSpout(new Fields(EVENT_TYPE, USER, TIMESTAMP, AMOUNT), 30,
                new Values(DEPOSIT, JOE, nextTime(), 100),
                new Values(DEPOSIT, JANE, nextTime(), 10),
                new Values(DEPOSIT, JEN, nextTime(), 50),
                new Values(DEPOSIT, JEN, nextTime(), 100),
                new Values(DEPOSIT, JEN, nextTime(), 30)
        );

        FixedBatchSpout transactionsSpout = new FixedBatchSpout(new Fields(EVENT_TYPE, USER, GAME, TIMESTAMP, AMOUNT), 30,
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
                new Values(BET, JEN, OTHER, nextTime(), 2),
                new Values(BET, JEN, OTHER, nextTime(), 20),
                new Values(BET, JOE, LIVE, nextTime(), 2)
        );

        TridentTopology topology = new TridentTopology();


        Stream rawDeposits = topology.newStream("deposits", deposits)
                .partitionBy(new Fields(USER));

        Stream sumDeposits = rawDeposits
                .groupBy(new Fields(USER))
                .aggregate(new Fields(AMOUNT), new Sum(), new Fields(SUM_DEPOSITS));

        Stream firstDeposit = rawDeposits
                .groupBy(new Fields(USER))
                .aggregate(new Fields(TIMESTAMP), new FirstInstanceAggregator(TIMESTAMP), new Fields(FIRST_DEPOSIT_TIME));

        // sum of deposits and timestamp for first deposit
        Stream depositStream = topology.join(firstDeposit, new Fields(USER), sumDeposits, new Fields(USER), new Fields(USER, FIRST_DEPOSIT_TIME, SUM_DEPOSITS));

        // All transactions, filter only bet (not win)
        Stream transactionStream = topology.newStream("transactions", transactionsSpout)
                .partitionBy(new Fields(USER))
                .each(new Fields(EVENT_TYPE, USER, AMOUNT), new BetFilter());

        // Calculate avg bet
        Stream transAggStream = transactionStream
                .groupBy(new Fields(USER))
                .chainedAgg()
                .partitionAggregate(new Fields(AMOUNT), new Sum(), new Fields(SUM_TRANS))
                .partitionAggregate(new Fields(USER), new Count(), new Fields(NUMBER_OF_BETS))
                .partitionAggregate(new Fields(TIMESTAMP), new LastInstanceAggregator(TIMESTAMP), new Fields(LAST_TRANS_TIME))
                .chainEnd()
                .each(new Fields(SUM_TRANS, NUMBER_OF_BETS), new Divide(), new Fields(AVERAGE_BET));

        // Get first game type
        Stream firstGameStream = transactionStream
                .groupBy(new Fields(USER))
                .aggregate(new Fields(GAME), new FirstInstanceAggregator(GAME), new Fields(FIRST_TYPE_OF_GAME_PLAYED));

        // Join avg and first game
        final Stream stream = topology
                .join(transAggStream, new Fields(USER), firstGameStream, new Fields(USER), new Fields(USER, SUM_TRANS, NUMBER_OF_BETS, LAST_TRANS_TIME, AVERAGE_BET, FIRST_TYPE_OF_GAME_PLAYED));

        // Add deposits
        final Stream transAndDeposits = topology.join(stream, new Fields(USER), depositStream, new Fields(USER), new Fields(USER, SUM_TRANS, NUMBER_OF_BETS, LAST_TRANS_TIME, AVERAGE_BET, FIRST_TYPE_OF_GAME_PLAYED, FIRST_DEPOSIT_TIME, SUM_DEPOSITS));

        // output all fields
        transAndDeposits.each(new Fields(USER, FIRST_TYPE_OF_GAME_PLAYED, AVERAGE_BET, NUMBER_OF_BETS, LAST_TRANS_TIME, FIRST_DEPOSIT_TIME, SUM_DEPOSITS), new com.betssontech.realtime.Utils.PrintFilter());
        return topology;
    }

    private String nextTime() {
        T0 = T0.plusSeconds((int) (Math.random() * 20));
        return T0.toDateTimeISO().toString();
    }


}
