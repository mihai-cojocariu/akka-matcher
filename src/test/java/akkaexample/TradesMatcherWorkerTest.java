package akkaexample;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.testkit.JavaTestKit;
import akka.testkit.TestActorRef;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Created by mcojocariu on 1/25/2017.
 */
public class TradesMatcherWorkerTest {
    static ActorSystem system;
    LoggingAdapter log = Logging.getLogger(system, this);
    static TestActorRef<TradeMatcherWorker> matcher;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
        matcher = TestActorRef.create(system, Props.create(TradeMatcherWorker.class), "TradeMatcherWorker");
    }

    @AfterClass
    public static void teardown() {
        system.shutdown();
        system.awaitTermination(Duration.create("10 seconds"));
    }

    @Test
    public void matchTest1() {
        new JavaTestKit(system) {
            {
                matcher.tell(getPartialMatchMessage(TradesType.TRADE), getTestActor());

                MatchResultsMessage matchResultsMessage = expectMsgClass(new FiniteDuration(5, TimeUnit.SECONDS), MatchResultsMessage.class);
                UnmatchedItems unmatchedItems = matchResultsMessage.getUnmatchedItems();
                List<Trade> unmatchedTrades = unmatchedItems.getTrades();

                assertEquals(1, unmatchedTrades.size());
            }
        };
    }

    @Test
    public void matchTest2() {
        new JavaTestKit(system) {
            {
                matcher.tell(getPartialMatchMessage(TradesType.CCP_TRADE), getTestActor());

                MatchResultsMessage matchResultsMessage = expectMsgClass(new FiniteDuration(5, TimeUnit.SECONDS), MatchResultsMessage.class);
                UnmatchedItems unmatchedItems = matchResultsMessage.getUnmatchedItems();
                List<CcpTrade> unmatchedCcpTrades = unmatchedItems.getCcpTrades();

                assertEquals(2, unmatchedCcpTrades.size());
            }
        };
    }

    private PartialMatchMessage getPartialMatchMessage(TradesType tradesType) {
        PartialMatchMessage partialMatchMessage;

        if (TradesType.TRADE.equals(tradesType)) {
            List<Trade> tradeList = new ArrayList<>();
            tradeList.add(new Trade("trade1", "trade1"));
            tradeList.add(new Trade("trade2", "trade2"));

            Map<String, CcpTrade> tradesMap = new HashMap<>();
            tradesMap.put("trade0", new CcpTrade("trade0"));
            tradesMap.put("trade1", new CcpTrade("trade1"));
            tradesMap.put("trade10", new CcpTrade("trade10"));

            partialMatchMessage = new PartialMatchMessage(tradeList, tradesMap, tradesType);
        } else {
            List<CcpTrade> tradeList = new ArrayList<>();
            tradeList.add(new CcpTrade("trade0"));
            tradeList.add(new CcpTrade("trade1"));
            tradeList.add(new CcpTrade("trade10"));

            Map<String, Trade> tradesMap = new HashMap<>();
            tradesMap.put("trade1", new Trade("trade1", "trade1"));
            tradesMap.put("trade2", new Trade("trade2", "trade2"));

            partialMatchMessage = new PartialMatchMessage(tradeList, tradesMap, tradesType);
        }

        return partialMatchMessage;
    }
}
