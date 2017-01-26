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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Created by mcojocariu on 1/25/2017.
 */
public class TradeMatcherOneWayTest {
    static ActorSystem system;
    LoggingAdapter log = Logging.getLogger(system, this);
    static TestActorRef<TradeMatcherWorker> matcher;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
        matcher = TestActorRef.create(system, Props.create(TradeMatcherOneWay.class), "TradeMatcherOneWay");
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
                matcher.tell(getOneWayMatchMessage(2000, 20, 1000, TradesType.TRADE), getTestActor());

                OneWayMatchResultsMessage oneWayMatchResultsMessage = expectMsgClass(new FiniteDuration(5, TimeUnit.SECONDS), OneWayMatchResultsMessage.class);
                UnmatchedItems unmatchedItems = oneWayMatchResultsMessage.getUnmatchedItems();
                List<Trade> unmatchedTrades = unmatchedItems.getTrades();

                assertEquals(1000, unmatchedTrades.size());
            }
        };
    }

    @Test
    public void matchTest2() {
        new JavaTestKit(system) {
            {
                matcher.tell(getOneWayMatchMessage(150, 20, 100, TradesType.TRADE), getTestActor());

                OneWayMatchResultsMessage oneWayMatchResultsMessage = expectMsgClass(new FiniteDuration(60, TimeUnit.SECONDS), OneWayMatchResultsMessage.class);
                UnmatchedItems unmatchedItems = oneWayMatchResultsMessage.getUnmatchedItems();
                List<Trade> unmatchedTrades = unmatchedItems.getTrades();

                assertEquals(75, unmatchedTrades.size());
            }
        };
    }

    private OneWayMatchMessage getOneWayMatchMessage(int numberOfTrades, int maxNumberOfActors, int chunkSize, TradesType tradesType) {

        Map<String, Trade> tradesMap = new HashMap<>();
        Map<String, CcpTrade> ccpTradesMap = new HashMap<>();

        for (int i=1; i<=numberOfTrades; i++) {
            String tradeReference = "trade" + i;
            tradesMap.put(tradeReference, new Trade(tradeReference, tradeReference));

            String ccpTradeReference = "trade" + (i*2);
            ccpTradesMap.put(ccpTradeReference, new CcpTrade(ccpTradeReference));
        }

        return new OneWayMatchMessage(tradesMap, ccpTradesMap, tradesType, maxNumberOfActors, chunkSize);
    }
}
