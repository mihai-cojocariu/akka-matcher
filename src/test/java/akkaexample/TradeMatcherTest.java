package akkaexample;

import akka.actor.ActorRef;
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

import java.util.concurrent.TimeUnit;

import static akkaexample.TestDataBuilder.aCcpTrade;
import static akkaexample.TestDataBuilder.aTrade;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.junit.Assert.assertEquals;

public class TradeMatcherTest{
    static ActorSystem system;
    LoggingAdapter log = Logging.getLogger(system, this);

    private static final Integer NUMBER_OF_TRADES = 30000;
    private static final Integer MAX_NUMBER_OF_ACTORS = 50;
    private static final Integer CHUNK_SIZE = 600;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        system.shutdown();
        system.awaitTermination(Duration.create("10 seconds"));
    }

    @Test
    public void canAddATrade() {
        new JavaTestKit(system) {{
            //TODO - can we create the actor ref once to avoid the hardcoding of the name?
            final TestActorRef<TradeMatcher> matcher =
                    TestActorRef.create(system, Props.create(TradeMatcher.class), "tradematcher1");

            final Trade trade = aTrade();
            matcher.tell(new NewTradeMessage(trade), getTestActor());
            matcher.tell(new GetTradesMessage(), getTestActor());

            final TradesMessage trades = expectMsgClass(TradesMessage.class);

            new Within(duration("10 seconds")) {
                protected void run() {
                    assertEquals(trade, trades.getTrades().get(0));
                }
            };
        }};
    }

    @Test
    public void canAddACcpTrade() {
        new JavaTestKit(system) {{
            final TestActorRef<TradeMatcher> matcher =
                    TestActorRef.create(system, Props.create(TradeMatcher.class), "tradematcher2");

            final CcpTrade ccpTrade = aCcpTrade();
            matcher.tell(new NewCcpTradeMessage(ccpTrade), getTestActor());
            matcher.tell(new GetCcpTradesMessage(), getTestActor());

            final CcpTradesMessage ccpTrades = expectMsgClass(CcpTradesMessage.class);

            new Within(duration("10 seconds")) {
                protected void run() {
                    assertEquals(ccpTrade, ccpTrades.getCcpTrades().get(0));
                }
            };
        }};
    }

    @Test
    public void canCancelATrade() {
        new JavaTestKit(system) {{
            final TestActorRef<TradeMatcher> matcher =
                    TestActorRef.create(system, Props.create(TradeMatcher.class), "tradematcher3");

            final Trade trade1 = aTrade();
            final Trade trade2 = aTrade();
            matcher.tell(new NewTradeMessage(trade1), getTestActor());
            matcher.tell(new NewTradeMessage(trade2), getTestActor());
            matcher.tell(new CancelTradeMessage(trade1), getTestActor());
            matcher.tell(new GetTradesMessage(), getTestActor());

            final TradesMessage trades = expectMsgClass(TradesMessage.class);

            new Within(duration("10 seconds")) {
                protected void run() {
                    assertEquals(1, trades.getTrades().size());
                    assertEquals(trade2, trades.getTrades().get(0));
                }
            };
        }};
    }

    @Test
    public void canPerformAMatch() {
        new JavaTestKit(system) {
            {
                final TestActorRef<TradeMatcher> matcher =
                        TestActorRef.create(system, Props.create(TradeMatcher.class), "tradematcher4");
                final Trade trade = aTrade();
                matcher.tell(new NewTradeMessage(trade), getTestActor());
                final CcpTrade ccpTrade = aCcpTrade(trade.getExchangeReference());
                matcher.tell(new NewCcpTradeMessage(ccpTrade), getTestActor());

                matcher.tell(new GetUnmatchedMessage(), getTestActor());

                final MatchResultsMessage results = expectMsgClass(MatchResultsMessage.class);

                new Within(duration("10 seconds")) {
                    protected void run() {
                        assertEquals(0, results.getUnmatchedItems().getTrades().size());
                        assertEquals(0, results.getUnmatchedItems().getCcpTrades().size());
                    }
                };
            }
        };
    }

    @Test
    public void canIdentifyAnUnmatchedCcpTrade(){
        new JavaTestKit(system) {
            {
                final TestActorRef<TradeMatcher> matcher =
                        TestActorRef.create(system, Props.create(TradeMatcher.class), "tradematcher5");
                final CcpTrade ccpTrade = aCcpTrade();
                matcher.tell(new NewCcpTradeMessage(ccpTrade), getTestActor());
                matcher.tell(new GetUnmatchedMessage(), getTestActor());

                final MatchResultsMessage results = expectMsgClass(MatchResultsMessage.class);

                new Within(duration("10 seconds")) {
                    protected void run() {
                        assertEquals(ccpTrade, results.getUnmatchedItems().getCcpTrades().get(0));
                    }
                };
            }
        };
    }

    @Test
    public void canIdentifyAnUnmatchedTrade(){
        new JavaTestKit(system) {
            {
                final TestActorRef<TradeMatcher> matcher =
                        TestActorRef.create(system, Props.create(TradeMatcher.class), "tradematcher6");
                final Trade trade = aTrade();
                matcher.tell(new NewTradeMessage(trade), getTestActor());
                matcher.tell(new GetUnmatchedMessage(), getTestActor());

                final MatchResultsMessage results = expectMsgClass(MatchResultsMessage.class);

                new Within(duration("10 seconds")) {
                    protected void run() {
                        assertEquals(trade, results.getUnmatchedItems().getTrades().get(0));
                    }
                };
            }
        };
    }

    @Test
    public void canIdentifyAnUnmatchPostCancel(){
        new JavaTestKit(system) {
            {
                final TestActorRef<TradeMatcher> matcher =
                        TestActorRef.create(system, Props.create(TradeMatcher.class), "tradematcher7");
                final Trade trade = aTrade();
                matcher.tell(new NewTradeMessage(trade), getTestActor());
                final CcpTrade ccpTrade = aCcpTrade(trade.getExchangeReference());
                matcher.tell(new NewCcpTradeMessage(ccpTrade), getTestActor());
                matcher.tell(new CancelTradeMessage(trade), getTestActor());

                matcher.tell(new GetUnmatchedMessage(), getTestActor());

                final MatchResultsMessage results = expectMsgClass(MatchResultsMessage.class);

                new Within(duration("10 seconds")) {
                    protected void run() {
                        assertEquals(ccpTrade, results.getUnmatchedItems().getCcpTrades().get(0));
                    }
                };
            }
        };
    }

    @Test
    public void volumeTestSingleActor() {
        new JavaTestKit(system) {
            {
                final TestActorRef<TradeMatcher> matcher = TestActorRef.create(system, Props.create(TradeMatcher.class), "tradematcher8");
                loadTrades(matcher, getTestActor(), NUMBER_OF_TRADES);

                Long startTimestamp = System.currentTimeMillis();

                matcher.tell(new GetUnmatchedMessage(MatchMethod.SINGLE_ACTOR, MAX_NUMBER_OF_ACTORS, CHUNK_SIZE), getTestActor());
                expectMsgClass(new FiniteDuration(10, TimeUnit.SECONDS), MatchResultsMessage.class);

                Long endTimestamp = System.currentTimeMillis();
                Long diff = endTimestamp - startTimestamp;
                log.debug("Trades matching duration (ms): " + diff);
            }
        };
    }

    @Test
    public void volumeTestMultipleActors() {
        new JavaTestKit(system) {
            {
                final TestActorRef<TradeMatcher> matcher = TestActorRef.create(system, Props.create(TradeMatcher.class), "tradematcher9");
                loadTrades(matcher, getTestActor(), NUMBER_OF_TRADES);

                Long startTimestamp = System.currentTimeMillis();

                matcher.tell(new GetUnmatchedMessage(MatchMethod.MULTIPLE_ACTORS, MAX_NUMBER_OF_ACTORS, CHUNK_SIZE), getTestActor());
                expectMsgClass(new FiniteDuration(20, TimeUnit.SECONDS), MatchResultsMessage.class);

                Long endTimestamp = System.currentTimeMillis();
                Long diff = endTimestamp - startTimestamp;
                log.debug("Trades matching duration (ms): " + diff);
            }
        };
    }

    private void loadTrades(ActorRef matcher, ActorRef testActor, Integer numberOfTrades) {
        Long startTimestamp = System.currentTimeMillis();

        Integer tradeExchangeReference = 0;
        Integer ccpTradeExchangeReference = 0;

        for(int i=1; i<=numberOfTrades; i++) {
            tradeExchangeReference += 2;
            Trade trade = new Trade(randomAlphabetic(10), tradeExchangeReference.toString());
            NewTradeMessage newTradeMessage = new NewTradeMessage(trade);
            matcher.tell(newTradeMessage, testActor);

            ccpTradeExchangeReference += 3;
            CcpTrade ccpTrade = new CcpTrade(ccpTradeExchangeReference.toString());
            NewCcpTradeMessage newCcpTradeMessage = new NewCcpTradeMessage(ccpTrade);
            matcher.tell(newCcpTradeMessage, testActor);
        }

        Long endTimestamp = System.currentTimeMillis();
        Long diff = endTimestamp - startTimestamp;
        log.debug("Trades loading duration (ms): " + diff);
    }
}