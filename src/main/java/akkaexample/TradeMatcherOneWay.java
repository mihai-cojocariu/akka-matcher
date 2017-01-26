package akkaexample;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.routing.ActorRefRoutee;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by mcojocariu on 1/24/2017.
 */
public class TradeMatcherOneWay extends UntypedActor {
    UnmatchedItems unmatchedItems = new UnmatchedItems();

    private ActorRef oneWaySenderActorRef;
    private TradesType tradesType;
    private int numberOfActors = 50;
    private int chunkSize;
    private int numberOfChunks;
    private int partialMatchResponsesLeft = 0;
    Router router;

    public void onReceive(Object message) throws Throwable {
        if (message instanceof OneWayMatchMessage) {
            performMatchMultipleActors((OneWayMatchMessage) message);
        } else if (message instanceof MatchResultsMessage) {
            handleMatchMultipleActorsResult((MatchResultsMessage) message);
        } else {
            unhandled(message);
        }
    }

    private void performMatchMultipleActors(OneWayMatchMessage oneWayMatchMessage) {
        oneWaySenderActorRef = getSender();
        numberOfActors = oneWayMatchMessage.getMaxNumberOfActors();
        chunkSize = oneWayMatchMessage.getChunkSize();

        Map<String, Trade> tradeMap = oneWayMatchMessage.getTradesMap();
        Map<String, CcpTrade> ccpTradeMap = oneWayMatchMessage.getCcpTradesMap();

        tradesType = oneWayMatchMessage.getTradesType();

        List tradesToBeMatched;
        Map tradesMapToBeMatchedAgainst;
        if (TradesType.TRADE.equals(tradesType)) {
            tradesToBeMatched = new ArrayList<>(tradeMap.values());
            tradesMapToBeMatchedAgainst = ccpTradeMap;
        } else {
            tradesToBeMatched = new ArrayList<>(ccpTradeMap.values());
            tradesMapToBeMatchedAgainst = tradeMap;
        }

        numberOfChunks = (int) Math.ceil((double) tradesToBeMatched.size() / chunkSize);

        partialMatchResponsesLeft = numberOfChunks;

        if (numberOfChunks < numberOfActors) {
            numberOfActors = numberOfChunks;
        }

        setUpRouter();

        for (int i=1; i<=numberOfChunks; i++) {
            int startIndex = (i - 1) * chunkSize;
            int endIndex = i * chunkSize;
            if (endIndex > tradesToBeMatched.size()) {
                endIndex = tradesToBeMatched.size();
            }

            PartialMatchMessage partialMatchMessage = new PartialMatchMessage(tradesToBeMatched.subList(startIndex, endIndex), tradesMapToBeMatchedAgainst, tradesType);
            router.route(partialMatchMessage, getSelf());
        }
    }

    private void handleMatchMultipleActorsResult(MatchResultsMessage matchResultsMessage) {
        partialMatchResponsesLeft--;

        UnmatchedItems unmatchedItemsChunk = matchResultsMessage.getUnmatchedItems();
        if (TradesType.TRADE.equals(tradesType)) {
            unmatchedItems.getTrades().addAll(unmatchedItemsChunk.getTrades());
        } else {
            unmatchedItems.getCcpTrades().addAll(unmatchedItemsChunk.getCcpTrades());
        }

        if (partialMatchResponsesLeft == 0) {
            OneWayMatchResultsMessage oneWayMatchResultsMessage = new OneWayMatchResultsMessage(unmatchedItems, tradesType);
            oneWaySenderActorRef.tell(oneWayMatchResultsMessage, getSelf());
        }

        adjustRouter();
    }

    private ActorRef getWorkerActor() {
        return getContext().actorOf(Props.create(TradeMatcherWorker.class));
    }

    private void setUpRouter() {
        List<Routee> routeeList = new ArrayList<>();

        for (int i=1; i<=numberOfActors; i++) {
            ActorRef actorRef = getWorkerActor();
            getContext().watch(actorRef);
            routeeList.add(new ActorRefRoutee(actorRef));
        }

        router = new Router(new RoundRobinRoutingLogic(), routeeList);
    }

    private void adjustRouter() {
        router = router.removeRoutee(getSelf());

        ActorRef actorRef = getWorkerActor();
        getContext().watch(actorRef);
        router = router.addRoutee(new ActorRefRoutee(actorRef));
    }
}
