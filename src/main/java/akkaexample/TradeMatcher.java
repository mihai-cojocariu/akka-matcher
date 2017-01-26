package akkaexample;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.ActorRefRoutee;
import akka.routing.RoundRobinRoutingLogic;
import akka.routing.Routee;
import akka.routing.Router;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TradeMatcher extends UntypedActor {
    private Map<String, Trade> trades = new HashMap<>();
    private Map<String, CcpTrade> ccpTrades = new HashMap<>();
    private List<Trade> unmatchedTrades = new ArrayList<>();
    private List<CcpTrade> unmatchedCcpTrades = new ArrayList<>();
    private int oneWayResponsesLeft = 2;
    LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    ActorRef sender;

    public void onReceive(Object message) throws Throwable {
        if (message instanceof NewTradeMessage){
            Trade trade = ((NewTradeMessage) message).getTrade();
            trades.put(trade.getExchangeReference(), trade);

        } else if (message instanceof CancelTradeMessage){
            trades.remove(((CancelTradeMessage) message).getTrade().getExchangeReference());

        } else if (message instanceof NewCcpTradeMessage){
            CcpTrade ccpTrade = ((NewCcpTradeMessage) message).getTrade();
            ccpTrades.put(ccpTrade.getExchangeReference(), ccpTrade);

        } else if (message instanceof GetTradesMessage){
            getSender().tell(new TradesMessage(new ArrayList<Trade>(trades.values())), getSelf());
        } else if (message instanceof GetCcpTradesMessage){
            getSender().tell(new CcpTradesMessage(new ArrayList<CcpTrade>(ccpTrades.values())), getSelf());
        } else if (message instanceof GetUnmatchedMessage){
            GetUnmatchedMessage unmatchedMessage = (GetUnmatchedMessage) message;
            if (MatchMethod.SINGLE_ACTOR.equals(unmatchedMessage.getMatchMethod())) {
                getSender().tell(new MatchResultsMessage(performMatch()), getSelf());
            } else {
                performMatchMultipleActors(unmatchedMessage);
            }
        } else if (message instanceof OneWayMatchResultsMessage) {
            handleMatchResult((OneWayMatchResultsMessage) message);
        }
        else unhandled(message);
    }

    private void performMatchMultipleActors(GetUnmatchedMessage message) {
        sender = getSender();

        log.debug("Starting matching for {} trades and {} ccp trades", trades.size(), ccpTrades.size());

        List<Routee> routeeList = new ArrayList<>();
        for (int i=1; i<=2; i++) {
            ActorRef actorRef = getContext().actorOf(Props.create(TradeMatcherOneWay.class));
            routeeList.add(new ActorRefRoutee(actorRef));
        }

        Router router = new Router(new RoundRobinRoutingLogic(), routeeList);

        OneWayMatchMessage tradesMatchMessage = new OneWayMatchMessage(trades, ccpTrades, TradesType.TRADE, message.getMaxNumberOfActors(), message.getChunkSize());
        router.route(tradesMatchMessage, getSelf());

        OneWayMatchMessage ccpTradesMatchMessage = new OneWayMatchMessage(trades, ccpTrades, TradesType.CCP_TRADE, message.getMaxNumberOfActors(), message.getChunkSize());
        router.route(ccpTradesMatchMessage, getSelf());
    }

    private void handleMatchResult(OneWayMatchResultsMessage message) {
        oneWayResponsesLeft--;

        if (TradesType.TRADE.equals(message.getTradesType())) {
            unmatchedTrades = message.getUnmatchedItems().getTrades();
        } else {
            unmatchedCcpTrades = message.getUnmatchedItems().getCcpTrades();
        }

        if (oneWayResponsesLeft == 0) {
            log.debug("We have {} matches and {} unmatched", trades.size() - unmatchedTrades.size(), unmatchedTrades.size() + unmatchedCcpTrades.size() );
            log.debug("Memory {}", Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());

            UnmatchedItems unmatchedItems = new UnmatchedItems(unmatchedTrades, unmatchedCcpTrades);
            sender.tell(new MatchResultsMessage(unmatchedItems), getSelf());
        }
    }

    private UnmatchedItems performMatch() {
        List<Trade> unmatchedTrades = new ArrayList<Trade>();
        List<CcpTrade> unmatchedCcpTrades = new ArrayList<CcpTrade>();
        log.debug("Starting matching for {} trades and {} ccp trades", trades.size(), ccpTrades.size());

        for (Trade trade : trades.values()){
            if (!ccpTrades.containsKey(trade.getExchangeReference())){
                unmatchedTrades.add(trade);
            }
            Utils.delayExec();
        }

        for (CcpTrade ccpTrade : ccpTrades.values()){
            if (!trades.containsKey(ccpTrade.getExchangeReference())){
                unmatchedCcpTrades.add(ccpTrade);
            }
            Utils.delayExec();
        }
        log.debug("We have {} matches and {} unmatched", trades.size() - unmatchedTrades.size(),
                unmatchedTrades.size() + unmatchedCcpTrades.size() );

        log.debug("Memory {}", Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
        return new UnmatchedItems(unmatchedTrades, unmatchedCcpTrades);
    }
}

class UnmatchedItems {
    private List<Trade> trades = new ArrayList<Trade>();
    private List<CcpTrade> ccpTrades = new ArrayList<CcpTrade>();
    public UnmatchedItems() {

    }

    public UnmatchedItems(List<Trade> trades, List<CcpTrade> ccpTrades){
        this.trades = trades;
        this.ccpTrades = ccpTrades;
    }

    public List<Trade> getTrades() {
        return trades;
    }

    public List<CcpTrade> getCcpTrades() {
        return ccpTrades;
    }
}
