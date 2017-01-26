package akkaexample;

import akka.actor.UntypedActor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by mcojocariu on 1/24/2017.
 */
public class TradeMatcherWorker extends UntypedActor {
    public void onReceive(Object message) throws Throwable {
        if (message instanceof PartialMatchMessage) {
            PartialMatchMessage partialMatchMessage = (PartialMatchMessage) message;
            TradesType tradesType = partialMatchMessage.getTradesType();
            List tradesList = partialMatchMessage.getTradesList();
            Map tradesMap = partialMatchMessage.getTradesMap();

            List<Trade> unmatchedTrades = new ArrayList<>();
            List<CcpTrade> unmatchedCcpTrades = new ArrayList<>();

            if (TradesType.TRADE.equals(tradesType)) {
                for (Object o : tradesList) {
                    Trade trade = (Trade) o;
                    if (!tradesMap.containsKey(trade.getExchangeReference())) {
                        unmatchedTrades.add(trade);
                    }
                    Utils.delayExec();
                }
            } else {
                for (Object o : tradesList) {
                    CcpTrade ccpTrade = (CcpTrade) o;
                    if (!tradesMap.containsKey(ccpTrade.getExchangeReference())) {
                        unmatchedCcpTrades.add(ccpTrade);
                    }
                    Utils.delayExec();
                }
            }

            UnmatchedItems unmatchedItems = new UnmatchedItems(unmatchedTrades, unmatchedCcpTrades);
            MatchResultsMessage matchResultsMessage = new MatchResultsMessage(unmatchedItems);
            getSender().tell(matchResultsMessage, getSelf());
        } else {
            unhandled(message);
        }
    }
}
