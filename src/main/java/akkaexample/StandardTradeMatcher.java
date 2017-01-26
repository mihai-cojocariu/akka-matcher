package akkaexample;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;

/**
 * Created by mcojocariu on 1/26/2017.
 */
public class StandardTradeMatcher {
    Map<String, Trade> trades = new HashMap<>();
    Map<String, CcpTrade> ccpTrades = new HashMap<>();

    public static void main(String[] args) {
        int numberOfTrades = 500000;

        StandardTradeMatcher standardTradeMatcher = new StandardTradeMatcher();

        Long startLoadTimestamp = System.currentTimeMillis();
        standardTradeMatcher.loadTrades(numberOfTrades);
        Long endLoadTimestamp = System.currentTimeMillis();

        Long diff = endLoadTimestamp - startLoadTimestamp;
        System.out.println("Trades loading duration (ms): " + diff);


        Long startMatchTimestamp = System.currentTimeMillis();
        standardTradeMatcher.performMatch();
        Long endMatchTimestamp = System.currentTimeMillis();


        diff = endMatchTimestamp - startMatchTimestamp;
        System.out.println("Trades matching duration (ms): " + diff);
    }

    private void loadTrades(int numberOfTrades) {
        Integer tradeExchangeReference = 0;
        Integer ccpTradeExchangeReference = 0;

        for(int i=1; i<=numberOfTrades; i++) {
            tradeExchangeReference += 2;
            Trade trade = new Trade(randomAlphabetic(10), tradeExchangeReference.toString());
            trades.put(trade.getExchangeReference(), trade);

            ccpTradeExchangeReference += 3;
            CcpTrade ccpTrade = new CcpTrade(ccpTradeExchangeReference.toString());
            ccpTrades.put(ccpTrade.getExchangeReference(), ccpTrade);
        }
    }

    private UnmatchedItems performMatch() {
        List<Trade> unmatchedTrades = new ArrayList<>();
        List<CcpTrade> unmatchedCcpTrades = new ArrayList<>();

        System.out.println("Starting matching for " + trades.size() + " trades and " + ccpTrades.size() + " ccp trades");

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

        System.out.println("We have " + (trades.size() - unmatchedTrades.size()) + " matches and " + (unmatchedTrades.size() + unmatchedCcpTrades.size()) + " unmatched");
        System.out.println("Memory " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));

        return new UnmatchedItems(unmatchedTrades, unmatchedCcpTrades);
    }
}
