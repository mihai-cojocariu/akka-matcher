package akkaexample;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;


public class App
{

}

class Trade implements Serializable{
    private String reference;
    private String exchangeReference;
    //someData is a payload to make the Trade class memory footprint more realistic
    private List<String> someData = new ArrayList<String>();
    public Trade(String reference, String exchangeReference) {
        this.reference = reference;
        this.exchangeReference = exchangeReference;
        for (int i=0;i<20;i++){
            someData.add(randomAlphabetic(10));
        }

    }
    public String getReference() {
        return reference;
    }
    public String getExchangeReference() {
        return exchangeReference;
    }
}

class CcpTrade implements Serializable{
    private String exchangeReference;
    //someData is a payload to make the CcpTrade class memory footprint more realistic
    private List<String> someData = new ArrayList<String>();
    public CcpTrade(String exchangeReference) {
        this.exchangeReference = exchangeReference;
        for (int i=0;i<20;i++){
            someData.add(randomAlphabetic(10));
        }

    }
    public String getExchangeReference() {
        return exchangeReference;
    }
}

class NewTradeMessage implements Serializable {
    private Trade trade;
    public NewTradeMessage(Trade trade){
        this.trade = trade;
    }
    public Trade getTrade() {
        return trade;
    }
}
class CancelTradeMessage implements Serializable {
    private Trade trade;
    public CancelTradeMessage(Trade trade){
        this.trade = trade;
    }
    public Trade getTrade() {
        return trade;
    }
}

class NewCcpTradeMessage implements Serializable {
    private CcpTrade trade;
    public NewCcpTradeMessage(CcpTrade trade){
        this.trade = trade;
    }
    public CcpTrade getTrade() {
        return trade;
    }
}

class GetTradesMessage implements Serializable {}
class GetCcpTradesMessage implements Serializable {}

class TradesMessage implements Serializable {
    private List<Trade> trades;
    public TradesMessage(List<Trade> trades){
        this.trades = trades;
    }
    public List<Trade> getTrades() {
        return trades;
    }
}

class CcpTradesMessage implements Serializable {
    private List<CcpTrade> ccpTrades;
    public CcpTradesMessage(List<CcpTrade> ccpTrades){
        this.ccpTrades = ccpTrades;
    }
    public List<CcpTrade> getCcpTrades() {
        return ccpTrades;
    }
}

class GetUnmatchedMessage implements Serializable {
    private MatchMethod matchMethod;
    private Integer maxNumberOfActors;
    private Integer chunkSize;

    public GetUnmatchedMessage(MatchMethod matchMethod, Integer maxNumberOfActors, Integer chunkSize) {
        this.matchMethod = matchMethod;
        this.maxNumberOfActors = maxNumberOfActors;
        this.chunkSize = chunkSize;
    }

    public GetUnmatchedMessage() {
        this(MatchMethod.SINGLE_ACTOR, null, null);
    }

    public MatchMethod getMatchMethod() {
        return matchMethod;
    }

    public Integer getMaxNumberOfActors() {
        return maxNumberOfActors;
    }

    public Integer getChunkSize() {
        return chunkSize;
    }
}

class MatchResultsMessage implements Serializable {
    private UnmatchedItems unmatchedItems;
    public MatchResultsMessage(UnmatchedItems unmatchedItems){
        this.unmatchedItems = unmatchedItems;
    }

    public UnmatchedItems getUnmatchedItems() {
        return unmatchedItems;
    }
}

class OneWayMatchMessage implements Serializable {
    private Map<String, Trade> tradesMap;
    private Map<String, CcpTrade> ccpTradesMap;
    private TradesType tradesType;
    private Integer maxNumberOfActors;
    private Integer chunkSize;

    public OneWayMatchMessage(Map<String, Trade> tradesMap, Map<String, CcpTrade> ccpTradesMap, TradesType tradesType, Integer maxNumberOfActors, Integer chunkSize) {
        this.tradesMap = tradesMap;
        this.ccpTradesMap = ccpTradesMap;
        this.tradesType = tradesType;
        this.maxNumberOfActors = maxNumberOfActors;
        this.chunkSize = chunkSize;
    }

    public Map<String, Trade> getTradesMap() {
        return tradesMap;
    }

    public Map<String, CcpTrade> getCcpTradesMap() {
        return ccpTradesMap;
    }

    public TradesType getTradesType() {
        return tradesType;
    }

    public Integer getMaxNumberOfActors() {
        return maxNumberOfActors;
    }

    public Integer getChunkSize() {
        return chunkSize;
    }
}

class OneWayMatchResultsMessage implements Serializable {
    private UnmatchedItems unmatchedItems;
    private TradesType tradesType;

    public OneWayMatchResultsMessage(UnmatchedItems unmatchedItems, TradesType tradesType){
        this.unmatchedItems = unmatchedItems;
        this.tradesType = tradesType;
    }

    public UnmatchedItems getUnmatchedItems() {
        return unmatchedItems;
    }

    public TradesType getTradesType() {
        return tradesType;
    }
}

class PartialMatchMessage<T, S> implements Serializable {
    private List<T> tradesList;
    private Map<String, S> tradesMap;
    private TradesType tradesType;

    public PartialMatchMessage(List<T> tradesList, Map<String, S> tradesMap, TradesType tradesType) {
        this.tradesList = tradesList;
        this.tradesMap = tradesMap;
        this.tradesType = tradesType;
    }

    public List<T> getTradesList() {
        return tradesList;
    }

    public Map<String, S> getTradesMap() {
        return tradesMap;
    }

    public TradesType getTradesType() {
        return tradesType;
    }
}
