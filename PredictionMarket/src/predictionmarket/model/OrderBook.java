package bitcoinoptions.exchange;
import java.util.*;

public class OrderBook {
	private HashMap<String, OrderBookSecurity> obs;
	String[] secList;
	HashMap<String,Integer> secMap;
	
	public OrderBook (String[] secList) {
		this.secList = secList;
		secMap = new HashMap<String,Integer>();
		for (String sec : secList) {
			secMap.put(sec, 1);
		}
		obs = new HashMap<String, OrderBookSecurity>();
		for (String sec : secList) {
			obs.put(sec, new OrderBookSecurity(sec));
		}
	}
	
	public boolean secExists (String security) {
		return secMap.get(security) != null;
	}
	
	public OrderBookSecurity getOB (String security) {
		return obs.get(security);
	}
}
