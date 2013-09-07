package predictionmarket.model;
import java.util.*;

public class OrderBook {
	private HashMap<Long, OrderBookSecurity> obs;
	ArrayList<Security> securities;
	HashMap<Long,Integer> mapSec;
	
	public OrderBook (ArrayList<Security> securities) {
		this.securities = securities;
		obs = new HashMap<Long, OrderBookSecurity>();
		mapSec = new HashMap<Long, Integer>();
		for (Security s : securities) {
			obs.put(s.id, new OrderBookSecurity(s));
			mapSec.put(s.id, 1);
		}
	}
	
	public OrderBookSecurity getOB (long security) {
		return obs.get(security);
	}
	
	public boolean secExists(long security) {
		return mapSec.containsKey(security);
	}
}
