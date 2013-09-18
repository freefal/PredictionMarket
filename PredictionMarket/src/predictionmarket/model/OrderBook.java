package predictionmarket.model;
import java.util.*;

public class OrderBook {
	private HashMap<Long, OrderBookSecurity> obs;
	ArrayList<Security> securities;
	HashMap<Long,Integer> mapSec;
	HashMap<Long,Long> orderSecurityMap;
	
	public OrderBook (ArrayList<Security> securities) {
		this.securities = securities;
		obs = new HashMap<Long, OrderBookSecurity>();
		mapSec = new HashMap<Long, Integer>();
		orderSecurityMap = new HashMap<Long,Long>();
		for (Security s : securities) {
			obs.put(s.id, new OrderBookSecurity(s));
			mapSec.put(s.id, 1);
		}
	}
	
	public OrderBookSecurity getOB (long security) {
		return obs.get(security);
	}
	
	public boolean secExists (long security) {
		return mapSec.containsKey(security);
	}
	
	public void putOrderSecurity (long order, long security) {
		orderSecurityMap.put(order, security);
	}
	
	public long getOrderSecurity (long order) {
		return orderSecurityMap.get(order);
	}

	public Long removeOrder (Order order) {
		Long security = getOrderSecurity(order.id);
		if (security == null)
			return null;
		OrderBookSecurity obs = getOB (security);
		if (obs == null)
			return null;
		Order o = obs.remove(order);
		if (o == null)
			return null;
		return o.id;
	}
}
