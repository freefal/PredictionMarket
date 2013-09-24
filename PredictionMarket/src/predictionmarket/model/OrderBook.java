package predictionmarket.model;
import java.util.*;

public class OrderBook {
	private HashMap<Long, OrderBookSecurity> obsecurities; // Maps security id to order book for that security
	ArrayList<Security> securities; // List of all securities currently available for trading
	HashMap<Long,Security> mapSec; // Efficient data structure to look up a Security object from its id
	HashMap<Long,Long> orderSecurityMap; // Maps order id to the security order book containing that order
	
	public OrderBook (ArrayList<Security> securities) {
		this.securities = securities;
		obsecurities = new HashMap<Long, OrderBookSecurity>();
		mapSec = new HashMap<Long, Security>();
		orderSecurityMap = new HashMap<Long,Long>();
		for (Security s : securities) {
			obsecurities.put(s.id, new OrderBookSecurity(s));
			mapSec.put(s.id, s);
		}
	}
	
	public OrderBookSecurity getOB (long security) {
		return obsecurities.get(security);
	}
	
	public boolean secExists (long security) {
		return mapSec.containsKey(security);
	}
	
	public Security getSecurity (long security) {
		return mapSec.get(security);
	}
	
	public void putOrderSecurity (long order, long security) {
		orderSecurityMap.put(order, security);
	}
	
	public long getOrderSecurity (long order) {
		return orderSecurityMap.get(order);
	}
	
	public ArrayList<Order> getUserOrders(long user) {
		ArrayList<Order> orders = new ArrayList<Order>();
		for (Security s : securities) {
			OrderBookSecurity obs = obsecurities.get(s.id);
			orders.addAll(obs.getUserOrders(user));
		}
		return orders;
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
