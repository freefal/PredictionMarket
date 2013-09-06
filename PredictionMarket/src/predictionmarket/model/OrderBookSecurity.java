package bitcoinoptions.exchange;

import java.util.*;

import org.json.*;

import bitcoinoptions.global.*;

public class OrderBookSecurity {
	private String security;
	private HashMap<Long, Order> idMap;
	private HashMap<Long, ArrayList<Order>> userMap;
	private PriorityQueue<Order> bids; 
	private PriorityQueue<Order> asks;
	
	public OrderBookSecurity (String security) {
		this.security = security;
		idMap = new HashMap<Long, Order>();
		userMap = new HashMap<Long, ArrayList<Order>>();
		this.bids = new PriorityQueue<Order>(100,new Comparator<Order>() {
			public int compare (Order o1, Order o2) {
				long diff = o2.price - o1.price;
				if (diff == 0)
					diff = o1.placed - o2.placed;
				return (int)(diff/Math.abs(diff));
			}
		});
		
		this.asks = new PriorityQueue<Order>(100,new Comparator<Order>() {
			public int compare (Order o1, Order o2) {
				long diff = o1.price - o2.price;
				if (diff == 0)
					diff = o1.placed - o2.placed;
				return (int)(diff/Math.abs(diff));
			}
		});
	}
	
	private synchronized void addOrder(Order order) {
		idMap.put(order.id, order);
		ArrayList<Order> orders = userMap.get(order.userID);
		if(orders == null) {
			orders = new ArrayList<Order>();
			userMap.put(order.userID, orders);
		}
		orders.add(order);
	}
	
	public synchronized void addBid(Order bid) {
		bids.add(bid);
		addOrder(bid);
	}
	
	public synchronized void addAsk(Order ask) {
		asks.add(ask);
		addOrder(ask);
	}
	
	public synchronized Order bestBid() {
		return bids.peek();
	}
	
	public synchronized Order bestAsk() {
		return asks.peek();
	}
	
	private synchronized void removeOrder(Order order) {
		idMap.remove(order.id);
		userMap.get(order.userID).remove(order);
	}
	
	public synchronized Order popBid() {
		Order order = bids.poll();
		if(order != null)
			removeOrder(order);
		return order;
	}
	
	public synchronized Order popAsk() {
		Order order = bids.poll();
		if(order != null)
			removeOrder(order);
		return order;
	}
	
	public synchronized Order remove (long orderID) {
		Order order = idMap.remove(orderID);
		if (order == null)
			return null;
		if (order.bid)
			bids.remove(order);
		else
			asks.remove(order);
		removeOrder(order);
		return order;
	}
	
	public JSONObject getUserOrders (long userID) {
		JSONObject job = null;
		try {
			job = new JSONObject();
			ArrayList<Order> orders = userMap.get(userID);
			JSONArray jBids = new JSONArray();
			JSONArray jAsks = new JSONArray();
			if(orders != null) {
				for (Order order : orders) {
					JSONObject newJob = new JSONObject();
					newJob.put("price", order.price);
					newJob.put("quantity", order.quantity);
					if(order.bid)
						jBids.put(newJob);
					else
						jAsks.put(newJob);
				}
			}
			job.put("bids", jBids);
			job.put("asks", jAsks); 
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Unable to covert order book to string");
		}
		return job;
	}
	
	public JSONObject getBook () {
		JSONObject job = null;
		try {
			job = new JSONObject();
			JSONArray jBids = new JSONArray();
			for (Order bid : bids) {
				JSONObject newJob = new JSONObject();
				newJob.put("price", bid.price);
				newJob.put("quantity", bid.quantity);
				jBids.put(newJob);
			}
			JSONArray jAsks = new JSONArray();
			for (Order ask : asks) {
				JSONObject newJob = new JSONObject();
				newJob.put("price", ask.price);
				newJob.put("quantity", ask.quantity);
				jAsks.put(newJob);
			}
			job.put("bids", jBids);
			job.put("asks", jAsks); 
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Unable to covert order book to string");
		}
		return job;
	}
	
	public String toString () {
		JSONObject job = getBook();
		return job.toString();
	}
}
