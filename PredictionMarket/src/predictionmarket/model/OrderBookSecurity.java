package predictionmarket.model;

import java.util.*;
import org.json.*;

public class OrderBookSecurity {
	private Security s;
	private HashMap<Long, Order> idMap;
	private HashMap<Long, ArrayList<Order>> userMap;
	private PriorityQueue<Order> bids; 
	private PriorityQueue<Order> asks;
	
	public OrderBookSecurity (Security s) {
		this.s = s;
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
	
	public synchronized Order remove (Order orderToDelete) {
		Order order = idMap.get(orderToDelete.id);
		if (order.userID != orderToDelete.userID) // Only the user can cancel his orders
			return null;
		order = idMap.remove(orderToDelete.id);
		if (order == null)
			return null;
		if (order.bid)
			bids.remove(order);
		else
			asks.remove(order);
		removeOrder(order);
		System.out.println(this);
		return order;
	}
	
	public ArrayList<Order> getUserOrders (long userID) {
		return userMap.get(userID);
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
