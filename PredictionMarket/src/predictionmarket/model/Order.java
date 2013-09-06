package bitcoinoptions.global;

import java.util.Random;

public class Order {
	public static Random rand = new Random();
	public long id;
	public long price;
	public long quantity;
	public long placed; // Time placed
	public long userID;
	public String security;
	public boolean bid;
	public long ttx; // Not currently used - only IOC or GTC allowed
	
	public Order () {
		
	}
	
	public Order (Order o) {
		this.id = rand.nextLong();
		this.price = o.price;
		this.quantity = o.quantity;
		this.placed = o.placed;
		this.userID = o.userID;
		this.security = o.security;
		this.bid = o.bid;
		this.ttx = o.ttx;
		
	}
	
	public Order(long price, long quantity, long placed, long userID, String security, boolean bid) {
		this.id = rand.nextLong();
		this.price = price;
		this.quantity = quantity;
		this.placed = placed;
		this.userID = userID;
		this.security = security;
		this.bid = bid;
	}
}
