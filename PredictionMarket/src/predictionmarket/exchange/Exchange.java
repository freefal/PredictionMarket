// TODO - does user have enough $ to place order?

package predictionmarket.exchange;

import java.io.*;
import java.net.*;
import java.util.*;
import java.sql.*;
import com.mchange.v2.c3p0.*;	
import org.json.*;
import predictionmarket.model.*;
import predictionmarket.btcnetwork.*;

public class Exchange extends Thread {
	private static final String CONFIG_FILE = "config.json";
	private static final long BIG_NUM = 100000000000L;
	private static final String USER = "predictions";
	private static final String PASS = "m.h.=wily7.exchange";
	private ComboPooledDataSource cpds;
	private HashMap<Long, Worker> userWorkerMap;
	private OrderBook ob;
	private BitcoinNetworkClient bnc;
	
	private ExchangeConfiguration ec;
	
	public static void main (String [] args) {
		ExchangeConfiguration ec = new ExchangeConfiguration();
		ec.loadFromFile(CONFIG_FILE);
		
		Exchange boe = new Exchange(ec);
		boe.startPI();
		boe.start();
		
	}
	
	public Exchange (ExchangeConfiguration ec) {
		this.ec = ec;
		
		try {
			cpds = new ComboPooledDataSource();
			cpds.setDriverClass( "com.mysql.jdbc.Driver" ); //loads the jdbc driver            
			cpds.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/prediction_market");
			cpds.setUser(USER);                                  
			cpds.setPassword(PASS);
			cpds.setMinPoolSize(5);                                     
			cpds.setAcquireIncrement(5);
			cpds.setMaxPoolSize(20);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		userWorkerMap = new HashMap<Long, Worker>();
		// loadConfig(CONFIG_FILE);
		loadData();
		if(ec.btcNetworkToggle)
			bnc = new BitcoinNetworkClient(this);
		
	}
	
	private void loadData() {
		Connection con = null;
		Statement s = null;
		ResultSet rs = null;
		
		try {
			System.out.println("hello");
			con = cpds.getConnection();
			System.out.println("hello");
			s = con.createStatement();
			System.out.println("hello");
			rs = s.executeQuery("SELECT * FROM securities");
			ArrayList<String> secList = new ArrayList<String>();
			System.out.println("hello");
			while (rs.next()) {
				secList.add(rs.getString("desc"));
			}
			ob = new OrderBook(secList.toArray(new String[0]));
		} catch (Exception e) {
			System.err.println("Unable to load data to start exchange");
			e.printStackTrace();
		} finally {
			try {
				if (rs != null) {
	                rs.close();
				}
				if (s != null) {
	                s.close();
				}
				if (con != null) {
	                con.close();
				}
			} catch (Exception e) { e.printStackTrace(); System.err.println("Unable to close"); }
		}
	}

	public void startPI() {
		int a;
	}
	public void run () {
		ServerSocket ss = null;
		try {
			ss = new ServerSocket(ec.port, 0, InetAddress.getByName(ec.address));
			
			while (true) {
				Socket s = ss.accept();
				Worker ws = new Worker();
				ws.setSocket(s);
				ws.start();
			}
						
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				ss.close();
			} catch (Exception e) { e.printStackTrace(); }
		}
	}
	
	class Worker extends Thread {
		private Socket s;
		private User user;
		
		private PrintWriter pw;
		
		Worker() {
			s = null;
			
		}

		synchronized void setSocket(Socket s) {
			this.s = s;
		}

		public synchronized void run() {
			for (int i = 0; i < 10 && s == null; i++) {
				/* nothing to do */
				try {
					wait();
				} catch (InterruptedException e) {
					/* should not happen */
					continue;
				}
			}
			try {
				if (s != null)
					handleClient();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		}
		
		
		int handleLogin (JSONObject in, PrintWriter pw) throws JSONException {
			String username = in.getString("username");
			String password = in.getString("password");
			Connection con = null;
			PreparedStatement ps = null;
			ResultSet rs = null;
			
			try {
				con = cpds.getConnection();
				ps = con.prepareStatement("SELECT id FROM users WHERE username=? AND password=PASSWORD(?)");
				ps.setString(1, username);
				ps.setString(2, password);
				rs = ps.executeQuery();
				boolean authenticated = false;
				try {
					authenticated = rs.next();
				} catch (Exception e) {
					System.err.println("Failed to login");
				}
				JSONObject out = new JSONObject();
				out.put("type", "login");
				if (authenticated) {
					
					this.user = new User(rs.getLong(1), username);
					
					System.out.println("Put worker in map");
					userWorkerMap.put(user.id, this);
					out.put("result", "success");
					out.put("type", "login");
				}
				else {
					out.put("result", "failure");
				}
				pw.println(out.toString());
			} catch (Exception e) {
				pw.println(errorMessage("Internal Server Error"));
				e.printStackTrace(System.err);
				return 2;
			} finally {
				try {
					if (rs != null) {
		                rs.close();
					}
					if (ps != null) {
		                ps.close();
					}
					if (con != null) {
		                con.close();
					}
				} catch (Exception e) { e.printStackTrace(); System.err.println("Unable to close"); }
			}
			return 0;
		}
		
		void reportTransaction (String security, long price, long quantity, long transactionID) {
			
			JSONObject job = null;
			
			try {
				job = new JSONObject();
				job.put("result", "success");
				job.put("type", "execution");
				job.put("security", security);
				job.put("price", price);
				job.put("quantity", quantity);
				job.put("id", transactionID);
			} catch(Exception e) {
				e.printStackTrace();
				System.err.println("Unable to report transaction");
			}
			synchronized (pw) {
				pw.println(job.toString());
			}
		}
		
		void reportDeposit (long userID, long amount, String address) {
			try {
				JSONObject job = new JSONObject();
				job.put("result", "success");
				job.put("type", "deposit");
				job.put("amount", amount);
				job.put("address", address);
				synchronized (pw) {
					pw.println(job.toString());
				}
			} catch (Exception e) {
				e.printStackTrace();
				System.err.println("Unable to report order");
			}
		}
		
		int handlePlaceOrder (JSONObject in, PrintWriter pw) throws JSONException {
			String security, buySell, orderType;
			security = buySell = orderType = null;
			long price = 0, quantity = 0;
			try { 
				security = in.getString("security");
				buySell = in.getString("buysell");
				orderType = in.getString("ordertype");
				quantity = in.getLong("quantity");
				if (!orderType.equalsIgnoreCase("market"))
					price = in.getLong("price");
			} catch (Exception e) {
				pw.println(errorMessage("Order must contain security, buysell, and type as strings and price as number"));
				return 0;
			}
			if(user == null) {
				pw.println(errorMessage("User not yet authenticated"));
				return 0;
			}
			if(!ob.secExists(security)) {
				pw.println(errorMessage("Security doesn't exist"));
				return 0;
			}
			if(!buySell.equalsIgnoreCase("buy") && !buySell.equalsIgnoreCase("sell")) {
				pw.println(errorMessage("buysell must be either 'buy' or 'sell'"));
				return 0;
			}
			if(!orderType.equalsIgnoreCase("gtc") && !orderType.equalsIgnoreCase("ioc") && !orderType.equalsIgnoreCase("market")) {
				pw.println(errorMessage("buysell must be either 'buy' or 'sell'"));
				return 0;
			}
			if(price <= 0) {
				pw.println(errorMessage("price must be greater than 0"));
				return 0;
			}
			if(quantity <= 0) {
				pw.println(errorMessage("quantity must be greater than 0"));
				return 0;
			}
			long time = System.currentTimeMillis();
			boolean bid = buySell.equalsIgnoreCase("buy");
			Order o = new Order(price, quantity, time, user.id, security, bid);
						
			//int checkMargin(o);
			System.out.println("executing order");
			int retCode = executeOrder (o, pw);
			System.out.println("finished executing order");
						
			return retCode;
		}
		
		int handleCancelOrder (JSONObject in, PrintWriter pw) throws JSONException {
			String security = null;
			long orderID = -1;
			try {
				orderID = in.getLong("id");
				security = in.getString("security");
			} catch (Exception e) {
				pw.println(errorMessage("Order must contain id and security	"));
				return 0;
			}
			if(user == null) {
				pw.println(errorMessage("User not yet authenticated"));
				return 0;
			}
			if(!ob.secExists(security)) {
				pw.println(errorMessage("Security doesn't exist"));
				return 0;
			}
			
			OrderBookSecurity obs = ob.getOB(security);
			Order order = obs.remove(orderID);
			if (order == null) {
				pw.println(errorMessage("Security doesn't exist"));
				return 0;
			}
			reportCancelOrder(order, pw);
			return 0;
		}
		
		int handleGetBook (JSONObject in, PrintWriter pw) throws JSONException {
			String security = null;
			
			try {
				security = in.getString("security");
			} catch (Exception e) {
				pw.println(errorMessage("Order must contain security"));
				return 0;
			}
			if(!ob.secExists(security)) {
				pw.println(errorMessage("Security doesn't exist"));
				return 0;
			}
			
			OrderBookSecurity obs = ob.getOB(security);
			JSONObject out = new JSONObject();
			out.put("result", "success");
			out.put("type", "getbook");
			out.put("orders", obs.getBook());
				
			pw.println(out);
			return 0;
		}
		
		int handleGetUserOrders (JSONObject in, PrintWriter pw) throws JSONException {
			String security = null;
						
			try {
				security = in.getString("security");
			} catch (Exception e) {
				pw.println(errorMessage("Order must contain security"));
				return 0;
			}
			if(user == null) {
				pw.println(errorMessage("User not yet authenticated"));
				return 0;
			}
			if(!ob.secExists(security)) {
				pw.println(errorMessage("Security doesn't exist"));
				return 0;
			}
			
			OrderBookSecurity obs = ob.getOB(security);
			JSONObject out = new JSONObject();
			out.put("result", "success");
			out.put("type", "getuserorders");
			out.put("orders", obs.getUserOrders(user.id));
				
			pw.println(out);
			
			return 0;
		}
		
		int handleGetDepositAddress (JSONObject in, PrintWriter pw) throws JSONException {
			if(user == null) {
				pw.println(errorMessage("User not yet authenticated"));
				return 0;
			}
						
			String address = createDepositAddress(user.id);
			
			JSONObject out = new JSONObject();
			out.put("result", "success");
			out.put("type", "getdepositaddress");
			out.put("address", address);
			pw.println(out);
			
			return 0;
		}
		
		int handleWithdraw (JSONObject in, PrintWriter pw) throws JSONException {
			if(user == null) {
				pw.println(errorMessage("User not yet authenticated"));
				return 0;
			}
			String to;
			long amount;
			
			try {
				to = in.getString("to");
				amount = in.getLong("amount");
			} catch (Exception e) {
				pw.println(errorMessage("Order must contain to address and amount"));
				return 0;
			}
			
			int retCode = withdraw(to, amount, user);
			
			
			JSONObject out = new JSONObject();
			out.put("type", "withdraw");
			if (retCode == 0)
				out.put("result", "success");
			else {
				out.put("result", "failure");
				out.put("reason", "overdraw");
			}
			pw.println(out);
			
			return 0;
		}
		
		int handleGetExistingPositionsMargin (JSONObject in, PrintWriter pw) throws JSONException {
			if(user == null) {
				pw.println(errorMessage("User not yet authenticated"));
				return 0;
			}
			
			long margin = existingPositionsInitialMargin(user);
			
			JSONObject out = new JSONObject();
			out.put("type", "getexistingpositionsmargin");
			out.put("margin", margin);
			out.put("result", "success");
			
			pw.println(out);
			
			return 0;
		}
		
		int handleGetBalance (JSONObject in, PrintWriter pw) throws JSONException {
			if(user == null) {
				pw.println(errorMessage("User not yet authenticated"));
				return 0;
			}
			
			long balance = currentBalance(user);
			
			JSONObject out = new JSONObject();
			out.put("type", "getbalance");
			out.put("balance", balance);
			out.put("result", "success");
			
			pw.println(out);
			
			return 0;
		}
		
		int handleGetWithdrawableAmount (JSONObject in, PrintWriter pw) throws JSONException {
			if(user == null) {
				pw.println(errorMessage("User not yet authenticated"));
				return 0;
			}
			
			
			long withdrawableAmount = withdrawableAmount(user);
						
			JSONObject out = new JSONObject();
			out.put("type", "getwithdrawableamount");
			out.put("withdrawableamount", withdrawableAmount);
			out.put("result", "success");
			
			pw.println(out);
			
			return 0;
		}
		
		int handleNOOP (JSONObject job, PrintWriter pw) throws JSONException {
			JSONObject out = new JSONObject();
			out.put("result", "success");
			out.put("type", "noop");
			pw.println(out);
			return 0;
		}
		

		/*
		 * 0 - all well
		 * 1 - exit normally
		 * 2+ - exit b/c of error
		 */
		
		int handleMessage(String message, PrintWriter pw) throws JSONException {
			JSONObject job = null;
			try {
				job = new JSONObject(message);
			} catch (Exception e) {
				pw.println(errorMessage("error parsing JSON"));
				e.printStackTrace();
				return 0;
			}
			String op = null;
			try {
				op = job.getString("op");
			} catch (Exception e) {
				pw.println(errorMessage("must provied an 'op'"));
				return 0;
			}
			
			if (op.equalsIgnoreCase("login")) {
				System.out.println("login command");
				return handleLogin(job, pw);
			}
			else if (op.equalsIgnoreCase("noop")) {
				System.out.println("noop command");
				return handleNOOP(job, pw);
			}
			else if (op.equalsIgnoreCase("placeorder")) {
				System.out.println("placeorder command");
				return handlePlaceOrder(job, pw);
			}
			else if (op.equalsIgnoreCase("cancelorder")) {
				System.out.println("cancelorder command");
				return handleCancelOrder(job, pw);
			}
			else if (op.equalsIgnoreCase("getbook")) {
				System.out.println("getbook command");
				return handleGetBook(job, pw);
			}
			else if (op.equalsIgnoreCase("getuserorders")) {
				System.out.println("getuserorders command");
				return handleGetUserOrders(job, pw);
			}
			else if (op.equalsIgnoreCase("getdepositaddress")) {
				System.out.println("getdepositaddress command");
				return handleGetDepositAddress(job, pw);
			}
			else if (op.equalsIgnoreCase("withdraw")) {
				System.out.println("withdraw command");
				return handleWithdraw(job, pw);
			}
			else if (op.equalsIgnoreCase("getexistingpositionsmargin")) {
				System.out.println("getexistingpositionsmargin command");
				return handleGetExistingPositionsMargin(job, pw);
			}
			else if (op.equalsIgnoreCase("getbalance")) {
				System.out.println("getbalance command");
				return handleGetBalance(job, pw);
			}
			else if (op.equalsIgnoreCase("getwithdrawableamount")) {
				System.out.println("getwithdrawableamount command");
				return handleGetWithdrawableAmount(job, pw);
			}
			else {
				pw.println(errorMessage("Unrecognized op"));
			}
			
			return 0;
		}
			
		void handleClient() {
			try {
				BufferedReader br = new BufferedReader(new InputStreamReader(s.getInputStream()));
				pw = new PrintWriter(s.getOutputStream(), true);
								
				int exit = 0;
				String line = "";
				
				while (exit == 0 && line != null) {
					line = br.readLine();
					if (line != null) {
						synchronized (pw) {
							exit = handleMessage(line, pw);
						}
					}
				}
				br.close();
				pw.close();
				s.close();
			} catch (Exception e) {
				e.printStackTrace();
				if (s.isClosed())
					userWorkerMap.remove(user.id);
			}
		}
	}
	
	public String errorMessage(String message) {
		JSONObject job = null;
		try {
			job = new JSONObject();
			job.put("result", "error");
			job.put("message", message);
		} catch (Exception e) { e.printStackTrace(); }
		
		return job.toString();
	}
	
	private synchronized void reportTransaction(String security, long price, long quantity, long buyerID, long sellerID, long transactionID) {
		Worker buyerWorker = userWorkerMap.get(buyerID);
		if (buyerWorker != null) {
			buyerWorker.reportTransaction(security, price, quantity, transactionID);
		}
		Worker sellerWorker = userWorkerMap.get(buyerID);
		if (sellerWorker != null) {
			sellerWorker.reportTransaction(security, price, -quantity, transactionID);
		}		
	}
	
	private void reportOrder (Order order, PrintWriter pw) {
		try {
			JSONObject job = new JSONObject();
			job.put("result", "success");
			job.put("type", "orderPlaced");
			job.put("price", order.price);
			job.put("quantity", order.quantity);
			job.put("security", order.security);
			job.put("timestamp", order.placed);
			job.put("buysell", order.bid ? "buy" : "sell");
			job.put("id", order.id);
			pw.println(job.toString());
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Unable to report order");
		}
	}
	
	private void reportCancelOrder (Order order, PrintWriter pw) {
		try {
			JSONObject job = new JSONObject();
			job.put("result", "success");
			job.put("type", "orderCancelled");
			job.put("price", order.price);
			job.put("quantity", order.quantity);
			job.put("security", order.security);
			job.put("timestamp", order.placed);
			job.put("buysell", order.bid ? "buy" : "sell");
			job.put("id", order.id);
			pw.println(job.toString());
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Unable to report order");
		}
	}

	private void reportNoOrder (Order order, PrintWriter pw) {
		try {
			JSONObject job = new JSONObject();
			job.put("result", "success");
			job.put("type", "noexecution");
			job.put("price", order.price);
			job.put("quantity", order.quantity);
			job.put("security", order.security);
			job.put("timestamp", order.placed);
			job.put("buysell", order.bid ? "buy" : "sell");
			pw.println(job.toString());
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Unable to report order");
		}
	}
	
	
	private synchronized int executeOrder (Order o, PrintWriter pw) {
		OrderBookSecurity obs = ob.getOB(o.security);
		System.out.println(obs);
		Order bestBid = obs.bestBid();
		Order bestAsk = obs.bestAsk();
		JSONObject job = new JSONObject();
		/*
		if (orderType.equalsIgnoreCase("market")) {
			orderType = "IOC";
			if (buySell.equalsIgnoreCase("buy")) {
				price = BIG_NUM;
			}
			if (buySell.equalsIgnoreCase("sell")) {
				price = 0;
			}
		}
		*/
		if (o.bid) {
			if (bestAsk != null && o.price >= bestAsk.price) {
				if (o.quantity < bestAsk.quantity) {
					bestAsk.quantity = bestAsk.quantity - o.quantity;
					long transactionID = executeTransaction(o.security, bestAsk.price, o.quantity, o.userID, bestAsk.userID);
					reportTransaction(o.security, bestAsk.price, bestAsk.quantity, o.userID, bestAsk.userID, transactionID);
				}
				else {
					obs.popAsk();
					long transactionID = executeTransaction(o.security, bestAsk.price, bestAsk.quantity, o.userID, bestAsk.userID);
					reportTransaction(o.security, bestAsk.price, bestAsk.quantity, o.userID, bestAsk.userID, transactionID);
					Order newOrder = new Order(o);
					executeOrder(newOrder, pw);
				}
			}
			else {
				// if (orderType.equalsIgnoreCase("GTC")) {
					obs.addBid(o);
					System.out.println(obs.bestBid().price);
					reportOrder(o, pw);
					/*
				}
				else {
					Order order = new Order(price, quantity, System.currentTimeMillis(), userID, security, true);
					reportNoOrder(order, pw);
				}
				*/
			}
		}
		else {
			if (bestBid != null && o.price <= bestBid.price) {
				if (o.quantity < bestBid.quantity) {
					bestBid.quantity = bestBid.quantity - o.quantity;
					long transactionID = executeTransaction(o.security, bestBid.price, o.quantity, bestBid.userID, o.userID);
					reportTransaction(o.security, bestBid.price, bestBid.quantity, o.userID, bestBid.userID, transactionID);
				}
				else {
					obs.popBid();
					long transactionID = executeTransaction(o.security, bestBid.price, bestBid.quantity, bestBid.userID, o.userID);
					reportTransaction(o.security, bestBid.price, bestBid.quantity, o.userID, bestBid.userID, transactionID);
					if (o.quantity - bestBid.quantity > 0)
						executeOrder(o, pw);
				}
			}
			else {
				// if (orderType.equalsIgnoreCase("GTC")) {
					obs.addAsk(o);
					reportOrder(o, pw);
				/*
				}
				else {
					Order order = new Order(price, quantity, System.currentTimeMillis(), userID, security, false);
					reportNoOrder(order, pw);
				}
				*/
			}
		}
		System.out.println(obs);
		return 0;
	}
	
	private synchronized String createDepositAddress(long userID) {
		String address = bnc.createReceivingAddress();
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			con = cpds.getConnection();
			ps = con.prepareStatement("INSERT INTO depositaddresses (userID, address) VALUES (?,?)");
			ps.setLong(1,userID);
			ps.setString(2,address);
			ps.executeUpdate();
			return address;
		} catch (Exception e) {
			e.printStackTrace(System.err);
		} finally {
			try {
				if (rs != null) {
	                rs.close();
				}
				if (ps != null) {
	                ps.close();
				}
				if (con != null) {
	                con.close();
				}
			} catch (Exception e) { e.printStackTrace(); System.err.println("Unable to close"); }
		}
		
		return "";
	}
	
	private synchronized long executeTransaction(String security, long price, long quantity, long buyerID, long sellerID) {
		Connection con = null;
		PreparedStatement ps = null;
		PreparedStatement ps2 = null;
		ResultSet rs = null;
		long transactionID = -1;
		try {
			con = cpds.getConnection();
			ps = con.prepareStatement("INSERT INTO transactions (security, price, quantity, buyerID, sellerID) VALUES (?,?,?,?,?)");
			ps.setString(1, security);
			ps.setLong(2, price);
			ps.setLong(3, quantity);
			ps.setLong(4, buyerID);
			ps.setLong(5, sellerID);
			ps.executeUpdate();
			rs = ps.getGeneratedKeys();
			rs.next();
			transactionID = rs.getLong(1);
			long cost = quantity*price;
			ps = con.prepareStatement("UPDATE users SET bitcoins = bitcoins + ? WHERE id = ?");
			ps.setLong(1, -cost);
			ps.setLong(2, buyerID);
			ps.executeUpdate();
			ps.setLong(1, cost);
			ps.setLong(2, sellerID);
			ps.executeUpdate();
			ps = con.prepareStatement("SELECT positions.id FROM positions WHERE userID=? and securityID=(SELECT securities.id FROM securities WHERE sec=?)");
			ps.setLong(1, buyerID);
			ps.setString(2, security);
			rs = ps.executeQuery();
			long buyerPositionID = -1;
			if (rs.next())
				buyerPositionID = rs.getLong(1);
			
			ps.setLong(1, sellerID);
			rs = ps.executeQuery();
			long sellerPositionID = -1;
			if (rs.next())
				sellerPositionID = rs.getLong(1);
			
			ps = con.prepareStatement("UPDATE positions SET amount = amount + ? WHERE id=?");
			ps2 = con.prepareStatement("INSERT INTO positions (userID, (SELECT securities.id FROM securities WHERE sec=?), amount) VALUES (?,?,?)");
				
			if(buyerPositionID  != -1) {
				ps.setLong(1, quantity);
				ps.setLong(2, buyerPositionID);
				ps.executeUpdate();
			}
			else {
				ps2.setLong(1, buyerID);
				ps2.setString(2, security);
				ps2.setLong(3, quantity);
				ps2.executeUpdate();
			}
			
			if(sellerPositionID  != -1) {
				ps.setLong(1, -quantity);
				ps.setLong(2, sellerPositionID);
				ps.executeUpdate();
			}
			else {
				ps2.setLong(1, sellerID);
				ps2.setString(2, security);
				ps2.setLong(3, -quantity);
				ps2.executeUpdate();
			}
			
			ps = con.prepareStatement("UPDATE positions SET bitcoins = bitcoins + ? WHERE id = ?");
			ps.setLong(1, -cost);
			ps.setLong(2, buyerID);
			ps.executeUpdate();
			ps.setLong(1, cost);
			ps.setLong(2, sellerID);
			ps.executeUpdate();
			
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error executing transaction");
			return 2;
		} finally {
			try {
				if (rs != null) {
	                ps.close();
				}
				if (ps != null) {
	                ps.close();
				}
				if (ps2 != null) {
	                ps.close();
				}
				if (con != null) {
	                con.close();
				}
			} catch (Exception e) { e.printStackTrace(); System.err.println("Unable to close"); }
		}
		return transactionID;
	}
	
	public synchronized long existingPositionsInitialMargin (User user) {
		
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			con = cpds.getConnection();
			ps = con.prepareStatement("SELECT *, positions.id as positionid, securities.id as securityid FROM positions INNER JOIN securities ON positions.securityid = securities.id WHERE userID=?");
			ps.setLong(1, user.id);
			rs = ps.executeQuery();
			ArrayList<Position> positions = new ArrayList<Position>();
			
			while (rs.next()) {
				Position position = new Position();
				position.id = rs.getLong("positionid");
				position.amount = rs.getLong("amount");
				Security sec = null;
				String type = rs.getString("type");
				if (type.equals("option")) {
					int a;
				}
				sec.name = rs.getString("sec");
				sec.type = type;
				sec.id = rs.getLong("securityid");
				position.sec = sec;
				positions.add(position);
			}
			
			return initialMargin(positions);
		} catch (Exception e) {
			e.printStackTrace(System.err);
		} finally {
			try {
				if (rs != null) {
	                rs.close();
				}
				if (ps != null) {
	                ps.close();
				}
				if (con != null) {
	                con.close();
				}
			} catch (Exception e) { e.printStackTrace(); System.err.println("Unable to close"); }
		}
		
		return 0;
	}
	
	public long initialMargin (ArrayList<Position> positions) {
		long margin = 0;
		for (Position p : positions) {
			margin += initialMargin(p);
		}
		return margin;
	}
	
	public long initialMargin (Position position) {
		long amount = position.amount;
		if(amount >= 0)
			return 0;
		Security sec = position.sec;
		long singleMargin = 0;
		if(sec.type.equals("option")) {
			int a;
		}
		
		long totalMargin = singleMargin * -amount;
		return totalMargin;
	}
	
	public long initialMargin (Security option) {
		return 0;
	}
	
	public synchronized long currentBalance(User user) {
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			con = cpds.getConnection();
			ps = con.prepareStatement("SELECT bitcoins FROM users WHERE id = ?");
			ps.setLong(1, user.id);
			rs = ps.executeQuery();
			
			if (rs.next()) {
				long currentBalance = rs.getLong("bitcoins");
				return currentBalance;
			}
			
			
		} catch (Exception e) {
			e.printStackTrace(System.err);
		} finally {
			try {
				if (rs != null) {
	                rs.close();
				}
				if (ps != null) {
	                ps.close();
				}
				if (con != null) {
	                con.close();
				}
			} catch (Exception e) { e.printStackTrace(); System.err.println("Unable to close"); }
		}
		
		return 0;
	}
	
	public synchronized long withdrawableAmount(User user) {
		long margin = existingPositionsInitialMargin(user);
		long balance = currentBalance(user);
		long withdrawableAmount = Math.max(0, balance - margin - BitcoinNetworkClient.FEE);
		return withdrawableAmount;
	}
	
	public synchronized int withdraw(String to, long amount, User user) {
		long margin = existingPositionsInitialMargin (user);
		long balance = currentBalance (user);
		
		int retCode = 1;
		
		if(balance > (margin + amount + BitcoinNetworkClient.FEE)) {
			retCode = bnc.sendCoins(to, amount);
			if (retCode == 0) {
				Connection con = null;
				PreparedStatement ps = null;
				ResultSet rs = null;
				
				try {
					con = cpds.getConnection();
					ps = con.prepareStatement("UPDATE users SET bitcoins = bitcoins - ? WHERE id = ?");
					ps.setLong(1, amount + BitcoinNetworkClient.FEE);
					ps.setLong(2, user.id);
					ps.executeUpdate();
					
					ps = con.prepareStatement("INSERT INTO withdrawals (amount, userID, fee) VALUES (?, ?, ?)");
					ps.setLong(1, amount);
					ps.setLong(2, user.id);
					ps.setLong(3, BitcoinNetworkClient.FEE);
					ps.executeUpdate();
					
				} catch (Exception e) {
					System.err.println("Error updating balance during withdrawal");
					e.printStackTrace(System.err);
				} finally {
					try {
						if (rs != null) {
			                rs.close();
						}
						if (ps != null) {
			                ps.close();
						}
						if (con != null) {
			                con.close();
						}
					} catch (Exception e) { e.printStackTrace(); System.err.println("Unable to close"); }
				}
			}
		}
		else {
			retCode = 1;
		}
		
		return retCode;
	}
	
	public void receivedCoins (String address, long amount) {
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			con = cpds.getConnection();
			ps = con.prepareStatement("SELECT userID FROM depositaddresses WHERE address=?");
			ps.setString(1, address);
			rs = ps.executeQuery();
			long userID = -1;
			if (rs.next()) {
				userID = rs.getLong(1);
				ps = con.prepareStatement("UPDATE users SET bitcoins = bitcoins + ? WHERE id = ?");
				ps.setLong(1, amount);
				ps.setLong(2, userID);
				ps.executeUpdate();
				
				ps = con.prepareStatement("INSERT INTO deposits (amount, userID) VALUES (?, ?)");
				ps.setLong(1, amount);
				ps.setLong(2, userID);
				ps.executeUpdate();
						
				userWorkerMap.get(userID).reportDeposit(userID, amount, address);
			}
			else {
				System.err.println("Coins received from unknown address");
			}
			
			
		} catch (Exception e) {
			e.printStackTrace(System.err);
		} finally {
			try {
				if (rs != null) {
	                rs.close();
				}
				if (ps != null) {
	                ps.close();
				}
				if (con != null) {
	                con.close();
				}
			} catch (Exception e) { e.printStackTrace(); System.err.println("Unable to close"); }
		}
	}
}