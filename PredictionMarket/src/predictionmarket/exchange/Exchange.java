// TODO - does user have enough $ to place order?

package predictionmarket.exchange;

import java.io.*;
import java.net.*;
import javax.net.ssl.*;
import java.util.*;
import java.sql.*;
import com.mchange.v2.c3p0.*;	
import org.json.*;
import predictionmarket.model.*;
import predictionmarket.btcnetwork.*;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.*;
import org.eclipse.jetty.servlet.*;
import javax.servlet.*;
import javax.servlet.http.*;

public class Exchange {
	private static final String CONFIG_FILE = "config.json";
	private static final String USER = "predictions";
	private static final String PASS = "m.h.=wily7.exchange";
	private ComboPooledDataSource cpds;
	private OrderBook ob;
	private BitcoinNetworkClient bnc;
	
	private ExchangeConfiguration ec;
	
	public static void main (String [] args) {
		ExchangeConfiguration ec = new ExchangeConfiguration();
		ec.loadFromFile(CONFIG_FILE);
		
		Exchange boe = new Exchange(ec);
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
		// loadConfig(CONFIG_FILE);
		loadData();
		if(ec.btcNetworkToggle)
			bnc = new BitcoinNetworkClient(this);
		
	}
	
	public void start() {
		try {
			Server server = new Server(8080);
			server.setHandler(new RequestHandler());
			server.start();
	        server.join();
		} catch(Exception e) {
			System.err.println("Unable to start server");
			e.printStackTrace();
		}
	}
	
	private void loadData() {
		Connection con = null;
		Statement s = null;
		ResultSet rs = null;
		
		try {
			con = cpds.getConnection();
			s = con.createStatement();
			rs = s.executeQuery("SELECT * FROM securities");
			ArrayList<Security> securities = new ArrayList<Security>();
			while (rs.next()) {
				Security sec = new Security();
				sec.id = rs.getLong("id");
				sec.desc = rs.getString("desc");
				sec.contractsize = rs.getLong("contractsize");
				sec.begin = rs.getTimestamp("begin").getTime();
				sec.end = rs.getTimestamp("end").getTime();
				securities.add(sec);
			}
			ob = new OrderBook(securities);
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

	private class RequestHandler extends ServletContextHandler {
	    public RequestHandler () {
	    	addServlet(new ServletHolder(new PlaceOrderServlet()),"/v1/order/place");
	    	addServlet(new ServletHolder(new PlaceOrderServlet()),"/v1/order/cancel");
	    }		
	}

	private class PlaceOrderServlet extends HttpServlet {
		protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
			response.setContentType("text/json");
	        response.setStatus(HttpServletResponse.SC_OK);
	        PrintWriter pw = response.getWriter();
			
			boolean isBid = true;
			long price = 0, quantity = 0, security = 0, user = 0;
			int bid = 0;
			try {
				security = Long.parseLong(request.getParameter("security"));
				price = Long.parseLong(request.getParameter("price"));
				quantity = Long.parseLong(request.getParameter("quantity"));
				bid = Integer.parseInt(request.getParameter("bid"));
				user = Long.parseLong(request.getParameter("user"));
			} catch(Exception e) {
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				pw.println(errorMessage("Malformed request"));
				return;
			}
			if(!ob.secExists(security)) {
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				pw.println(errorMessage("Security doesn't exist"));
				return;
			}
			if(price <= 0 || price >= 1000000) {
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				pw.println(errorMessage("price must be greater than 0 and less than 1000000"));
				return;
			}
			if(quantity <= 0) {
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				pw.println(errorMessage("quantity must be greater than 0"));
				return;
			}
			
			long time = System.currentTimeMillis();
			Order o = new Order(price, quantity, time, user, security, isBid);
			long id = executeOrder (o);
			JSONObject job = null;
			try {
				job = new JSONObject();
				job.put("order", o.id);
			} catch (Exception e) { e.printStackTrace(); }
			pw.println(job.toString());
	    }
	}
	
	private class CancelOrderServlet extends HttpServlet {
		protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
			response.setContentType("text/json");
	        response.setStatus(HttpServletResponse.SC_OK);
	        PrintWriter pw = response.getWriter();
			
			long orderID = 0, userID = 0;
			
			try {
				orderID = Long.parseLong(request.getParameter("order"));
				userID = Long.parseLong(request.getParameter("user"));
			} catch(Exception e) {
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				pw.println(errorMessage("Malformed request"));
				return;
			}
			
			Order o = new Order();
			o.id = orderID;
			o.userID = userID;
			Long retOrder = cancelOrder(o);
			
			if (retOrder == null) {
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				pw.println(errorMessage("Order not found in the order book. Did you place this order? Was it already executed?"));
				return;
			}
			
			JSONObject job = null;
			try {
				job = new JSONObject();
				job.put("order", retOrder);
			} catch (Exception e) { e.printStackTrace(); }
			pw.println(job.toString());
	    }
	}
	/*	
		int handleGetBook (JSONObject in, PrintWriter pw) throws JSONException {
			long security = 0;
			
			try {
				security = in.getLong("security");
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
			long security = 0;
						
			try {
				security = in.getLong("security");
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
			
	}
	*/
	public String errorMessage(String message) {
		JSONObject job = null;
		try {
			job = new JSONObject();
			job.put("error", message);
		} catch (Exception e) { e.printStackTrace(); }
		
		return job.toString();
	}

	private synchronized int executeOrder (Order o) {
		ob.putOrderSecurity(o.id, o.security);
		OrderBookSecurity obs = ob.getOB(o.security);
		Order bestBid = obs.bestBid();
		Order bestAsk = obs.bestAsk();
		JSONObject job = new JSONObject();
		if (o.bid) {
			if (bestAsk != null && o.price >= bestAsk.price) {
				if (o.quantity < bestAsk.quantity) {
					bestAsk.quantity = bestAsk.quantity - o.quantity;
					long transactionID = executeTransaction(o.security, bestAsk.price, o.quantity, o.userID, bestAsk.userID);
				}
				else {
					Order oldAsk = obs.popAsk();
					long transactionID = executeTransaction(o.security, bestAsk.price, bestAsk.quantity, o.userID, bestAsk.userID);
					o.quantity = o.quantity - oldAsk.quantity;
					if(o.quantity > 0)
						executeOrder(o);
				}
			}
			else {
				obs.addBid(o);
			}
		}
		else {
			if (bestBid != null && o.price <= bestBid.price) {
				if (o.quantity < bestBid.quantity) {
					bestBid.quantity = bestBid.quantity - o.quantity;
					long transactionID = executeTransaction(o.security, bestBid.price, o.quantity, bestBid.userID, o.userID);
				}
				else {
					Order oldBid = obs.popBid();
					long transactionID = executeTransaction(o.security, bestBid.price, bestBid.quantity, bestBid.userID, o.userID);
					o.quantity = o.quantity - oldBid.quantity;
					if(o.quantity > 0)
						executeOrder(o);
				}
			}
			else {
					obs.addAsk(o);
			}
		}
		System.out.println(obs);
		return 0;
	}
	
	private synchronized Long cancelOrder (Order o) {
		return ob.removeOrder(o);
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
	
	private synchronized long executeTransaction(long security, long price, long quantity, long buyerID, long sellerID) {
		Connection con = null;
		PreparedStatement ps = null;
		PreparedStatement ps2 = null;
		ResultSet rs = null;
		long transactionID = -1;
		try {
			con = cpds.getConnection();
			ps = con.prepareStatement("INSERT INTO transactions (security, price, quantity, buyerID, sellerID) VALUES (?,?,?,?,?)");
			ps.setLong(1, security);
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
			ps.setLong(2, security);
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
				ps2.setLong(2, security);
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
				ps2.setLong(2, security);
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
				sec.id = rs.getLong("securityid");
				sec.desc = rs.getString("desc");
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