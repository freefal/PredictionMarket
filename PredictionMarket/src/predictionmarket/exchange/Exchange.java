// TODO - Actually implement API keys rather than just having users pass their ids

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
		System.out.println(ec.btcNetworkToggle);
		if(ec.btcNetworkToggle)
			bnc = new BitcoinNetworkClient(this);
		
	}
	
	public void start() {
		try {
			Server server = new Server(ec.port);
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
	    	addServlet(new ServletHolder(new PlaceOrderServlet()),"/v1/do/order/place");
	    	addServlet(new ServletHolder(new CancelOrderServlet()),"/v1/do/order/cancel");
	    	addServlet(new ServletHolder(new GetUserOrdersServlet()),"/v1/get/userorders");
	    	addServlet(new ServletHolder(new GetUserPositionsServlet()),"/v1/get/userpositions");
	    	addServlet(new ServletHolder(new DepositServlet()),"/v1/do/deposit");
	    	addServlet(new ServletHolder(new WithdrawServlet()),"/v1/do/withdraw");
	    }
	}

	private class PlaceOrderServlet extends HttpServlet {
		private static final long serialVersionUID = 1L;
		
		protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
			System.out.println("Order Placed");
			response.setContentType("application/json");
	        response.setStatus(HttpServletResponse.SC_OK);
	        PrintWriter pw = response.getWriter();
			
			boolean isBid = true;
			long price = 0, quantity = 0, security = 0, user = 0;
			int bid = 0;
			System.out.println("Before try");
			try {
				security = Long.parseLong(request.getParameter("security"));
				price = Long.parseLong(request.getParameter("price"));
				quantity = Long.parseLong(request.getParameter("quantity"));
				bid = Integer.parseInt(request.getParameter("bid"));
				isBid = (bid != 0);
				user = Long.parseLong(request.getParameter("user"));
			} catch(Exception e) {
				e.printStackTrace();
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				pw.println(errorMessage("Malformed request"));
				return;	
			}
			System.out.println("After try");
			if(!ob.secExists(security)) {
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				pw.println(errorMessage("Security doesn't exist"));
				return;
			}
			System.out.println("1");
			if(price <= 0 || price >= 1000000) {
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				pw.println(errorMessage("price must be greater than 0 and less than 1000000"));
				return;
			}
			System.out.println("2");
			if(quantity <= 0) {
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				pw.println(errorMessage("quantity must be greater than 0"));
				return;
			}
			System.out.println("3");
			if(price*quantity > availableFunds(user)) {
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				pw.println(errorMessage("insufficient funds"));
				return;
			}
			System.out.println("ready to execute order");
			
			long time = System.currentTimeMillis();
			Order o = new Order(price, quantity, time, user, security, isBid);
			executeOrder (o);
			JSONObject job = null;
			try {
				job = new JSONObject();
				job.put("order", o.id);
			} catch (Exception e) { e.printStackTrace(); }
			pw.println(job.toString());
	    }
	}
	
	private class CancelOrderServlet extends HttpServlet {
		private static final long serialVersionUID = 1L;
		
		protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
			response.setContentType("application/json");
	        response.setStatus(HttpServletResponse.SC_OK);
	        PrintWriter pw = response.getWriter();
			
			long orderID = 0, user = 0;
			try {
				orderID = Long.parseLong(request.getParameter("order"));
				user = Long.parseLong(request.getParameter("user"));
			} catch(Exception e) {
				e.printStackTrace();
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				pw.println(errorMessage("Malformed request"));
				return;
			}
			
			Order o = new Order();
			o.id = orderID;
			o.userID = user;
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
	
	private class DepositServlet extends HttpServlet {
		private static final long serialVersionUID = 1L;

		protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
			response.setContentType("application/json");
	        response.setStatus(HttpServletResponse.SC_OK);
	        PrintWriter pw = response.getWriter();
			
			long userID = 0;
			try {
				userID = Long.parseLong(request.getParameter("user"));
			} catch(Exception e) {
				e.printStackTrace();
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				pw.println(errorMessage("Malformed request"));
				return;
			}
			
			
			String address = createDepositAddress(userID);
						
			JSONObject job = null;
			try {
				job = new JSONObject();
				job.put("address", address);
			} catch (Exception e) { e.printStackTrace(); }
			pw.println(job.toString());
	    }
	}
	
	private class WithdrawServlet extends HttpServlet {
		private static final long serialVersionUID = 1L;

		protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
			response.setContentType("application/json");
	        response.setStatus(HttpServletResponse.SC_OK);
	        PrintWriter pw = response.getWriter();
			
			String address = null;
			long amount = 0, user = 0;
			try {
				address = request.getParameter("address");
				amount = Long.parseLong(request.getParameter("amount"));
				user = Long.parseLong(request.getParameter("user"));
			} catch(Exception e) {
				e.printStackTrace();
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				pw.println(errorMessage("Malformed request"));
				return;
			}
			
			long coins = availableFunds(user);
			
			if (coins < (amount + BitcoinNetworkClient.FEE)) {
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				pw.println(errorMessage("Can't withdraw (" + amount + ") more than available funds ("+ coins + ") plus Bitcoin network fee ("+BitcoinNetworkClient.FEE+")"));
				return;
			}
			
			withdraw(address, amount, user);
			
			JSONObject job = null;
			try {
				job = new JSONObject();
				job.put("address", address);
				job.put("amount", amount);
			} catch (Exception e) { e.printStackTrace(); }
			pw.println(job.toString());
	    }
	}
	
	private class GetUserOrdersServlet extends HttpServlet {
		private static final long serialVersionUID = 1L;

		protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
			response.setContentType("application/json");
	        response.setStatus(HttpServletResponse.SC_OK);
	        PrintWriter pw = response.getWriter();
			
	        long user = 0;
			try {
				user = Long.parseLong(request.getParameter("user"));
			} catch(Exception e) {
				e.printStackTrace();
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				pw.println(errorMessage("Malformed request"));
				return;
			}
			
			ArrayList<Order> orders = ob.getUserOrders(user);
			
			JSONObject job = null;
			try {
				job = new JSONObject();
				JSONArray ja = new JSONArray();
				for (Order o : orders) {
					ja.put(o.id);
				}
				job.put("orders", ja);
			} catch (Exception e) { e.printStackTrace(); }
			pw.println(job.toString());
	    }
	}
	
	private class GetUserPositionsServlet extends HttpServlet {
		private static final long serialVersionUID = 1L;

		protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
			response.setContentType("application/json");
	        response.setStatus(HttpServletResponse.SC_OK);
	        PrintWriter pw = response.getWriter();
			
	        long user = 0;
			try {
				user = Long.parseLong(request.getParameter("user"));
			} catch(Exception e) {
				e.printStackTrace();
				response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
				pw.println(errorMessage("Malformed request"));
				return;
			}
			
			ArrayList<Order> orders = ob.getUserOrders(user);
			
			JSONObject job = null;
			try {
				job = new JSONObject();
				JSONArray ja = new JSONArray();
				for (Order o : orders) {
					ja.put(o.id);
				}
				job.put("orders", ja);
			} catch (Exception e) { e.printStackTrace(); }
			pw.println(job.toString());
	    }
	}
	
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
	
	private synchronized long availableFunds (long user) {
		long curBalance = currentBalance(user);
		long fundAllocatedToPositions = fundsAllocatedToPositions(user);
		long fundsAllocatedToOrders = fundsAllocatedToOrders(user);
		long total = curBalance - fundsAllocatedToOrders - fundsAllocatedToPositions;
		return total;
	}
	
	private synchronized long currentBalance (long user) {
		long coins = 0;
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			con = cpds.getConnection();
			ps = con.prepareStatement("SELECT * FROM users WHERE id = ?");
			ps.setLong(1, user);
			rs = ps.executeQuery();
			rs.next();
			coins = rs.getLong("coins");
			rs.close();
			ps.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error executing transaction");
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
		return coins;
	}
	
	private synchronized long fundsAllocatedToOrders (long user) {
		long totalFunds = 0;
		try {
			ArrayList<Order> orders = ob.getUserOrders(user);
			for (Order o : orders) {
				if (o.bid){
					totalFunds += o.price * o.quantity;
				}
				else {
					Security s = (ob.getSecurity(o.security));
					System.out.println(s);
					totalFunds += s.contractsize - o.price * o.quantity;
				}
			}
		} catch (Exception e) { e.printStackTrace(); }
		return totalFunds;
	}
	
	private synchronized long fundsAllocatedToPositions (long user) {
		long totalFunds = 0;
		try {
			ArrayList<Order> orders = ob.getUserOrders(user);
			for (Order o : orders) {
				if (o.bid){
					totalFunds += o.price * o.quantity;
				}
				else {
					Security s = (ob.getSecurity(o.security));
					System.out.println(s);
					totalFunds += s.contractsize - o.price * o.quantity;
				}
			}
		} catch (Exception e) { e.printStackTrace(); }
		return totalFunds;
	}
	
	private synchronized ArrayList<Position> getPositions (long user) {
		long totalFunds = 0;
		try {
			ArrayList<Order> orders = ob.getUserOrders(user);
			for (Order o : orders) {
				if (o.bid){
					totalFunds += o.price * o.quantity;
				}
				else {
					Security s = (ob.getSecurity(o.security));
					System.out.println(s);
					totalFunds += s.contractsize - o.price * o.quantity;
				}
			}
		} catch (Exception e) { e.printStackTrace(); }
		return totalFunds;
	}
	
	private String createDepositAddress(long userID) {
		String address = bnc.createReceivingAddress();
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			con = cpds.getConnection();
			ps = con.prepareStatement("INSERT INTO depositaddresses (userid, address) VALUES (?,?)");
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
			ps = con.prepareStatement("INSERT INTO transactions (securityid, price, quantity, buyerid, sellerid) VALUES (?,?,?,?,?)", Statement.RETURN_GENERATED_KEYS);
			ps.setLong(1, security);
			ps.setLong(2, price);
			ps.setLong(3, quantity);
			ps.setLong(4, buyerID);
			ps.setLong(5, sellerID);
			System.out.println(ps);
			ps.executeUpdate();
			rs = ps.getGeneratedKeys();
			rs.next();
			transactionID = rs.getLong(1);
			rs.close();
			ps.close();
			long cost = quantity*price;
			ps = con.prepareStatement("UPDATE users SET coins = coins + ? WHERE id = ?");
			ps.setLong(1, -cost);
			ps.setLong(2, buyerID);
			ps.executeUpdate();
			ps.setLong(1, cost);
			ps.setLong(2, sellerID);
			ps.executeUpdate();
			System.out.println(ps);
			ps.close();
			ps = con.prepareStatement("SELECT positions.id FROM positions WHERE userID=? and securityid=?");
			ps.setLong(1, buyerID);
			ps.setLong(2, security);
			System.out.println(ps);
			rs = ps.executeQuery();
			long buyerPositionID = -1;
			if (rs.next())
				buyerPositionID = rs.getLong(1);
			
			ps.setLong(1, sellerID);
			System.out.println(ps);
			rs = ps.executeQuery();
			long sellerPositionID = -1;
			if (rs.next())
				sellerPositionID = rs.getLong(1);
			
			ps = con.prepareStatement("UPDATE positions SET amount = amount + ? WHERE id=?");
			ps2 = con.prepareStatement("INSERT INTO positions (userid, securityid, amount) VALUES (?,?,?)");
				
			if(buyerPositionID  != -1) {
				ps.setLong(1, quantity);
				ps.setLong(2, buyerPositionID);
				System.out.println(ps);
				ps.executeUpdate();
			}
			else {
				ps2.setLong(1, buyerID);
				ps2.setLong(2, security);
				ps2.setLong(3, quantity);
				System.out.println(ps2);
				ps2.executeUpdate();
			}
			
			if(sellerPositionID  != -1) {
				ps.setLong(1, -quantity);
				ps.setLong(2, sellerPositionID);
				System.out.println(ps);
				ps.executeUpdate();
			}
			else {
				ps2.setLong(1, sellerID);
				ps2.setLong(2, security);
				ps2.setLong(3, -quantity);
				System.out.println(ps2);
				ps2.executeUpdate();
			}
			ps.close();
			ps2.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println("Error executing transaction");
			return 2;
		} finally {
			try {
				if (rs != null) {
	                rs.close();
				}
				if (ps != null) {
	                ps.close();
				}
				if (ps2 != null) {
	                ps2.close();
				}
				if (con != null) {
	                con.close();
				}
			} catch (Exception e) { e.printStackTrace(); System.err.println("Unable to close"); }
		}
		return transactionID;
	}
	
	
	public synchronized void withdraw(String address, long amount, long user) {
		System.out.println("in withdraw");
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			con = cpds.getConnection();
			ps = con.prepareStatement("UPDATE users SET coins = coins - ? WHERE id = ?");
			ps.setLong(1, amount + BitcoinNetworkClient.FEE);
			ps.setLong(2, user);
			ps.executeUpdate();
			ps.close();
			
			ps = con.prepareStatement("INSERT INTO withdrawals (amount, userID, fee) VALUES (?, ?, ?)");
			ps.setLong(1, amount);
			ps.setLong(2, user);
			ps.setLong(3, BitcoinNetworkClient.FEE);
			ps.executeUpdate();
			System.out.println("did db stuff");			
			bnc.sendCoins(address, amount);
			System.out.println("did bitcoin stuff");
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
				rs.close();
				ps.close();
				ps = con.prepareStatement("UPDATE users SET coins = coins + ? WHERE id = ?");
				ps.setLong(1, amount);
				ps.setLong(2, userID);
				ps.executeUpdate();
				ps.close();
								
				ps = con.prepareStatement("INSERT INTO deposits (amount, userID, address) VALUES (?, ?, ?)");
				ps.setLong(1, amount);
				ps.setLong(2, userID);
				ps.setString(3, address);
				ps.executeUpdate();
				ps.close();		
			}
			else {
				System.err.println(amount + " coins received from unknown address");
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