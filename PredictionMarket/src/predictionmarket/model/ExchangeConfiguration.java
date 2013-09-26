package predictionmarket.model;

import java.io.*;

import org.json.JSONObject;

public class ExchangeConfiguration {
	private static final String DEFAULT_ADDRESS = "127.0.0.1";
	private static final int DEFAULT_PORT = 3176;
	private static final boolean BTC_NETWORK_TOGGLE = true;
	
	public String address;
	public int port;
	public boolean btcNetworkToggle;
	
	public ExchangeConfiguration () {
		address = DEFAULT_ADDRESS;
		port = DEFAULT_PORT;
		btcNetworkToggle = BTC_NETWORK_TOGGLE;
	}
	
	public void loadFromFile(String fileName) {
		try {
			System.out.println(fileName);
			File f = new File(fileName);
			if(!f.exists())
				return;
			
			BufferedReader br = new BufferedReader(new FileReader(f));
			
			String line = null;
			String jsonString = "";
				
			while ((line = br.readLine()) != null) {
				jsonString += line;
			}
			br.close();
			JSONObject job = new JSONObject (jsonString);
			
			address = job.optString("address", DEFAULT_ADDRESS);
			port = job.optInt("port", DEFAULT_PORT);
			btcNetworkToggle = job.optBoolean("btc_network_toggle", BTC_NETWORK_TOGGLE);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
