package predictionmarket.btcnetwork;

import java.io.*;
import java.net.*;
import java.math.*;

import com.google.bitcoin.core.*;
import com.google.bitcoin.core.TransactionConfidence.Listener.ChangeReason;
import com.google.bitcoin.discovery.*;
import com.google.bitcoin.params.*;
import com.google.bitcoin.store.*;

import predictionmarket.exchange.*;

public class BitcoinNetworkClient extends Thread {
	private NetworkParameters params;
    private Wallet wallet;
    private PeerGroup peerGroup;
    private BlockChain chain;
	private Exchange boe;
    public static final long FEE = 50000L;
	
    public BitcoinNetworkClient (Exchange boe) {
    	this.boe = boe;
    	try {
    		setup();
    	} catch (Exception e) { e.printStackTrace(); }
    }
    
    public String createReceivingAddress () {
    	String strAddr = null;
    	try {
	    	ECKey key = new ECKey();
	    	wallet.addKey(key);
	    	Address addr = key.toAddress(params);
	    	strAddr = addr.toString(); 
    	} catch (Exception e) { e. printStackTrace(); }
    	return strAddr;
    }
    
    public void setup() throws Exception {
		System.out.println("start steup");
		params = MainNetParams.get();
		
		// Try to read the wallet from storage, create a new one if not possible.
		final File walletFile = new File("wallet/options.wallet");
		
		boolean freshWallet = false;
		try {
		    wallet = Wallet.loadFromFile(walletFile);
		} catch (Exception e) {
		    wallet = new Wallet(params);
		    wallet.addKey(new ECKey());
		    wallet.saveToFile(walletFile);
		    freshWallet = true;
		}
		
		// Load the block chain, if there is one stored locally.
		System.out.println("Reading block store from disk");
		File blockChainFile = new File("wallet/options.blockchain");
		
		if (!blockChainFile.exists() && !freshWallet) {
	            // No block chain, but we had a wallet. So empty out the transactions in the wallet so when we rescan
	            // the blocks there are no problems (wallets don't support replays without being emptied).
	            wallet.clearTransactions(0);
		}
		// BlockStore blockStore = new DiskBlockStore(params, blockChainFile);
		BlockStore blockStore = new SPVBlockStore(params, blockChainFile);
		// BlockStore blockStore = new MemoryBlockStore(params);
		chain = new BlockChain(params, wallet, blockStore);
		
		peerGroup = new PeerGroup(params, chain);
		peerGroup.setUserAgent("MyUserAgent", "1.0");
		peerGroup.addWallet(wallet);
		peerGroup.setFastCatchupTimeSecs(wallet.getEarliestKeyCreationTime());
		peerGroup.addAddress(new PeerAddress(InetAddress.getLocalHost()));
		peerGroup.addPeerDiscovery(new DnsDiscovery(params));
		peerGroup.addPeerDiscovery(new SeedPeers(params));
		wallet.addEventListener(new AbstractWalletEventListener() {
	            @Override
	            public void onCoinsReceived(Wallet wallet, Transaction tx, BigInteger prevBalance, BigInteger newBalance) {
	                super.onCoinsReceived(wallet, tx, prevBalance, newBalance);
	                System.out.println("Coins received");
	                for (TransactionOutput txOut : tx.getOutputs()) {
		                if (txOut.isMine(wallet)) {
		                	try {
			                	final long amount = txOut.getValue().longValue();
			                	final Address toAddress = txOut.getScriptPubKey().getToAddress(params);
				                TransactionConfidence tc = tx.getConfidence();
				                tc.addEventListener(new TransactionConfidence.Listener() {
				                	boolean alreadyAdded = false;
									public void onConfidenceChanged(Transaction tx, ChangeReason reason) {
										int depth = tx.getConfidence().getDepthInBlocks();
										System.out.println ("Depth of transaction: " + depth);
										if (depth > 1 && !alreadyAdded) {
											boe.receivedCoins(toAddress.toString(), amount);
											alreadyAdded = true;
										}
									}
								});
		                	} catch (Exception e) { e.printStackTrace(); }
		                }
	                }
	            }
	          
	            
	            @Override
	            public void onCoinsSent(Wallet wallet, Transaction tx, BigInteger prevBalance, BigInteger newBalance) {
	            	super.onCoinsSent(wallet, tx, prevBalance, newBalance);
	            	System.out.println("Coins were sent");
	            }

	            @Override
	            public void onChange() {
	                try {
	                    //System.out.println("Wallet changed");
	                    wallet.saveToFile(walletFile);
	                } catch (IOException e) {
	                    throw new RuntimeException(e);
	                }
	            }
	        });
		peerGroup.start();
		peerGroup.downloadBlockChain();
	}
    
    public int sendCoins (String toStr, long amount) {
    	Transaction sendTx = null;
    	try {
	    	Address toAddress = new Address(params, toStr);
	    	BigInteger amountToSend = BigInteger.valueOf(amount-FEE);
	    	System.out.println(toAddress);
	    	System.out.println(amountToSend);
	    	
	    	final Wallet.SendResult sendResult = wallet.sendCoins(peerGroup, toAddress, amountToSend);
	    	System.out.println("Sending ...");
	    	
    	} catch (Exception e) {
    		e.printStackTrace();
    		System.err.println("Unable to send " + amount + " to " + toStr);
    	}
    	int retCode = 0;
    		
    	if(sendTx == null)
    		retCode = 1;
	    return retCode;
    }
}
