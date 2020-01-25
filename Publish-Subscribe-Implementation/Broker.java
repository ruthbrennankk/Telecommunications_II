package publish_subscribe;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;

import tcdIO.Terminal;

// OBJECTIVES
//		1. Receive Subscriptions
//		2. Maintain list of subscribers
//		3. Forward incoming messages to subscribers with matching topics

/*
 * 00 from subscriber to unsubscribe
 * 01 from broker
 * 10 from subscriber to subscribe
 * 11 from publisher to distribute
 */

public class Broker extends Node {
	static final int DEFAULT_SRC_PORT = 50001;
	static final int DEFAULT_DST_PORT = 50002;
	static final String DEFAULT_DST_NODE = "localhost";	
	
	int port_number; //= 50001;
	Terminal terminal;
	
	HashMap<String, ArrayList<InetSocketAddress>> topics;
	
	Broker(int port_number) {
		topics = new HashMap<String, ArrayList<InetSocketAddress>>();
		this.port_number=port_number;
		try {
			socket= new DatagramSocket(port_number);
			listener.go();
		}
		catch(java.lang.Exception e) {e.printStackTrace();}
		terminal = new Terminal("Broker");
	} 
	
	public synchronized void onReceipt(DatagramPacket packet) {
		
		try {
				StringContent content= new StringContent(packet);
				
				//take off header
				byte[] data = content.getData();
				byte[] dataTemp = new byte[data.length-3];
				for (int i=0; i<dataTemp.length; i++) {
					dataTemp[i] = data[i+3];
				}
				
				//check header
				//get control bits
				int control = data[0];
				control = control&0b11111111;
				control = control>>>6;
				//get length
				int length = (data[1]<<8)|data[2];
				//get sequence number and ACK seq no
				byte mask = 0b00111000; //clear bits (7,6,2,1,0)
				int seq = data[0]&mask;
				mask = 0b00000111;
				int ack = data[0]&mask;
				
				terminal.println("length = "+ length);
				
			if (control==0b11) {	//from publisher
					
					String topic = content.toString(dataTemp);
					terminal.println(topic);
					
					//Separate topic and message
					topic = topic.substring(0,length);
					String[] result = topic.split("-");
					topic = result[0];
					String message = result[1];
					
					//format topic
					topic = topic.replaceAll("\\s","");
					topic.toLowerCase();
					
					//check if topic has subscribers
					if (topics.containsKey(topic)) {
						//forward message to subscribers
						terminal.println("old topic");
						ArrayList<InetSocketAddress> addresses = topics.get(topic);
						
						if (addresses.size()>0){
							
							terminal.println("sending to subscribers");
							
							//format message
							dataTemp= (message).getBytes();
							data = new byte[dataTemp.length+1];
							for (int i=0; i<dataTemp.length; i++) {
								data[i+1] = dataTemp[i];
							}
							
							//format header
							int firstByte = 0b11;	//forwarded from publisher
							firstByte = firstByte<<6;	//first two bits are control
							firstByte = firstByte|seq; //next three bits are seq nos
							data[0] = (byte) firstByte;
							
							
							//send packets
							for (int i=0; i<addresses.size(); i++) {
								DatagramPacket subPacket;
								subPacket= (new StringContent(data)).toDatagramPacket();
								subPacket.setSocketAddress( addresses.get(i) );
								socket.send(subPacket);
							}
							
						} else {
							terminal.println("No Subscribers");
						}
					} else {
						terminal.println("new topic");
						//add topic to hashmap
						topics.put(topic, new ArrayList<InetSocketAddress>()); 
					}
					
					//acknowledgement
					DatagramPacket response;
					byte acknowledgement[] = new byte[1];
					int firstByte = 0b1;	//from broker
					firstByte = firstByte<<6;	//first two bits are control
					firstByte = firstByte|ack; //next three bits are seq no
					acknowledgement[0] = (byte) firstByte;
					response= (new StringContent(acknowledgement)).toDatagramPacket();
					response.setSocketAddress(packet.getSocketAddress());
					socket.send(response);
					
				} else if (control == 0b10) {	//from subscriber
					
					String topic = content.toString(dataTemp);
					terminal.println(topic);
					
					//format topic
					topic = topic.replaceAll("\\s","");
					topic.toLowerCase();
					topic = topic.substring(0,length);
					
					int sub = 0b0;
					
					if ( (int)(data[0]&0b1) == 0b1 ) {	//subscribe
						
						sub=0b1;
						
						//does topic already exist
						if (topics.containsKey(topic)) {
							
							terminal.println("old topic");
							topics.get(topic).add( (InetSocketAddress)packet.getSocketAddress() );
						
						} else {
							
							//add this topic & subscriber
							terminal.println("new topic");
							topics.put(topic, new ArrayList<InetSocketAddress>() );
							topics.get(topic).add((InetSocketAddress) packet.getSocketAddress());
							
						}
					} else {	//unsubscribe
						
						//does topic already exist
						if (topics.containsKey(topic)) {
							
							topics.get(topic).remove( (InetSocketAddress)packet.getSocketAddress() );
						
						}
					}
					//acknowledgement
					DatagramPacket response;
					byte acknowledgement[] = new byte[1];
					int firstByte = 0b1;	//from broker
					firstByte = firstByte<<6;	//first two bits are control
					firstByte = firstByte|sub; //last bit is sub/unsub
					acknowledgement[0] = (byte) firstByte;
					response= (new StringContent(acknowledgement)).toDatagramPacket();
					response.setSocketAddress(packet.getSocketAddress());
					socket.send(response);
				} 
		}
		catch(Exception e) {e.printStackTrace();}	
	}

	public synchronized void start() throws Exception {
		
		terminal.println("Waiting for contact");
		this.wait();
		terminal.println("Program completed");
		
	}

	public static void main (String[] args) {
		
		Broker broker = new Broker(DEFAULT_SRC_PORT);
		try {					
			broker.start();
		} catch(java.lang.Exception e) {e.printStackTrace();}
		
	}
	
}
