package publish_subscribe;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.ArrayList;

import tcdIO.Terminal;

//OBJECTIVES
//		1. Accept Topic as input  
//		2. Send subscription message to broker
//		3. Print published messages from the broker


public class Subscriber extends Node{

	static final int DEFAULT_SRC_PORT = 50002;	//subscriber
	static final int DEFAULT_DST_PORT = 50001;	//broker
	static final String DEFAULT_DST_NODE = "localhost";
	final int MTU = 65535;	//Maximum transmission Unit
	
	int port_number;
	Terminal terminal;
	InetSocketAddress broker;
	ArrayList<String> messages;
	
	Subscriber(int port_number, int broker_port) {
		broker = new InetSocketAddress(DEFAULT_DST_NODE, broker_port);
		this.port_number = port_number;
		
		try {
			socket= new DatagramSocket(port_number);
			listener.go();
		}
		catch(java.lang.Exception e) {e.printStackTrace();}
		
		terminal = new Terminal("Subscriber");
		messages = new ArrayList<String>();
		
	} 
	
	public synchronized void onReceipt(DatagramPacket packet) {
		
		this.notify();
		try {
			
			StringContent content= new StringContent(packet);
			
			//take off header
			byte[] data = content.getData();
			byte[] dataTemp = new byte[data.length -1];
			for (int i=0; i<dataTemp.length; i++) {
				dataTemp[i] = data[i+1];
			}
			
			//check header
			int control = data[0]&0b11111111;
			control = control>>>6;
			
			if ( control==0b11) {	//from  publisher
				String message = content.toString(dataTemp);
				int seqNo = (data[0]&0b00111000)>>3;
				message = Integer.toString(seqNo) + "-" + message;
				
				if (messages.size()>0) {
					String tmp = (messages.get(messages.size() - 1)).toString();
					String[] result = tmp.split("-");
					int endSeq = Integer.parseInt(result[0]);
					tmp = (messages.get(0)).toString();
					result = tmp.split("-");
					int startSeq = Integer.parseInt(result[0]);
					if (seqNo <= startSeq) {
						messages.add(0, message);
					} else if (seqNo > endSeq) {
						messages.add(message);
					} else {
						boolean found = false;
						int low = 0, middle, high = messages.size() - 1;
						while (!found) {
							middle = high / 2;
							tmp = (messages.get(middle)).toString();
							result = tmp.split("-");
							int i = Integer.parseInt(result[0]);
							if (seqNo > i) {
								low = middle;
							} else { //if (seqNo<i) {
								high = middle;
							}
							if (high == low) {
								found = true;
							}
						}
						messages.add(low, message);
					} 
				} else {
					messages.add(message);
				}
			} else if (control==0b1) {	//from broker ie ACK
				this.notify();
			}
		}
		catch(Exception e) {e.printStackTrace();}	
		
	}
	
	public synchronized void subscribe() throws Exception {
		
		boolean subscribe = true;
		while (subscribe) {
			
			String temp = terminal.readString("to subscribe/unsubscribe press enter to recieve messages press any other key");
			
			if ( temp.equals("") ) {
					byte[] dataTemp = null;
					DatagramPacket packet = null;
					
					String topic = terminal.readString("Topic: ");
					dataTemp = (topic).getBytes();
					byte[] data = new byte[dataTemp.length + 3];
					
					if (topic.length()<MTU) {
						String sub = terminal.readString("Type 1->subscribe, 2->unsubscribe");
						sub = sub.replaceAll("\\s", ""); //error handling for spaces
						if (sub.equals("1") || sub.equals("2")) {

							//header
							for (int i = 0; i < dataTemp.length; i++) {
								data[i + 3] = dataTemp[i];
							}
							//control
							int firstByte = 0b10;
							firstByte = firstByte << 6; //first two bits are control
							if (sub.equals("2"))
								firstByte = firstByte | 0b0; //last bit is for subscribe
							else
								firstByte = firstByte | 0b1; //or unsubscribe
							//length
							int length = topic.length();
							int one = ((length&0b1111111100000000)>>>8);
							int two = length&0b11111111;

							
							data[0] = (byte) firstByte;
							data[1] = (byte) one;	
							data[2] = (byte) two;
							
							//send packet
							terminal.println("Sending packet...");
							packet = new DatagramPacket(data, data.length, broker);
							socket.send(packet);
							terminal.println("Packet sent");

						} else {
							terminal.println("Error, subscripion/unsubsribtion not sent");
						} 
					} else {
						terminal.println("Error, topic too long");
					} 
					this.wait();	//stop & wait
				
			} else {
				this.wait(500);
				for (int i=0; i<messages.size(); i++) {
					terminal.println(messages.get(i));
					messages.remove(i);
					this.wait(500);
				}
				terminal.println("That's all for now");	
			}
		}
	}
	
	public static void main (String[] args) {
		
		Subscriber subscriber = new Subscriber(DEFAULT_SRC_PORT, DEFAULT_DST_PORT);
		try {					
			subscriber.subscribe();
			subscriber.terminal.println("Program completed");
		} catch(java.lang.Exception e) {e.printStackTrace();}
		
	}

}
