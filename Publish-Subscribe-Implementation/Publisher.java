package publish_subscribe;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;

import tcdIO.Terminal;

//OBJECTIVES
//		1. Accept topic and message as input
//		2. Transmit topic and message to broker


public class Publisher extends Node{
	static final int DEFAULT_SRC_PORT = 50000;
	static final int DEFAULT_DST_PORT = 50001;	//broker
	static final String DEFAULT_DST_NODE = "localhost";	
	final int MTU = 65535;	//Maximum transmission Unit
	Terminal terminal;
	int port_number;
	InetSocketAddress broker;
	
	Publisher(int port_number, int broker_port) {
		
		this.port_number = port_number;
		terminal = new Terminal("Publisher");
		try {
			broker = new InetSocketAddress(DEFAULT_DST_NODE, broker_port);
			socket = new DatagramSocket(port_number);
			listener.go();
		}
		catch(java.lang.Exception e) {e.printStackTrace();}

	}
	
	synchronized void publish() throws InterruptedException {
		boolean publish = true;
		int seqNo = 0;
		while (publish) {
			
				//setting up
				DatagramPacket packet = null;
				//reading in the data
				try {

					String topic = terminal.readString("Topic: ");
					String message = terminal.readString("Message: ");
					message = topic + "-" + message;

					if (message.length()<MTU) {
						
						//format header
						byte[] dataTemp = (message).getBytes();
						byte[] data = new byte[dataTemp.length + 3];
						for (int i = 0; i < dataTemp.length; i++) {
							data[i + 3] = dataTemp[i];
						}
						//control
						int firstByte = 0b11; //from publisher
						firstByte = firstByte << 6; //first two bits are control (7,6)
						firstByte =  (firstByte | ((seqNo++) << 3)); //next three bits are seqNo (5,4,3)
						firstByte = (firstByte | seqNo); //next three are used to piggyback (2,1,0)
						//length
						int length = message.length();
						terminal.println("length = " + length);
						int one = ((length&0b1111111100000000)>>>8);
						int two = length&0b11111111;
						
						data[0] = (byte) firstByte;
						data[1] = (byte) one;
						data[2] = (byte) two;
						
						//sending the packet
						terminal.println("Sending packet...");
						packet = new DatagramPacket(data, data.length, broker);
						socket.send(packet);
						terminal.println("Packet sent");
						
						//check for ACK
						this.wait();
						
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
		}

	}
	
public synchronized void onReceipt(DatagramPacket packet) {
		try {
			StringContent content= new StringContent(packet);
			//take off header
			byte[] data = content.getData();
			//get control bits
			int control = data[0];
			control = control&0b11111111;
			control = control>>>6;
			if (control==0b1) {	//from  broker ie ACK
				this.notify();
			}	
		}
		catch(Exception e) {e.printStackTrace();}
	}
	
	public static void main (String[] args) {
	
		Publisher publisher = new Publisher(DEFAULT_SRC_PORT, DEFAULT_DST_PORT);
		try {					
			publisher.publish();
			publisher.terminal.println("Program completed");
		} catch(java.lang.Exception e) {e.printStackTrace();}
		
	}

}
