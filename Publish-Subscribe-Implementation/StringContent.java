package publish_subscribe;

import java.net.DatagramPacket;

public class StringContent implements PacketContent {
	String string;
	byte[] data;
	
	//constructor
	public StringContent(DatagramPacket packet) {
		
		data = packet.getData();
		
	}
	
	//constructor
	public StringContent(String string) {
		this.string = string;
		data = string.getBytes();
	}
	
	//constructor
	public StringContent(byte[] data) {
		this.data = data;
	}
	
	//return data
	public byte[] getData() {
		return data;
	}

	//return data as a string
	public String toString() {
		return new String(data);
	}
	
	//return passed byte array as string
	public String toString(byte[] temp) {
		return new String(temp);
	}

	//create new datagram packet with contents from this one
	public DatagramPacket toDatagramPacket() {
		DatagramPacket packet= null;
		try {
			packet= new DatagramPacket(data, data.length);
		}
		catch(Exception e) {e.printStackTrace();}
		return packet;
	}
}
