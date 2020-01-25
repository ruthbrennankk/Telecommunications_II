package publish_subscribe;

import java.net.DatagramPacket;

public interface PacketContent {
	public String toString();
	public DatagramPacket toDatagramPacket();
}
