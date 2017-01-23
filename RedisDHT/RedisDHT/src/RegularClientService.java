import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public class RegularClientService
{
	private static final int MESSAGE_SIZE = 4096000;

	public ArrayList<Node> updateRoutingTable(String remoteIp, int remotePort, int choice)
	{
		System.out.println("Querying " + remoteIp + " " + remotePort);
		InetSocketAddress inetSocketAddress = new InetSocketAddress(remoteIp, remotePort);
		Message msg = null;

		try {
			SocketChannel socketChannel = SocketChannel.open(inetSocketAddress);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			msg = new Message("fetchroutingtable");

			oos.writeObject(msg);
			oos.flush();
			
			byte[] message = baos.toByteArray();

			ByteBuffer buffer = ByteBuffer.wrap(message);
			socketChannel.write(buffer);

			//listen for response
			buffer.clear();

			ByteBuffer buf = ByteBuffer.allocate(MESSAGE_SIZE);

			socketChannel.read(buf);

			ByteArrayInputStream bais = new ByteArrayInputStream(buf.array());
			ObjectInputStream ois = new ObjectInputStream(bais);

			try
			{
				Object readObject = ois.readObject();

				if(readObject instanceof Message)
				{
					msg = (Message) readObject;
				}
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}

			socketChannel.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if(choice != 3)
			System.out.println("Routing table successfully updated!!");

		return msg.getClusterNodes();
	}

	public ConcurrentSkipListMap<Integer, Node> updateSlots(String remoteIp, int remotePort)
	{
		System.out.println("Querying " + remoteIp + " " + remotePort);
		
		InetSocketAddress inetSocketAddress = new InetSocketAddress(remoteIp, remotePort);
		Message msg = null;

		try {
			SocketChannel socketChannel = SocketChannel.open(inetSocketAddress);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			msg = new Message("fetchslots");

			oos.writeObject(msg);
			oos.flush();
			
			byte[] message = baos.toByteArray();

			ByteBuffer buffer = ByteBuffer.wrap(message);
			socketChannel.write(buffer);

			//listen for response
			buffer.clear();

			ByteBuffer buf = ByteBuffer.allocate(MESSAGE_SIZE);

			socketChannel.read(buf);

			ByteArrayInputStream bais = new ByteArrayInputStream(buf.array());
			ObjectInputStream ois = new ObjectInputStream(bais);

			try
			{
				Object readObject = ois.readObject();

				if(readObject instanceof Message)
				{
					msg = (Message) readObject;
				}
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}

			socketChannel.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("Slots table successfully updated!!");

		//temporarily handled
		RegularClient.version = msg.getVersion();

		return msg.getSlots();
	}

	public Set<Node> getReplicas(String remoteIp, int remotePort)
	{
		System.out.println("Querying " + remoteIp + " " + remotePort);
		
		InetSocketAddress inetSocketAddress = new InetSocketAddress(remoteIp, remotePort);
		Message msg = null;

		try {
			SocketChannel socketChannel = SocketChannel.open(inetSocketAddress);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			msg = new Message("fetchreplicas");

			oos.writeObject(msg);
			oos.flush();
			
			byte[] message = baos.toByteArray();

			ByteBuffer buffer = ByteBuffer.wrap(message);
			socketChannel.write(buffer);

			//listen for response
			buffer.clear();

			ByteBuffer buf = ByteBuffer.allocate(MESSAGE_SIZE);

			socketChannel.read(buf);

			ByteArrayInputStream bais = new ByteArrayInputStream(buf.array());
			ObjectInputStream ois = new ObjectInputStream(bais);

			try
			{
				Object readObject = ois.readObject();

				if(readObject instanceof Message)
				{
					msg = (Message) readObject;
				}
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}

			socketChannel.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("Fetched replicas successfully");

		return msg.getReplicaList();	
	}
}
