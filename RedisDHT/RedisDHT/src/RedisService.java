
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class RedisService {

	private static final int MESSAGE_SIZE = 4096000;

	public void loadBalance(String fromIpAddress, int fromPort, String toUuid, String slots)
	{
		InetSocketAddress fromInetSocketAddress = new InetSocketAddress(fromIpAddress, fromPort);

		try
		{
			SocketChannel socketChannel = SocketChannel.open(fromInetSocketAddress);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);

			Message msg = new Message("loadbalance");

			msg.setPayload(toUuid + ":" + slots);

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

					System.out.println(msg.getPayload());
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

	}

	public void addSlots(String ipAddress, int port, String slots)
	{
		InetSocketAddress inetSocketAddress = new InetSocketAddress(ipAddress, port);

		try {
			SocketChannel socketChannel = SocketChannel.open(inetSocketAddress);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			Message msg = new Message("addslots");

			msg.setPayload(slots);

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

					System.out.println(msg.getPayload());
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
	}

	public void deleteSlots(String ipAddress, int port, String slots)
	{
		InetSocketAddress inetSocketAddress = new InetSocketAddress(ipAddress, port);

		try {
			SocketChannel socketChannel = SocketChannel.open(inetSocketAddress);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			Message msg = new Message("deleteslots");

			msg.setPayload(slots);

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

					System.out.println(msg.getPayload());
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
	}

	//in all functions assuming clusterNode is always A (Centralized)
	public void deleteNode(String uuid, String ipAddress, int port, Node clusterNode)
	{
		InetSocketAddress inetSocketAddress = new InetSocketAddress(clusterNode.getIp(), clusterNode.getPort());
		try {
			SocketChannel socketChannel = SocketChannel.open(inetSocketAddress);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			Message msg = new Message("delete");
			msg.setPayload(uuid+":"+ipAddress+":"+port);
			oos.writeObject(msg);
			oos.flush();
			
			byte[] message = baos.toByteArray();

			ByteBuffer buffer = ByteBuffer.wrap(message);
			socketChannel.write(buffer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Node addNode(String ipAddress, int port, Node clusterNode)
	{
		String id = null;
		InetSocketAddress inetSocketAddress = new InetSocketAddress(ipAddress, port);
		try {
			SocketChannel socketChannel = SocketChannel.open(inetSocketAddress);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			Message msg = new Message("addnode");
			msg.setPayload(clusterNode.getId()+":"+clusterNode.getIp()+":"+clusterNode.getPort());
			oos.writeObject(msg);
			oos.flush();
			
			byte[] message = baos.toByteArray();

			ByteBuffer buffer = ByteBuffer.wrap(message);
			socketChannel.write(buffer);
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

					if(msg.getType().equals("uuid"))
					{
						id = msg.getPayload();
					}
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

		System.out.println("Received uuid : " + id);

		Node n = new Node(id, port, ipAddress, true);

		return n;
	}

	public Node addFirstNode(String ipAddress, int port)
	{
		String id = null;
		InetSocketAddress inetSocketAddress = new InetSocketAddress(ipAddress, port);
		try {
			SocketChannel socketChannel = SocketChannel.open(inetSocketAddress);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			Message msg = new Message("addnode");
			oos.writeObject(msg);
			oos.flush();
			
			byte[] message = baos.toByteArray();

			//byte[] message = new String("addnode").getBytes();
			ByteBuffer buffer = ByteBuffer.wrap(message);
			socketChannel.write(buffer);
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

					if(msg.getType().equals("uuid"))
					{
						id = msg.getPayload();
					}
				}
			}
			catch (ClassNotFoundException e)
			{
				e.printStackTrace();
			}

			System.out.println("Received uuid : " + id);

			socketChannel.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Node n = new Node(id, port, ipAddress, true);

		return n;
	}
}
