import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;

public class Node implements Serializable
{
	private static final long serialVersionUID = 3680234181551558686L;

	private String id;
	private int port;
	private String ip;
	private boolean isMaster;
	private InetSocketAddress neighborAddress;
	private transient SocketChannel socketChannel;
	
	public Node()
	{
		this.id = "";
	}

	public Node(String id, int port, String ip, boolean isMaster)
	{
		this.id = id;
		this.port = port;
		this.ip = ip;
		this.isMaster = isMaster;

		try
		{
			neighborAddress = new InetSocketAddress(InetAddress.getByName(ip), port);
		}
		catch (UnknownHostException e)
		{
			e.printStackTrace();
		}
	}

	public String getId()
	{
		return id;
	}

	public void setId(String id)
	{
		this.id = id;
	}

	public int getPort()
	{
		return port;
	}

	public void setPort(int port)
	{
		this.port = port;
	}

	public String getIp()
	{
		return ip;
	}

	public void setIp(String ip)
	{
		this.ip = ip;
	}

	public boolean isMaster()
	{
		return isMaster;
	}

	public void setMaster(boolean isMaster)
	{
		this.isMaster = isMaster;
	}

	public InetSocketAddress getNeighborAddress() {
		return neighborAddress;
	}

	public void setNeighborAddress(InetSocketAddress neighborAddress) {
		this.neighborAddress = neighborAddress;
	}

	public SocketChannel getSocketChannel()
	{
		return socketChannel;
	}

	public void setSocketChannel(SocketChannel socketChannel)
	{
		this.socketChannel = socketChannel;
	}

	public String toString()
	{
		return id+" "+ip+" "+port;
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result	+ ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		Node other = (Node) obj;
		if (id == null) {
			if (other.id != null) {
				return false;
			}
		} else if (!id.equals(other.id)) {
			return false;
		}
		return true;
	}
}
