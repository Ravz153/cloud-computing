import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.ArrayList;
import java.util.UUID;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class MainDriver
{
	private Master m;
	private Network network;
	
	public MainDriver(String uniqueID, String address, Logger logger)
	{
		this.m = new Master(uniqueID);
		
		this.network = new Network(m, address, logger);
	}
	
	public void execute()
	{
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run()
			{
				//close redis Server channel
				ServerSocketChannel redisServerSocketChannel = network.getRedisServerSocketChannel();
				ServerSocketChannel clientServerSocketChannel = network.getClientServerSocketChannel();
				
				try
				{
					redisServerSocketChannel.close();
					clientServerSocketChannel.close();
					
					ArrayList<Node> clusterNodes = m.getClusterNodes();
					
					for(Node n : clusterNodes)
					{
						n.getSocketChannel().close();
					}
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
				
				System.out.println("Goodbye!!");
			}
		}));
		
		Thread net = new Thread(network);
		net.start();
	}
	
	public static void main(String[] args)
	{
		Logger logger = Logger.getLogger("MyLog");
		
		String address = args[0];
		
		//can be same...check later
		String uniqueID = UUID.randomUUID().toString();
		
		FileHandler fh = null; 
		try
		{
			fh = new FileHandler("/root/RedisDHT_logs/MyLogFile_"+uniqueID+".log");
			logger.addHandler(fh);
		}
		catch (SecurityException e1)
		{
			logger.log(Level.INFO, e1.getMessage(), e1);
		}
		catch (IOException e1)
		{
			logger.log(Level.INFO, e1.getMessage(), e1);
		}
		
		fh.setFormatter(new SimpleFormatter());
		
		MainDriver main = new MainDriver(uniqueID, address, logger);
		main.execute();
	}
}
