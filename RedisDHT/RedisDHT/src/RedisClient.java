import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;

public class RedisClient
{
	static ArrayList<Node> clusterNodes = new ArrayList<>();

	public static void main(String[] args)
	{
		Timer timer = new Timer();
		
		String ipAddress;
		String slots;
		int port;
		Node node = null;

		RedisService redisService;
		// TODO Auto-generated method stub
		Scanner sc = new Scanner(System.in);

		boolean exit = false;

		redisService = new RedisService();

		Random rand = new Random();

		File configFile = new File("/home/cloudcr1/launcher_config.ini");
		Scanner s = null;
		try {
			s = new Scanner(configFile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		String str = null;
		String[] splits = null;

		int count = Integer.parseInt(s.nextLine());

		boolean isFirst = true;

		while(count-- > 0)
		{
			str = s.nextLine();
			splits = str.split(":");

			ipAddress = splits[1];
			port = Integer.parseInt(splits[2]);

			if(isFirst)
			{
				timer.start();
				node = redisService.addFirstNode(ipAddress, port);
				timer.end();
				
				System.out.println("Time taken to add node: " + timer.toString());
				isFirst = false;
				clusterNodes.add(node);
			}
			else
			{
				timer.start();
				node = redisService.addNode(ipAddress, port, clusterNodes.get(0));
				timer.end();
				
				System.out.println("Time taken to add node: " + timer.toString());
				clusterNodes.add(node);
			}
		}
		
		s.close();

		while(!exit)
		{
			System.out.println("Redis Cluster Operations");
			System.out.println("Select any one");
			System.out.println("1. Add a node");
			System.out.println("2. Delete a node");
			System.out.println("3. Add Slots");
			System.out.println("4. Delete Slots");
			System.out.println("5. Load Balancing");
			System.out.println("Enter your choice: ");
			int choice = sc.nextInt();

			switch (choice)
			{
			case 1: 
				System.out.println("Enter IP");
				ipAddress = sc.next();
				//ipAddress = "10.176.128.111";
				System.out.println("Enter port");
				port = sc.nextInt();

				timer.start();
				if(clusterNodes.isEmpty())
				{
					node = redisService.addFirstNode(ipAddress, port);
				}
				else
				{
					//TODO
					/*
					 * Don't hardcode 0. If deleting the centralized node then process the request but change the centralized to something else.
					 */
					int idx = rand.nextInt(clusterNodes.size());
					node = redisService.addNode(ipAddress, port, clusterNodes.get(idx));
				}
				timer.end();
				
				clusterNodes.add(node);
				
				System.out.println("Time taken to add node: " + timer.toString());

				break;
			case 2:
				System.out.println("Enter IP");
				ipAddress = sc.next();
				//ipAddress = "10.176.128.111";
				System.out.println("Enter port");
				port = sc.nextInt();

				for(int i = 0; i < clusterNodes.size(); i++)
				{
					Node n = clusterNodes.get(i);
					if(n.getIp().equals(ipAddress) && n.getPort() == port)
					{
						timer.start();
						//remove from control client
						clusterNodes.remove(i);
						
						int idx = rand.nextInt(clusterNodes.size());
						
						redisService.deleteNode(n.getId(), n.getIp(), n.getPort(), clusterNodes.get(idx));
						timer.end();
						
						System.out.println("Time taken to delete node: " + timer.toString());
						break;
					}
				}
				break;
				//ADDSLOTS IP:PORT RANGE
				//ADDSLOTS IP:PORT space delimited numbers
			case 3:
				System.out.println("Enter IP");
				ipAddress = sc.next();
				//ipAddress = "10.176.128.111";
				System.out.println("Enter port");
				port = sc.nextInt();
				sc.nextLine();
				System.out.print("Enter Slots: ");
				slots = sc.nextLine();
				
				timer.start();
				redisService.addSlots(ipAddress, port, slots);
				timer.end();
				
				System.out.println("Time taken to add slots: " + timer.toString());
				break;

			case 4:
				System.out.println("Enter IP");
				ipAddress = sc.next();
				//ipAddress = "10.176.128.111";
				System.out.println("Enter port");
				port = sc.nextInt();
				sc.nextLine();
				System.out.print("Enter Slots: ");
				slots = sc.nextLine();
				
				timer.start();
				redisService.deleteSlots(ipAddress, port, slots);
				timer.end();
				
				System.out.println("Time taken to delete slots: " + timer.toString());
				
				break;

			case 5:
				System.out.println("Enter From IP");
				String fromIpAddress = sc.next();
				//String fromIpAddress = "10.176.128.111";
				System.out.println("Enter port");
				int fromPort = sc.nextInt();
				System.out.println("Enter to IP");
				String toIpAddress = sc.next();
				//String toIpAddress = "10.176.128.111";
				System.out.println("Enter port");
				int toPort = sc.nextInt();
				sc.nextLine();
				System.out.print("Enter Slots: ");
				slots = sc.nextLine();

				for(Node n : clusterNodes)
				{
					if(n.getIp().equals(toIpAddress) && n.getPort() == toPort)
					{
						System.out.println(toIpAddress+":"+toPort);
						System.out.println("Calling loadbalance");
						timer.start();
						redisService.loadBalance(fromIpAddress, fromPort, n.getId(), slots);
						timer.end();
						
						System.out.println("Time taken to load balance: " + timer.toString());
						break;
					}
				}

				break;

			case 6:
				exit = true;
			}
		}

		sc.close();
	}

}
