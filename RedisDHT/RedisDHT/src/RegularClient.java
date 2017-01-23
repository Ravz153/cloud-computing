import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import com.bethecoder.ascii_table.ASCIITable;

public class RegularClient {
	static ConcurrentSkipListMap<Integer, Node> slotsTable = new ConcurrentSkipListMap<>();
	static ArrayList<Node> routingTable = new ArrayList<>();
	static HashMap<String, Integer> summary;
	public static int[] version;
	static int currMaxVersion = 0;
	static String lastUpdatedById = "";

	static final String[] ROUTING_TABLE_HEADER = { "UUID", "IP ADDRESS", "PORT" };
	static final String[] LOOKUP_TABLE_HEADER = { "SLOTS", "UUID", "IP ADDRESS", "PORT", "VERSION" };

	// print summary..counts of slots held by each node
	static void summarize() {
		summary = new HashMap<>();

		Iterator it = slotsTable.entrySet().iterator();

		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry) it.next();

			Node value = (Node) pair.getValue();

			String ip = value.getIp();
			int port = value.getPort() - 10000;

			String concat = ip + ":" + Integer.toString(port);

			if (summary.containsKey(concat)) {
				int count = summary.get(concat);
				count++;
				summary.put(concat, count);
			} else {
				summary.put(concat, 1);
			}
		}
	}

	static void printSlots(String slots) {
		/*
		 * slots can be a range 1-10 slots can be space seperated slots can be
		 * ip
		 */

		String[] header = new String[LOOKUP_TABLE_HEADER.length];
		String[][] data = null;

		for (int i = 0; i < LOOKUP_TABLE_HEADER.length; i++) {
			header[i] = LOOKUP_TABLE_HEADER[i];
		}

		if (slotsTable.isEmpty()) {
			System.out.println("Hash Slots table is empty!!");
			return;
		}

		String[] splits = null;

		if (slots.contains("-")) {
			splits = slots.split("-");

			int start = Integer.parseInt(splits[0]);
			int end = Integer.parseInt(splits[1]);

			data = new String[end - start + 1][LOOKUP_TABLE_HEADER.length];

			int x = 0;

			for (int i = start; i <= end; i++) {
				for (int j = 0; j < LOOKUP_TABLE_HEADER.length; j++) {
					if (j == 0) {
						data[x][j] = Integer.toString(i);
					} else if (j == 1) {
						if (!slotsTable.get(i).getId().equals("")) {
							data[x][j] = slotsTable.get(i).getId();
						} else {
							data[x][j] = "UNASSIGNED";
						}
					} else if (j == 2) {
						if (!slotsTable.get(i).getId().equals("")) {
							data[x][j] = slotsTable.get(i).getIp();
						} else {
							data[x][j] = "UNASSIGNED";
						}
					} else if (j == 3) {
						if (!slotsTable.get(i).getId().equals("")) {
							data[x][j] = Integer.toString(slotsTable.get(i).getPort() - 10000);
						} else {
							data[x][j] = "UNASSIGNED";
						}
					} else if (j == 4) {
						if (!slotsTable.get(i).getId().equals("")) {
							data[x][j] = Integer.toString(version[i]);
						} else {
							data[x][j] = "UNASSIGNED";
						}
					}
				}
				x++;
			}
		}

		else if (slots.contains(" ")) {
			splits = slots.split(" ");
			int[] tSlots = new int[splits.length];

			for (int i = 0; i < splits.length; i++) {
				tSlots[i] = Integer.parseInt(splits[i]);
			}

			data = new String[tSlots.length][LOOKUP_TABLE_HEADER.length];

			for (int i = 0; i < tSlots.length; i++) {
				for (int j = 0; j < LOOKUP_TABLE_HEADER.length; j++) {
					if (j == 0) {
						data[i][j] = Integer.toString(tSlots[i]);
					} else if (j == 1) {
						if (!slotsTable.get(tSlots[i]).getId().equals("")) {
							data[i][j] = slotsTable.get(tSlots[i]).getId();
						} else {
							data[i][j] = "UNASSIGNED";
						}
					} else if (j == 2) {
						if (!slotsTable.get(tSlots[i]).getId().equals("")) {
							data[i][j] = slotsTable.get(tSlots[i]).getIp();
						} else {
							data[i][j] = "UNASSIGNED";
						}
					} else if (j == 3) {
						if (!slotsTable.get(tSlots[i]).getId().equals("")) {
							data[i][j] = Integer.toString(slotsTable.get(tSlots[i]).getPort() - 10000);
						} else {
							data[i][j] = "UNASSIGNED";
						}
					} else if (j == 4) {
						if (!slotsTable.get(tSlots[i]).getId().equals("")) {
							data[i][j] = Integer.toString(version[tSlots[i]]);
						} else {
							data[i][j] = "UNASSIGNED";
						}
					}
				}

			}
		} else if (slots.contains(":")) {
			splits = slots.split(":");
			String ip = splits[0];
			int port = Integer.parseInt(splits[1]);

			String uuid = null;

			boolean found = false;

			for (Node n : routingTable) {
				if (n.getIp().equals(ip) && n.getPort() - 10000 == port) {
					uuid = n.getId();
					found = true;
					break;
				}
			}

			if (!found) {
				System.out.println("Node not part of cluster");
				return;
			}

			// slots ip:port
			data = new String[summary.get(slots)][LOOKUP_TABLE_HEADER.length];

			Iterator it = slotsTable.entrySet().iterator();
			int x = 0, y = 0;

			while (it.hasNext()) {
				y = 0;

				Map.Entry pair = (Map.Entry) it.next();

				int key = (int) pair.getKey();

				Node value = (Node) pair.getValue();

				if (value.getId().equals(uuid)) {
					data[x][y++] = Integer.toString(key);
					data[x][y++] = value.getId();
					data[x][y++] = value.getIp();
					data[x][y++] = Integer.toString(value.getPort() - 10000);
					data[x][y++] = Integer.toString(version[key]);
					x++;
				}
			}
		}

		ASCIITable.getInstance().printTable(header, data);
	}

	static void printRoutingTable() {
		String[] header = new String[ROUTING_TABLE_HEADER.length];

		String[][] data = new String[routingTable.size()][ROUTING_TABLE_HEADER.length];

		for (int i = 0; i < ROUTING_TABLE_HEADER.length; i++) {
			header[i] = ROUTING_TABLE_HEADER[i];
		}

		if (routingTable.isEmpty()) {
			System.out.println("Routing Table is Empty");
		} else {
			for (int i = 0; i < routingTable.size(); i++) {
				for (int j = 0; j < ROUTING_TABLE_HEADER.length; j++) {
					if (j == 0) {
						data[i][j] = routingTable.get(i).getId();
					} else if (j == 1) {
						data[i][j] = routingTable.get(i).getIp();
					} else if (j == 2) {
						data[i][j] = Integer.toString(routingTable.get(i).getPort() - 10000);
					}
				}
			}

			ASCIITable.getInstance().printTable(header, data);
		}
	}


	public static void main(String[] args) {
		RegularClientService regularClientService = new RegularClientService();

		Scanner sc = new Scanner(System.in);

		boolean exit = false;

		String remoteIp = args[0];
		int remotePort = Integer.parseInt(args[1]);

		Timer timer = new Timer();

		while (!exit) {
			System.out.println("Regular Client Operations");
			System.out.println("1. Print Routing Table");
			System.out.println("2. Update Routing Table");
			System.out.println("3. Local Lookup");
			System.out.println("4. Remote Lookup");
			System.out.println("5. Slots Distribution Summary");
			System.out.println("6. Get Replica List");
			System.out.println("Select any one");

			int choice = sc.nextInt();

			switch (choice) {
			case 1:
				printRoutingTable();
				break;
			case 2:
				if (routingTable.isEmpty()) {
					timer.start();
					routingTable = regularClientService.updateRoutingTable(remoteIp, remotePort, choice);
					timer.end();

					System.out.println("Time taken to update routing table: " + timer.toString());
				} else {
					Random rand = new Random();
					Node n = routingTable.get(rand.nextInt(routingTable.size()));

					timer.start();
					try
					{
						routingTable = regularClientService.updateRoutingTable(n.getIp(), n.getPort() - 10000, choice);
					}
					catch(Exception e)
					{
						System.out.println("Randomly choosen node maybe dead");
						continue;
					}
					timer.end();

					System.out.println("Time taken to update routing table: " + timer.toString());
				}
				break;
			case 3:
				System.out.print("Enter slots: ");
				sc.nextLine();
				if (routingTable.isEmpty()) {
					routingTable = regularClientService.updateRoutingTable(remoteIp, remotePort, choice);
				}
				summarize();
				printSlots(sc.nextLine());
				break;
			case 4:
				if (routingTable.isEmpty()) {
					System.out.println("Slots table empty. Fetching anyways regardless of version");

					timer.start();
					slotsTable = regularClientService.updateSlots(remoteIp, remotePort);
					timer.end();

					System.out.println("Time taken to update slots table : " + timer.toString());
					
				} else {
					Random rand = new Random();
					Node n = routingTable.get(rand.nextInt(routingTable.size()));
					
					timer.start();
					slotsTable = regularClientService.updateSlots(n.getIp(), n.getPort() - 10000);
					timer.end();
				}
				break;
			case 5:
				// implement summ
				break;
			case 6:
				System.out.println("Enter IP");
				String ipAddress = sc.next();
				//String ipAddress = "10.176.128.111";
				System.out.println("Enter port");
				int port = sc.nextInt();

				timer.start();
				Set<Node> replicaList = regularClientService.getReplicas(ipAddress, port);
				timer.end();

				System.out.println("Time taken to get replica list: " + timer.toString());

				for(Node n : replicaList)
				{
					int tempPort = n.getPort() - 10000;
					System.out.println(n.getId()+" "+n.getIp()+" "+ tempPort);
				}
				break;
			case 7:
				exit = true;
				break;
			}
		}
	}
}
