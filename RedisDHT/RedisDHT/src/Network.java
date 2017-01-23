import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

//TODO
/*
 * locks logic
 * update come first the  delete
 * vice versa
 */

//listen to control client on normal port
//use +10000 for gossip protocol
public class Network implements Runnable {
	private InetSocketAddress address; // my address
	private InetSocketAddress redisAddress; // my address
	private Logger logger;
	private Selector clientServerSelector;
	private ServerSocketChannel clientServerSocket;
	private Selector redisServerSelector;
	private Selector redisClientSelector;
	private ServerSocketChannel redisServerSocket;
	private static final int MESSAGE_SIZE = 4096000;
	private Master m;
	private LinkedList<Message> clientQueue = new LinkedList<>();
	private BlockingQueue<Message> redisQueue = new LinkedBlockingQueue<>();
	private String myIp;
	private int offset = 1;
	private Object dieLock = new Object();
	private Object slotsLock = new Object();
	private Object addLock = new Object();
	private Object deleteLock = new Object();

	public Network(Master m, String address, Logger logger) {
		this.m = m;
		this.logger = logger;

		String[] splits = address.split(":");

		try {
			this.address = new InetSocketAddress(InetAddress.getByName(splits[0]), Integer.parseInt(splits[1]));
			this.redisAddress = new InetSocketAddress(InetAddress.getByName(splits[0]),
					Integer.parseInt(splits[1]) + 10000);
		} catch (UnknownHostException e) {
			logger.log(Level.INFO, e.getMessage(), e);
		}

		myIp = splits[0];
		try {
			clientServerSelector = Selector.open();
			redisServerSelector = Selector.open();
		} catch (Exception e) {
			logger.log(Level.INFO, e.getMessage(), e);
		}

		Node myself = new Node(m.getId(), redisAddress.getPort(), myIp, false);
		m.setMyself(myself);
	}

	public ServerSocketChannel getClientServerSocketChannel() {
		return clientServerSocket;
	}

	public ServerSocketChannel getRedisServerSocketChannel() {
		return redisServerSocket;
	}

	class RedisServer implements Runnable
	{
		public void run()
		{
			while (true)
			{
				try
				{
					if (redisServerSelector.select() <= 0)
					{
						continue;
					}

					processReadySet(redisServerSelector.selectedKeys());
				}
				catch (Exception ioe)
				{
					logger.log(Level.INFO, ioe.getMessage(), ioe);
				}
			}
		}

		public void processReadySet(Set readySet)
		{
			Iterator iterator = readySet.iterator();

			while (iterator.hasNext())
			{
				SelectionKey key = (SelectionKey) iterator.next();
				iterator.remove();

				if (key.isAcceptable())
				{
					ServerSocketChannel ssChannel = (ServerSocketChannel) key.channel();
					SocketChannel clientChannel;
					try
					{
						clientChannel = (SocketChannel) ssChannel.accept();
						clientChannel.configureBlocking(false);
						clientChannel.register(key.selector(), SelectionKey.OP_READ);
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
				}

				if (key.isReadable())
				{
					Message msg = processRead(key);

					if (msg != null)
					{
						logger.info("message received " + msg.toString());

						// modify data structures..our client will write to
						// redis neighbor nodes
						// processing add me message
						// Ex: addme:uuid:ip:port

						String[] splits = null;

						if (msg.getPayload() != null)
							splits = msg.getPayload().split(":", -1);

						if (msg.getType().equals("addme") && splits != null)
						{
							logger.info("Received a message of add me type");
							// splits[0] uuid, splits[1] ip, splits[2] port
							// add about a to master.clusterNodes and put reply
							// processing in redisQueue

							String dummyConnStr = splits[0] + ":" + splits[1] + ":" + splits[2];

							logger.info("Constructing dummy addnode");

							Message dummyConnMsg = new Message("dummyaddnode");
							dummyConnMsg.setPayload(dummyConnStr);

							redisQueue.add(dummyConnMsg);

							// add to redis queue the nieghbor list as 2nd
							// msg...in redis queue processing thread handle
							// neighbor message
						}
						else if (msg.getType().equals("welcome"))
						{
							ArrayList<Node> temp = msg.getClusterNodes();
							logger.info("List of nodes received size : " + temp.size());
							logger.info("List of nodes received : " + temp);

							logger.info("putting welcome msg in redis queue");
							redisQueue.add(msg);
						}
						else if (msg.getType().equals("hi"))
						{
							logger.info("Received a hi message");
							redisQueue.add(msg);
						}
						else if (msg.getType().equals("kill"))
						{
							// shut myself down gracefully...close all channels,
							// shutdown CS and RS
							// offload your keys to other nodes

							Message dieMsg = new Message("iamdead");
							dieMsg.setPayload(m.getId());

							synchronized (dieLock)
							{
								try
								{
									redisQueue.add(dieMsg);

									logger.info("Waiting till die message is sent to one random node");

									dieLock.wait();
								}
								catch (InterruptedException e)
								{
									e.printStackTrace();
								}
							}

							// can't call system.exit(0) directly. Need to wait
							// on a lock and need to be notified by
							// redisconsumer after it has sent out a message to
							// a random neighbor that I'm dying.
							System.exit(0);
							
						}
						else if (msg.getType().equals("update"))
						{
							// remove the uuid received from cluster node
							logger.info("Adding dummyupdate for an update");
							msg.setType("dummyupdate");
							redisQueue.add(msg);
						}
						else if (msg.getType().equals("broadcast_slots"))
						{
							msg.setType("broadcast_slots_dummy");
							redisQueue.add(msg);
						}
						else if (msg.getType().equals("iamdead"))
						{
							msg.setType("iamdead_dummy");
							redisQueue.add(msg);
						}
					}
				}
			}
		}

		public Message processRead(SelectionKey key)
		{
			SocketChannel clientChannel = (SocketChannel) key.channel();
			ByteBuffer buffer = ByteBuffer.allocate(MESSAGE_SIZE);
			Message msg = null;
			int bytesRead = 0;

			try {
				bytesRead = clientChannel.read(buffer);

				if (bytesRead > 0) {
					msg = extractMessage(buffer);
				}
			} catch (Exception e) {
				logger.log(Level.INFO, e.getMessage(), e);
			}

			return msg;
		}
	}

	private Message extractMessage(ByteBuffer buffer)
	{
		ByteArrayInputStream bais = new ByteArrayInputStream(buffer.array());
		ObjectInputStream ois;
		Message msg = null;

		try {
			ois = new ObjectInputStream(bais);

			try {
				Object readObject = ois.readObject();

				if (readObject instanceof Message) {
					msg = (Message) readObject;
				}
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}

		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		return msg;
	}

	class ClientServer implements Runnable
	{
		public void run()
		{
			while (true)
			{
				try
				{
					if (clientServerSelector.select() <= 0)
					{
						continue;
					}

					processReadySet(clientServerSelector.selectedKeys());
				}
				catch (Exception ioe)
				{
					logger.log(Level.INFO, ioe.getMessage(), ioe);
				}
			}
		}

		private void loadBalance(int start, int end, String toUUID)
		{

			Node destination = null;
			for (Node n : m.getClusterNodes())
			{
				if (n.getId().equals(toUUID))
				{
					destination = n;
					break;
				}
			}

			ConcurrentSkipListMap<Integer, Node> mySlots = m.getSlots();
			int[] versionArray = m.getVersion();

			for (int i = start; i <= end; i++) {
				mySlots.put(i, destination);
				versionArray[i]++;
			}
		}

		private void loadBalance(int[] tSlots, String toUUID)
		{

			Node destination = null;
			for (Node n : m.getClusterNodes())
			{
				if (n.getId().equals(toUUID))
				{
					destination = n;
					break;
				}
			}
			
			ConcurrentSkipListMap<Integer, Node> mySlots = m.getSlots();
			int[] versionArray = m.getVersion();

			for (int i = 0; i < tSlots.length; i++)
			{
				mySlots.put(tSlots[i], destination);
				versionArray[tSlots[i]]++;
			}
		}

		public void processReadySet(Set readySet) {
			Iterator iterator = readySet.iterator();

			while (iterator.hasNext())
			{
				SelectionKey key = (SelectionKey) iterator.next();
				iterator.remove();

				if (key.isAcceptable())
				{
					SocketChannel clientChannel;
					try {
						clientChannel = clientServerSocket.accept();
						clientChannel.configureBlocking(false);
						clientChannel.register(key.selector(), SelectionKey.OP_READ);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				if (key.isReadable())
				{
					Message msg = processRead(key);

					if (msg != null)
					{
						logger.info(m.getId() + " " + msg);
						logger.info("Message Received: " + msg.toString());
						// Message processing code to be handled here

						String[] splits = null;

						if (msg.getPayload() != null)
							splits = msg.getPayload().split(":", -1);

						if (msg.getType().equals("addnode") && splits == null)
						{
							clientQueue.add(msg);
							logger.info("Size before: " + clientQueue.size());

							SocketChannel clientChannel = (SocketChannel) key.channel();

							try
							{
								clientChannel.register(key.selector(), SelectionKey.OP_WRITE);
							}
							catch (ClosedChannelException e1)
							{
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
						}
						else if (msg.getType().equals("addnode") && splits.length == 3)
						{
							logger.info("Added " + msg + " to redis queue");

							synchronized (addLock)
							{
								try
								{
									redisQueue.add(msg);

									logger.info("Waiting till hi message sent out to welcome list");

									addLock.wait();
								}
								catch (InterruptedException e)
								{
									e.printStackTrace();
								}
							}

							clientQueue.add(msg);

							SocketChannel clientChannel = (SocketChannel) key.channel();

							try
							{
								clientChannel.register(key.selector(), SelectionKey.OP_WRITE);
							}
							catch (ClosedChannelException e1)
							{
								e1.printStackTrace();
							}
						}
						else if (msg.getType().equals("delete") && splits.length == 3)
						{
							// handle
							// remove node
							// synch block on list
							// add to redis queue

							// uuid, ip, port

							synchronized (deleteLock)
							{
								try
								{
									logger.info("Adding kill to redis queue");
									Message killMsg = new Message("kill");
									killMsg.setPayload(splits[0]);
									redisQueue.add(killMsg);

									logger.info("Adding update to redis queue");
									// adding update message to queue
									Message updateMsg = new Message("update");
									updateMsg.setPayload(splits[0]);
									redisQueue.add(updateMsg);

									logger.info("Waiting till update and kill message sent out");

									deleteLock.wait();
								}
								catch (InterruptedException e)
								{
									e.printStackTrace();
								}
							}
						}
						else if (msg.getType().equals("addslots"))
						{
							String payload = msg.getPayload();

							ConcurrentSkipListMap<Integer, Node> slots = m.getSlots();
							Node value = null;
							int[] version = m.getVersion();

							boolean failure = false;

							if (payload.contains("-")) {
								// range
								String[] range = payload.split("-");
								int start = Integer.parseInt(range[0]);
								int end = Integer.parseInt(range[1]);

								for (int i = start; i <= end; i++) {
									value = slots.get(i);

									if (version[i] > 0 && !value.getId().equals("")) {
										// put error message in client queue
										Message error = new Message("failure");
										error.setPayload("Slot " + i + " is not owned by me");
										clientQueue.add(error);
										failure = true;
										break;
									}
								}

								if (!failure) {
									synchronized (slotsLock) {
										for (int i = start; i <= end; i++) {
											m.addSlot(i);
										}
									}

									Message success = new Message("success");
									success.setPayload("Successfully Added Slot");
									clientQueue.add(success);
								}
							} else {
								String[] spSlots = payload.split(" ");
								int[] tSlots = new int[spSlots.length];

								for (int i = 0; i < spSlots.length; i++) {
									tSlots[i] = Integer.parseInt(spSlots[i]);
								}

								for (int i = 0; i < tSlots.length; i++) {
									value = slots.get(tSlots[i]);

									if (version[tSlots[i]] > 0 && !value.getId().equals("")) {
										// put error message in client queue
										Message error = new Message("failure");
										error.setPayload("Slot " + tSlots[i] + " is not owned by me");
										clientQueue.add(error);
										failure = true;
										break;
									}
								}

								if (!failure) {
									synchronized (slotsLock) {
										for (int i = 0; i < tSlots.length; i++) {
											m.addSlot(tSlots[i]);
										}
									}

									Message success = new Message("success");
									success.setPayload("Successfully Added Slot");
									clientQueue.add(success);
								}
							}

							if (!failure) {
								Message broadcast_slots = new Message("broadcast_slots");
								redisQueue.add(broadcast_slots);

							}

							SocketChannel clientChannel = (SocketChannel) key.channel();

							try {
								clientChannel.register(key.selector(), SelectionKey.OP_WRITE);
							} catch (ClosedChannelException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
						} else if (msg.getType().equals("deleteslots")) {
							String payload = msg.getPayload();

							ConcurrentSkipListMap<Integer, Node> slots = m.getSlots();
							Node value = null;
							int[] version = m.getVersion();

							boolean failure = false;

							if (payload.contains("-")) {
								// range
								String[] range = payload.split("-");
								int start = Integer.parseInt(range[0]);
								int end = Integer.parseInt(range[1]);

								for (int i = start; i <= end; i++) {
									value = slots.get(i);

									if (version[i] == 0 || !slots.get(i).getId().equals(m.getId())) {
										// put error message in client queue
										Message error = new Message("failure");
										error.setPayload("Slot " + i + " is not owned by me");
										clientQueue.add(error);
										failure = true;
										break;
									}
								}

								if (!failure) {
									synchronized (slotsLock) {
										for (int i = start; i <= end; i++) {
											m.deleteSlot(i);
										}
									}

									Message success = new Message("success");
									success.setPayload("Successfully Deleted Slot");
									clientQueue.add(success);
								}
							} else {
								String[] spSlots = payload.split(" ");
								int[] tSlots = new int[spSlots.length];

								for (int i = 0; i < spSlots.length; i++) {
									tSlots[i] = Integer.parseInt(spSlots[i]);
								}

								for (int i = 0; i < tSlots.length; i++) {
									value = slots.get(tSlots[i]);

									if (version[tSlots[i]] == 0 || !value.getId().equals(m.getId())) {
										// put error message in client queue
										Message error = new Message("failure");
										error.setPayload("Slot " + tSlots[i] + " is not owned by me");
										clientQueue.add(error);
										failure = true;
										break;
									}
								}

								if (!failure) {
									synchronized (slotsLock) {
										for (int i = 0; i < tSlots.length; i++) {
											m.deleteSlot(tSlots[i]);
										}
									}

									Message success = new Message("success");
									success.setPayload("Successfully Deleted Slot");
									clientQueue.add(success);
								}
							}

							if (!failure) {
								Message broadcast_slots = new Message("broadcast_slots");
								redisQueue.add(broadcast_slots);
							}

							SocketChannel clientChannel = (SocketChannel) key.channel();

							try {
								clientChannel.register(key.selector(), SelectionKey.OP_WRITE);
							} catch (ClosedChannelException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
						}
						else if (msg.getType().equals("loadbalance"))
						{
							String slotToMigrate = splits[1];
							String toUUID = splits[0];

							ConcurrentSkipListMap<Integer, Node> slots = m.getSlots();
							Node value = null;
							int[] version = m.getVersion();

							boolean failure = false;

							if (slotToMigrate.contains("-")) {
								// range
								String[] range = slotToMigrate.split("-");
								int start = Integer.parseInt(range[0]);
								int end = Integer.parseInt(range[1]);

								for (int i = start; i <= end; i++) {
									value = slots.get(i);

									if (version[i] == 0 || !slots.get(i).getId().equals(m.getId())) {
										// put error message in client queue
										Message error = new Message("failure");
										error.setPayload(
												"Slot " + i + " is not owned by me. Can't perform load balancing.");
										clientQueue.add(error);
										failure = true;
										break;
									}
								}

								if (!failure) {
									synchronized (slotsLock) {
										loadBalance(start, end, toUUID);
									}
									Message success = new Message("success");
									success.setPayload("Successfully Issued load balancing");
									clientQueue.add(success);
								}
							} else {
								String[] spSlots = slotToMigrate.split(" ");
								int[] tSlots = new int[spSlots.length];

								for (int i = 0; i < spSlots.length; i++) {
									tSlots[i] = Integer.parseInt(spSlots[i]);
								}

								for (int i = 0; i < tSlots.length; i++) {
									value = slots.get(tSlots[i]);

									if (version[tSlots[i]] == 0 || !value.getId().equals(m.getId())) {
										// put error message in client queue
										Message error = new Message("failure");
										error.setPayload(
												"Slot " + tSlots[i] + " is not owned by me.  Can't perform load balancing.");
										clientQueue.add(error);
										failure = true;
										break;
									}
								}

								if (!failure) {
									synchronized (slotsLock) {
										loadBalance(tSlots, toUUID);
									}
									Message success = new Message("success");
									success.setPayload("Successfully Issued load balancing");
									clientQueue.add(success);
								}
							}

							if (!failure)
							{
								logger.info("Load balancing done. Broadcasting slots to other nodes");
								msg.setType("broadcast_slots");
								redisQueue.add(msg);
							}

							SocketChannel clientChannel = (SocketChannel) key.channel();

							try {
								clientChannel.register(key.selector(), SelectionKey.OP_WRITE);
							} catch (ClosedChannelException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
						} else if (msg.getType().equals("fetchroutingtable")) {
							ArrayList<Node> duplicateClusterNodes = (ArrayList<Node>) m.getClusterNodes().clone();
							duplicateClusterNodes.add(m.getMyself());

							msg.setClusterNodes(duplicateClusterNodes);

							clientQueue.add(msg);

							SocketChannel clientChannel = (SocketChannel) key.channel();

							try {
								clientChannel.register(key.selector(), SelectionKey.OP_WRITE);
							} catch (ClosedChannelException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
						} else if (msg.getType().equals("fetchslots")) {
							synchronized (slotsLock) {
								logger.info("Making shallow copy of slots and version.");
								ConcurrentSkipListMap<Integer, Node> duplicateSlots = m.getSlots().clone();
								int[] duplicateVersion = m.getVersion().clone();
								
								msg.setSlots(duplicateSlots);
								msg.setVersion(duplicateVersion);
							}

							clientQueue.add(msg);

							SocketChannel clientChannel = (SocketChannel) key.channel();

							try {
								clientChannel.register(key.selector(), SelectionKey.OP_WRITE);
							} catch (ClosedChannelException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
						} else if(msg.getType().equals("fetchreplicas")) {
							logger.info("Message received of fetchreplicas type");
							//TODO check if sync needed here
							msg.setReplicaList(m.getReplicaList());

							clientQueue.add(msg);

							SocketChannel clientChannel = (SocketChannel) key.channel();

							try {
								clientChannel.register(key.selector(), SelectionKey.OP_WRITE);
							} catch (ClosedChannelException e1) {
								// TODO Auto-generated catch block
								e1.printStackTrace();
							}
						}
					}
				}

				if (key.isWritable()) {
					if (!clientQueue.isEmpty()) {
						Message msg = clientQueue.removeFirst();

						if (msg.getType().equals("addnode")) {
							logger.info("Message Received of addnode type");

							SocketChannel client = (SocketChannel) key.channel();

							String s = m.getId();

							try {
								logger.info("Writing response");
								Message uuidMsg = new Message("uuid");
								uuidMsg.setPayload(s);

								ByteArrayOutputStream baos = new ByteArrayOutputStream();
								ObjectOutputStream oos = new ObjectOutputStream(baos);

								oos.writeObject(uuidMsg);
								oos.flush();
								
								byte[] message = baos.toByteArray();

								ByteBuffer buffer = ByteBuffer.wrap(message);
								client.write(buffer);

								client.close();
							} catch (IOException e) {
								e.printStackTrace();
							}
						} else {
							SocketChannel client = (SocketChannel) key.channel();

							try {

								ByteArrayOutputStream baos = new ByteArrayOutputStream();
								ObjectOutputStream oos = new ObjectOutputStream(baos);

								logger.info("Message type: " + msg.getType());
								oos.writeObject(msg);
								oos.flush();
								byte[] message = baos.toByteArray();

								ByteBuffer buffer = ByteBuffer.wrap(message);
								client.write(buffer);

								client.close();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
				}

			}
		}

		public Message processRead(SelectionKey key) {
			SocketChannel clientChannel = (SocketChannel) key.channel();
			ByteBuffer buffer = ByteBuffer.allocate(MESSAGE_SIZE);
			Message msg = null;
			int bytesRead = 0;

			try {
				bytesRead = clientChannel.read(buffer);

				if (bytesRead > 0) {
					msg = extractMessage(buffer);
				}
			} catch (Exception e) {
				logger.log(Level.INFO, e.getMessage(), e);
			}

			return msg;
		}
	}

	class RedisQueueConsumer implements Runnable {
		Selector clientSelector;

		private void merge(Message msg) {
			ConcurrentSkipListMap<Integer, Node> remoteSlots = msg.getSlots();
			int[] remoteVersion = msg.getVersion();

			int size = remoteSlots.size();

			ConcurrentSkipListMap<Integer, Node> mySlots = m.getSlots();
			int[] myVersion = m.getVersion();

			for (int i = 0; i < size; i++) {
				if (remoteVersion[i] > myVersion[i]) {
					mySlots.put(i, remoteSlots.get(i));
					myVersion[i] = remoteVersion[i];
				} else if (remoteVersion[i] == myVersion[i] && myVersion[i] > 0) {
					// break ties using uuid
					if (mySlots.get(i).getId().compareTo(remoteSlots.get(i).getId()) < 0) {
						mySlots.put(i, remoteSlots.get(i));
						myVersion[i] = remoteVersion[i];
					}
				}
			}

			Iterator it = mySlots.entrySet().iterator();

			logger.info("Printing my merged slots");

			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry) it.next();
				logger.info(pair.getKey() + " = " + pair.getValue().toString());
			}

		}

		/*
		 * id represent the id of the node to be reassigned
		 */
		private boolean reassignSlots(String id) {
			boolean modified = false;

			// calculating total number of nodes...initialzed to 1 to include
			// myself in the count
			int totalNodes = 1;

			for (Node n : m.getClusterNodes()) {
				// ignoring the deleted node
				if (!n.getId().equals(id)) {
					totalNodes++;
				}
			}

			logger.info("Total Nodes: " + totalNodes);

			HashMap<Integer, Node> tempMap = new HashMap<>();

			Iterator it = m.getSlots().entrySet().iterator();

			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry) it.next();

				Node n = (Node) pair.getValue();
				int slot = (int) pair.getKey();
				if (n.getId().equals(id)) {
					tempMap.put(slot, n);
				}
			}

			if (tempMap.size() > 0) {
				modified = true;
			}

			logger.info("temp map size : " + tempMap.size());

			int splitSize = tempMap.size() / totalNodes;

			logger.info("splitSize : " + splitSize);

			if(splitSize == 0)
			{
				int index = 0;
				logger.info("Number of nodes greater than slots held by this node");
				Iterator tempIt = tempMap.entrySet().iterator();
				while (tempIt.hasNext())
				{
					Map.Entry pair = (Map.Entry) tempIt.next();
					int key = (int) pair.getKey();
					
					tempMap.put(key, m.getClusterNodes().get(index++));
				}
			}
			else
			{
				logger.info("Number of slots held greater than number of nodes");
				int index = 0;
				Node assign = null;

				int count = 0;

				for (Node n : m.getClusterNodes()) {
					logger.info("Node : " + n.getPort());
				}

				logger.info("Myself : " + m.getMyself().getPort());

				Iterator tempIt = tempMap.entrySet().iterator();
				while (tempIt.hasNext()) {
					Map.Entry pair = (Map.Entry) tempIt.next();
					int key = (int) pair.getKey();

					if (count % splitSize == 0 && index < m.getClusterNodes().size()) {
						if (count == 0) {
							assign = m.getMyself();
						} else {
							assign = m.getClusterNodes().get(index++);
							// ignoring the deleted node
							if (assign.getId().equals(id)) {
								assign = m.getClusterNodes().get(index++);
							}
						}
					}

					tempMap.put(key, assign);

					count++;
				}
			}

			Iterator tempIt = tempMap.entrySet().iterator();
			// merge tempMap and hashSlots
			ConcurrentSkipListMap<Integer, Node> mySlots = m.getSlots();
			int[] myVersion = m.getVersion();

			while (tempIt.hasNext()) {
				Map.Entry pair = (Map.Entry) tempIt.next();
				int key = (int) pair.getKey();
				Node value = (Node) pair.getValue();

				myVersion[key]++;
				mySlots.put(key, value);
			}

			logger.info("Printing reassigned slots");
			it = mySlots.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry) it.next();
				logger.info(pair.getKey() + " = " + pair.getValue().toString());
			}

			return modified;
		}

		private boolean clientProcessReadySet(Set readySet) {
			Iterator iterator = readySet.iterator();

			while (iterator.hasNext()) {
				SelectionKey key = (SelectionKey) iterator.next();
				iterator.remove();
				if (key.isConnectable()) {
					processConnect(key);
					return true;
				}
			}

			return false;
		}

		private boolean processConnect(SelectionKey key) {
			SocketChannel channel = (SocketChannel) key.channel();
			while (channel.isConnectionPending()) {
				try {
					channel.finishConnect();
				} catch (Exception e) {
					logger.log(Level.INFO, e.getMessage(), e);
				}
			}
			return true;
		}

		public SocketChannel connectToRemoteRedisMaster(Node n) {
			try {
				clientSelector = Selector.open();
			} catch (Exception e1) {
				logger.log(Level.INFO, e1.getMessage(), e1);
			}

			SocketChannel channel = null;
			int operations = SelectionKey.OP_CONNECT;

			try {
				channel = SocketChannel.open();
				channel.configureBlocking(false);
				logger.info("Tryign to bind client");
				channel.bind(new InetSocketAddress(redisAddress.getPort() + offset++));
				channel.connect(n.getNeighborAddress());

				channel.register(clientSelector, operations);

				n.setSocketChannel(channel);
			} catch (Exception e) {
				// e.printStackTrace();
				logger.log(Level.INFO, e.getMessage(), e);
			}

			boolean connected = false;
			while (true) {
				try {
					if (clientSelector.select() > 0) {
						connected = clientProcessReadySet(clientSelector.selectedKeys());

						if (connected) {
							break;
						}
					}
				} catch (Exception e) {
					logger.log(Level.INFO, e.getMessage(), e);
				}
			}

			return channel;
		}

		private void sendMsg(SocketChannel channel, Message msg) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos;
			try {
				oos = new ObjectOutputStream(baos);
				oos.writeObject(msg);
				oos.flush();
				
				byte[] buf = baos.toByteArray();
				ByteBuffer buffer = ByteBuffer.wrap(buf);

				channel.write(buffer);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void run() {
			while (true) {
				try {
					Message msg = redisQueue.take();

					String[] splits = null;

					if (msg.getPayload() != null)
						splits = msg.getPayload().split(":", -1);

					if (msg.getType().equals("addnode") && splits != null && splits.length == 3) {
						// id, ip, port
						// add +10000 to port...since the addnode mesage
						// contains clientserver port address
						Node n = new Node(splits[0], Integer.parseInt(splits[2]) + 10000, splits[1], true);

						SocketChannel channel = connectToRemoteRedisMaster(n);

						m.addNode(n);

						// send addme message to oldNode
						logger.info("Sending addme to oldNode");

						Message addme = new Message("addme");

						// using address.getPort() instead of
						// redisAddress.getPort() because doing +10000 when
						// processing addnode
						addme.setPayload(m.getId() + ":" + myIp + ":" + address.getPort());

						sendMsg(channel, addme);
					} else if (msg.getType().equals("dummyaddnode")) {
						// id, ip, port
						// add +10000 to port...since the addnode mesage
						// contains clientserver port address
						Node n = new Node(splits[0], Integer.parseInt(splits[2]) + 10000, splits[1], true);

						SocketChannel channel = connectToRemoteRedisMaster(n);

						Message welcomeMsg = new Message("welcome");
						
						synchronized (slotsLock) {
							ConcurrentSkipListMap<Integer, Node> duplicateSlots = m.getSlots().clone();
							int[] duplicateVersion = m.getVersion().clone();
							ArrayList<Node> duplicateClusterNodes = (ArrayList<Node>) m.getClusterNodes().clone();
							
							welcomeMsg.setClusterNodes(duplicateClusterNodes);
							welcomeMsg.setSlots(duplicateSlots);
							welcomeMsg.setVersion(duplicateVersion);
						}

						logger.info("Sending welcome message from redis queue");

						// sending first and the adding to avoid the receiver to
						// connect to itself
						sendMsg(channel, welcomeMsg);

						m.addReplica(n);

						m.addNode(n);
					} else if (msg.getType().equals("welcome")) {
						// connect to all nodes in the neighbor list
						// send Hi to all with uuid...The receiver will check if
						// he has this uuid as a neighbor in clusterNodes. If he
						// doesn't put a addnode to redisqueue.
						ArrayList<Node> remoteNeighborList = msg.getClusterNodes();

						for (Node n : remoteNeighborList) {
							SocketChannel channel = connectToRemoteRedisMaster(n);
							n.setSocketChannel(channel);
							m.addNode(n);

							Message hiMsg = new Message("hi");
							hiMsg.setPayload(m.getId() + ":" + myIp + ":" + redisAddress.getPort());
							hiMsg.setClusterNodes(m.getClusterNodes());

							logger.info("Sending hi message from redis consumer");

							sendMsg(channel, hiMsg);

							//here
							m.addReplica(n);
						}

						logger.info("Merging my empty slots with remote slots");

						merge(msg);

						logger.info("Notifying addLock on sending out hi");
						synchronized (addLock)
						{
							addLock.notify();
						}
					}
					else if (msg.getType().equals("hi"))
					{
						Node n = new Node(splits[0], Integer.parseInt(splits[2]), splits[1], true);
						SocketChannel channel = connectToRemoteRedisMaster(n);
						n.setSocketChannel(channel);
						m.addNode(n);

						m.addReplica(n);
					}
					else if (msg.getType().equals("kill"))
					{
						String uuid = msg.getPayload().split(":", -1)[0];
						// send kill for a node
						for (Node n : m.getClusterNodes())
						{
							if (n.getId().equals(uuid)) {
								SocketChannel channel = n.getSocketChannel();
								sendMsg(channel, msg);
								m.removeNode(uuid);
								break;
							}
						}

						logger.info("latest node list");
						for (Node n : m.getClusterNodes()) {
							logger.info(n.toString());
						}

						// close channel to the one you sent kill
					} else if (msg.getType().equals("update")) {
						for (Node n : m.getClusterNodes()) {
							SocketChannel channel = n.getSocketChannel();
							sendMsg(channel, msg);
						}

						logger.info("Sent out kill and update messages. Now can return control to control client");
						synchronized (deleteLock) {
							deleteLock.notify();
						}

					}
					else if (msg.getType().equals("dummyupdate"))
					{
						String uuid = msg.getPayload().split(":", -1)[0];
						// send kill for a node
						for (Node n : m.getClusterNodes())
						{
							if (n.getId().equals(uuid)) {
								SocketChannel channel = n.getSocketChannel();
								try {
									logger.info("Closing channel");
									channel.close();
								} catch (IOException e) {
									e.printStackTrace();
								}

								m.removeNode(uuid);
								break;
							}
						}

						//check if uuid present in replicaList
						Node deletedReplica = null;

						for (Node n : m.getReplicaList())
						{
							if(n.getId().equals(uuid))
							{
								deletedReplica = n;
								break;
							}
						}

						if(deletedReplica != null)
						{
							logger.info("Deleting replica " + deletedReplica.getId());
							m.removeReplica(deletedReplica);
						}


						Set<Node> set = m.getReplicaList();
						List<Node> candidates = new ArrayList<>();

						for(Node n : m.getClusterNodes())
						{
							if(!set.contains(n))
							{
								candidates.add(n);
							}
						}

						Random rand = new Random();
						int idx = rand.nextInt(candidates.size());
						m.addReplica(candidates.get(idx));

						logger.info("Added new replica: " + candidates.get(idx).getId());

						logger.info("latest node list");
						for (Node n : m.getClusterNodes()) {
							logger.info(n.toString());
						}
					}
					else if (msg.getType().equals("broadcast_slots"))
					{
						synchronized (slotsLock) {
							Message mySlots = new Message("broadcast_slots");
							mySlots.setSlots(m.getSlots());
							mySlots.setVersion(m.getVersion());
							//id of the node updating slots
							mySlots.setPayload(m.getId());

							for (Node n : m.getClusterNodes()) {
								SocketChannel channel = n.getSocketChannel();
								sendMsg(channel, mySlots);
							}
						}
					}
					else if(msg.getType().equals("broadcast_slots_dummy"))
					{
						synchronized (slotsLock) {
							logger.info("merging remote slots with my slots");
							merge(msg);
						}
					}
					else if (msg.getType().equals("iamdead")) {
						Random rand = new Random();
						int randomNum = rand.nextInt(m.getClusterNodes().size());
						Node n = m.getClusterNodes().get(randomNum);

						logger.info("Sent iamdead message to " + n.getId());
						SocketChannel channel = n.getSocketChannel();
						sendMsg(channel, msg);

						synchronized (dieLock) {
							dieLock.notify();
						}
					} else if (msg.getType().equals("iamdead_dummy")) {
						synchronized (slotsLock) {
							String id = msg.getPayload();
							logger.info(id + " is dead. Reassigning its hashslots");

							if (reassignSlots(id)) {
								Message mySlots = new Message("broadcast_slots");
								mySlots.setSlots(m.getSlots());
								mySlots.setVersion(m.getVersion());
								//id of the node updating slots
								mySlots.setPayload(m.getId());

								for (Node n : m.getClusterNodes()) {
									SocketChannel channel = n.getSocketChannel();
									sendMsg(channel, mySlots);
								}
							}
						}
					} 
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	public void run() {
		logger.info("Starting Networking");

		try {
			clientServerSocket = ServerSocketChannel.open();
			clientServerSocket.bind(this.address);
			clientServerSocket.configureBlocking(false);
			int ops = clientServerSocket.validOps();
			clientServerSocket.register(clientServerSelector, ops, null);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			redisServerSocket = ServerSocketChannel.open();
			redisServerSocket.bind(this.redisAddress);
			redisServerSocket.configureBlocking(false);
			int ops = redisServerSocket.validOps();
			redisServerSocket.register(redisServerSelector, ops, null);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// starting client server thread
		Thread clientServerThread = new Thread(new ClientServer());
		clientServerThread.start();

		// starting client server thread
		Thread redisServerThread = new Thread(new RedisServer());
		redisServerThread.start();

		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			logger.log(Level.INFO, e.getMessage(), e);
		}

		// start a thread to read from redisQueue
		Thread redisQueueThread = new Thread(new RedisQueueConsumer());
		redisQueueThread.start();

		try {
			clientServerThread.join();
		} catch (InterruptedException e) {
			logger.log(Level.INFO, e.getMessage(), e);
		}

		try {
			redisServerThread.join();
		} catch (InterruptedException e) {
			logger.log(Level.INFO, e.getMessage(), e);
		}
	}
}
