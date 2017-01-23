import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public class Master
{
	private String id;
	ConcurrentSkipListMap<Integer, Node> slots;
	int[] version;
	ArrayList<Node> clusterNodes;
	Node myself;

	Set<Node> replicaList = new HashSet<>();

	final static int SLOT_SIZE = 100;

	final static int REPLICA_SIZE = 3;

	public Master(String id)
	{
		this.id = id;
		clusterNodes = new ArrayList<>();

		Node n = new Node();

		this.slots = new ConcurrentSkipListMap<>();

		for(int i = 0; i < SLOT_SIZE; i++)
		{
			this.slots.put(i, n);
		}

		version = new int[SLOT_SIZE];
	}

	public String getId()
	{
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void addNode(Node n)
	{
		clusterNodes.add(n);
	}

	public ConcurrentSkipListMap<Integer, Node> getSlots()
	{
		return slots;
	}

	public void addSlot(int slot)
	{
		slots.put(slot, myself);
		version[slot]++;
	}

	public void deleteSlot(int slot)
	{
		slots.put(slot, new Node());
		version[slot]++;	
	}	

	public void setMyself(Node myself)
	{
		this.myself = myself;
	}

	public Node getMyself()
	{
		return myself;
	}

	public void removeNode(String uuid)
	{
		for(int i = 0; i < clusterNodes.size(); i++)
		{
			Node n = clusterNodes.get(i);

			if(n.getId().equals(uuid))
			{
				clusterNodes.remove(i);
				break;
			}
		}
	}

	public ArrayList<Node> getClusterNodes()
	{
		return clusterNodes;
	}

	public int[] getVersion() {
		return version;
	}

	public void setVersion(int[] version) {
		this.version = version;
	}

	public void addReplica(Node n)
	{
		if(replicaList.size() < REPLICA_SIZE)
		{
			replicaList.add(n);
		}
	}

	public void removeReplica(Node n)
	{
		replicaList.remove(n);
	}

	public Set<Node> getReplicaList() {
		return replicaList;
	}

	public void setReplicaList(Set<Node> replicaList) {
		this.replicaList = replicaList;
	}
}
