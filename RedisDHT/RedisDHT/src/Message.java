import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public class Message implements Serializable
{
	private static final long serialVersionUID = 8387950590016941525L;
	
	private String type;
	private String payload;
	private ArrayList<Node> clusterNodes;
	private ConcurrentSkipListMap<Integer, Node> slots;
	private int[] version;
	private Set<Node> replicaList;

	public Message(String type)
	{
		this.type = type;
	}

	public int[] getVersion() {
		return version;
	}

	public void setVersion(int[] version) {
		this.version = version;
	}

	public ArrayList<Node> getClusterNodes() {
		return clusterNodes;
	}

	public void setClusterNodes(ArrayList<Node> clusterNodes) {
		this.clusterNodes = clusterNodes;
	}

	public ConcurrentSkipListMap<Integer, Node> getSlots() {
		return slots;
	}

	public void setSlots(ConcurrentSkipListMap<Integer, Node> slots) {
		this.slots = slots;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getPayload() {
		return payload;
	}

	public void setPayload(String payload) {
		this.payload = payload;
	}

	public String toString() {
		return type + ":" + payload;
	}
	
	public Set<Node> getReplicaList() {
		return replicaList;
	}

	public void setReplicaList(Set<Node> replicaList) {
		this.replicaList = replicaList;
	}
}
