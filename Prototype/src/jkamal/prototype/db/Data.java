/**
 * @author Joarder Kamal
 * 
 * The term "Data" has been considered as a row for a Relation DBMS and a single data item for a Key-Value Data Store
 */

package jkamal.prototype.db;

public class Data implements Comparable<Data> {
	private int data_id;
	private String data_label;
	private String data_value;
	private float data_size;
	private int data_weight;
	private boolean data_isMoveable;
	
	// HyperGraph Partitioning Attributes
	private int data_hmetis_cluster_id;
	private int data_shadow_hmetis_id;
	private boolean data_hasShadowHMetisId;
	// Partition Attributes		
	private int data_partition_id;			// Currently residing (Roaming/Home) Partition Id
	private int data_home_partition_id;		// Original Home Partition Id	
	// Node Attributes
	private int data_node_id;				// Currently residing (Roaming/Home) Node Id
	private int data_home_node_id;			// Original Home Node Id
	// Roaming Attributes
	private boolean data_isRoaming;							
	
	// Default Constructor
	public Data(int id, String label, int pid, int nid, boolean roaming) {
		this.setData_id(id); // default data id = -1 means undefined.
		this.setData_label("d"+label);
		this.setData_value("Value:"+this.getData_label());
		this.setData_size(1); // 1.0 = 1 MegaBytes
		this.setData_weight(0);
		this.setData_isMoveable(false);		
				
		this.setData_hmetisClusterId(-1);
		this.setData_shadowHMetisId(-1);
		this.setData_hasShadowHMetisId(false);				
		
		this.setData_partitionId(pid); // default partition id = -1 means undefined.
		this.setData_homePartitionId(pid);
		
		this.setData_nodeId(nid);
		this.setData_homeNodeId(nid);
			
		this.setData_isRoaming(roaming);
	}
	
	// Copy Constructor
	public Data(Data data) {
		this.setData_id(data.getData_id());
		this.setData_label(data.getData_label());
		this.setData_value(data.getData_value());
		this.setData_size(data.getData_size());
		this.setData_weight(data.getData_weight());
		this.setData_isMoveable(data.isData_isMoveable());		
				
		this.setData_hmetisClusterId(data.getData_hmetisClusterId());
		this.setData_shadowHMetisId(data.getData_shadowHMetisId());
		this.setData_hasShadowHMetisId(data.isData_hasShadowHMetisId());
				
		this.setData_partitionId(data.getData_partitionId());				
		this.setData_homePartitionId(data.getData_homePartitionId());
		
		this.setData_nodeId(data.getData_nodeId());
		this.setData_homeNodeId(data.getData_homeNodeId());

		this.setData_isRoaming(data.isData_isRoaming());
	}

	public int getData_id() {
		return data_id;
	}

	public void setData_id(int data_id) {
		this.data_id = data_id;
	}

	public String getData_label() {
		return data_label;
	}

	public void setData_label(String data_label) {
		this.data_label = data_label;
	}

	public String getData_value() {
		return data_value;
	}

	public int getData_weight() {
		return data_weight;
	}

	public void setData_weight(int data_weight) {
		this.data_weight = data_weight;
	}
	
	public void setData_value(String data_value) {
		this.data_value = data_value;
	}

	public float getData_size() {
		return data_size;
	}

	public void setData_size(float data_size) {
		this.data_size = data_size;
	}

	public boolean isData_isRoaming() {
		return data_isRoaming;
	}

	public void setData_isRoaming(boolean data_isRoaming) {
		this.data_isRoaming = data_isRoaming;
	}

	public int getData_partitionId() {
		return data_partition_id;
	}

	public void setData_partitionId(int data_partition_id) {
		this.data_partition_id = data_partition_id;
	}

	public int getData_homePartitionId() {
		return data_home_partition_id;
	}

	public void setData_homePartitionId(int data_home_partition_id) {
		this.data_home_partition_id = data_home_partition_id;
	}

	public int getData_hmetisClusterId() {
		return data_hmetis_cluster_id;
	}

	public void setData_hmetisClusterId(int data_hmetis_cluster_id) {
		this.data_hmetis_cluster_id = data_hmetis_cluster_id;
	}

	public int getData_nodeId() {
		return data_node_id;
	}

	public void setData_nodeId(int data_node_id) {
		this.data_node_id = data_node_id;
	}

	public int getData_homeNodeId() {
		return data_home_node_id;
	}

	public void setData_homeNodeId(int data_home_node_id) {
		this.data_home_node_id = data_home_node_id;
	}

	public int getData_shadowHMetisId() {
		return data_shadow_hmetis_id;
	}

	public void setData_shadowHMetisId(int data_hmetis_id) {
		this.data_shadow_hmetis_id = data_hmetis_id;
	}

	public boolean isData_hasShadowHMetisId() {
		return data_hasShadowHMetisId;
	}

	public void setData_hasShadowHMetisId(boolean data_hasHMetisId) {
		this.data_hasShadowHMetisId = data_hasHMetisId;
	}

	public boolean isData_isMoveable() {
		return data_isMoveable;
	}

	public void setData_isMoveable(boolean data_isMoveable) {
		this.data_isMoveable = data_isMoveable;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Data)) {
			return false;
		}
		
		Data data = (Data) obj;
		  return data_label.equals(data.data_label);
		}

	@Override
	public int hashCode() {
		return data_label.hashCode();
	}

	@Override
	public String toString() {
		//return (this.data_label+"("+this.data_id+")");
		//return (this.data_label+"[N"+this.data_node_id+"]");
		
		if(this.isData_isRoaming())
			return (this.data_label+"[P"+this.data_partition_id+"((H)P"+this.data_home_partition_id+")|N"+this.data_node_id+"((H)N"+this.data_home_node_id+")] @C("+this.data_hmetis_cluster_id+") @h("+this.data_shadow_hmetis_id+")");
		else
			return (this.data_label+"[P"+this.data_partition_id+"|N"+this.data_node_id+"] @C("+this.data_hmetis_cluster_id+") //@h("+this.data_shadow_hmetis_id+")");
		//return (this.data_label+"["+this.data_current_trId+"]");
	}

	@Override
	public int compareTo(Data data) {		
		int compare = ((int)this.data_id < (int)data.data_id) ? -1: ((int)this.data_id > (int)data.data_id) ? 1:0;
		return compare;
	}
}