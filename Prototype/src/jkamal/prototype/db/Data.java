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
	private boolean data_isRoaming;
	// HyperGraph Partitioning Attributes
	private int data_hmetis_cluster_id;
	private int data_shadow_hmetis_id;
	private boolean data_hasShadowHMetisId;
	// Partition Attributes	
	private int data_home_partition_id;
	private int data_partition_id;
	private int data_roaming_partition_id;
	private boolean data_isPartitionRoaming;
	// Node Attributes
	private int data_node_id;
	private int data_roaming_node_id;	
	private boolean data_isNodeRoaming;						
	
	// Default Constructor
	public Data(int id, String label, int pid, int nid) {
		this.setData_id(id); // default data id = -1 means undefined.
		this.setData_label("d"+label);
		this.setData_value("Value:"+this.getData_label());
		this.setData_size(1); // 1.0 = 1 MegaBytes
		this.setData_weight(0);
		this.setData_isMoveable(false);
		this.setData_isRoaming(false);
				
		this.setData_hmetis_cluster_id(-1);
		this.setData_shadow_hmetis_id(-1);
		this.setData_hasShadowHMetisId(false);				

		this.setData_home_partition_id(pid);
		this.setData_partition_id(pid); // default partition id = -1 means undefined.						
		this.setData_roaming_partition_id(-1);
		this.setData_isPartitionRoaming(false);
		
		this.setData_node_id(nid);
		this.setData_roaming_node_id(-1);
		this.setData_isNodeRoaming(false);				
	}
	
	// Copy Constructor
	public Data(Data data) {
		this.setData_id(data.getData_id());
		this.setData_label(data.getData_label());
		this.setData_value(data.getData_value());
		this.setData_size(data.getData_size());
		this.setData_weight(data.getData_weight());
		this.setData_isMoveable(data.isData_isMoveable());
		this.setData_isRoaming(data.isData_isRoaming());
				
		this.setData_hmetis_cluster_id(data.getData_hmetis_cluster_id());
		this.setData_shadow_hmetis_id(data.getData_shadow_hmetis_id());
		this.setData_hasShadowHMetisId(data.isData_hasShadowHMetisId());
		
		this.setData_home_partition_id(data.getData_home_partition_id());
		this.setData_partition_id(data.getData_partition_id());
		this.setData_roaming_partition_id(data.getData_roaming_partition_id());		
		this.setData_isPartitionRoaming(data.isData_isPartitionRoaming());		
		
		this.setData_node_id(data.getData_node_id());
		this.setData_roaming_node_id(data.getData_roaming_node_id());
		this.setData_isNodeRoaming(data.isData_isNodeRoaming());				
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

	public int getData_home_partition_id() {
		return data_home_partition_id;
	}

	public void setData_home_partition_id(int data_home_partition_id) {
		this.data_home_partition_id = data_home_partition_id;
	}

	public int getData_partition_id() {
		return data_partition_id;
	}

	public void setData_partition_id(int data_partition_id) {
		this.data_partition_id = data_partition_id;
	}

	public int getData_hmetis_cluster_id() {
		return data_hmetis_cluster_id;
	}

	public void setData_hmetis_cluster_id(int data_hmetis_cluster_id) {
		this.data_hmetis_cluster_id = data_hmetis_cluster_id;
	}

	public int getData_roaming_partition_id() {
		return data_roaming_partition_id;
	}

	public void setData_roaming_partition_id(int data_roaming_partition_id) {
		this.data_roaming_partition_id = data_roaming_partition_id;
	}

	public int getData_roaming_node_id() {
		return data_roaming_node_id;
	}

	public void setData_roaming_node_id(int data_roaming_node_id) {
		this.data_roaming_node_id = data_roaming_node_id;
	}

	public boolean isData_isPartitionRoaming() {
		return data_isPartitionRoaming;
	}

	public void setData_isPartitionRoaming(boolean data_isPartitionRoaming) {
		this.data_isPartitionRoaming = data_isPartitionRoaming;
	}
	
	public boolean isData_isNodeRoaming() {
		return data_isNodeRoaming;
	}

	public void setData_isNodeRoaming(boolean data_isNodeRoaming) {
		this.data_isNodeRoaming = data_isNodeRoaming;
	}

	public int getData_node_id() {
		return data_node_id;
	}

	public void setData_node_id(int data_node_id) {
		this.data_node_id = data_node_id;
	}

	public int getData_weight() {
		return data_weight;
	}

	public void setData_weight(int data_weight) {
		this.data_weight = data_weight;
	}

	public int getData_shadow_hmetis_id() {
		return data_shadow_hmetis_id;
	}

	public void setData_shadow_hmetis_id(int data_hmetis_id) {
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
	public String toString() {
		//return (this.data_label+"("+this.data_id+")");
		//return (this.data_label+"[N"+this.data_node_id+"]");
		if(this.isData_isPartitionRoaming())
			return (this.data_label+"[P"+this.data_partition_id+"(*P"+this.data_roaming_partition_id+")|N"+this.data_node_id+"]");//-C"+this.data_hmetis_cluster_id);
		else if(this.isData_isPartitionRoaming() && this.isData_isNodeRoaming())
			return (this.data_label+"[P"+this.data_partition_id+"(*P"+this.data_roaming_partition_id+")|N"+this.data_node_id+"(*N"+this.data_roaming_node_id+")]");//-C"+this.data_hmetis_cluster_id);
		else
			return (this.data_label+"[P"+this.data_partition_id+"|N"+this.data_node_id+"]");//-C"+this.data_hmetis_cluster_id);
		//return (this.data_label+"["+this.data_current_trId+"]");
	}

	@Override
	public int compareTo(Data data) {		
		int compare = ((int)this.data_id < (int)data.data_id) ? -1: ((int)this.data_id > (int)data.data_id) ? 1:0;
		return compare;
	}
}