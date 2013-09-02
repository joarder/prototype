/**
 * @author Joarder Kamal
 * 
 * The term "Data" has been considered as a row for a Relation DBMS and a single data item for a Key-Value Data Store
 */

package jkamal.prototype.db;

public class Data implements Comparable<Data> {
	private int data_id;
	private String data_label;
	private float data_size;
	
	private int data_partition_id;	
	private int data_hmetis_partition_id;
	private int data_prototype_partition_id;
	
	private int data_roaming_partition_id;
	private boolean data_isRoaming;
	
	private int data_node_id;
	private int data_weight;
	private String data_current_trId;
	
	private int data_shadow_hmetis_id;
	private boolean data_hasShadowHMetisId;
	
	private boolean data_isMoveable;
	
	public Data(int id, String label, int pid, int nid) {
		this.setData_id(id); // default data id = -1 means undefined.
		this.setData_label("d"+label);
		this.setData_size(1); // 1.0 = 1 MegaBytes
		
		this.setData_partition_id(pid); // default partition id = -1 means undefined.
		this.setData_hmetis_partition_id(-1);
		this.setData_prototype_partition_id(-1);
		
		this.setData_roaming_partition_id(-1);
		this.setData_isRoaming(false);
		
		this.setData_node_id(nid);
		this.setData_weight(0);
		this.setData_current_trId("");
		
		this.setData_shadow_hmetis_id(-1);
		this.setData_hasShadowHMetisId(false);
		
		this.setData_isMoveable(false);
	}
	
	// Copy Constructor
	public Data(Data data) {
		data_id = data.getData_id();
		data_label = data.getData_label();
		data_size = data.getData_size();
		
		data_partition_id = data.getData_partition_id();
		data_hmetis_partition_id = data.getData_hmetis_partition_id();
		data_prototype_partition_id = data.getData_prototype_partition_id();
		
		data_roaming_partition_id = data.getData_roaming_partition_id();
		data_isRoaming = data.isData_isRoaming();
		
		data_node_id = data.getData_node_id();
		data_weight = data.getData_weight();
		data_current_trId = data.getData_current_trId();
		
		data_shadow_hmetis_id = data.getData_shadow_hmetis_id();
		data_hasShadowHMetisId = data.isData_hasShadowHMetisId();		
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

	public float getData_size() {
		return data_size;
	}

	public void setData_size(float data_size) {
		this.data_size = data_size;
	}

	public int getData_partition_id() {
		return data_partition_id;
	}

	public void setData_partition_id(int data_partition_id) {
		this.data_partition_id = data_partition_id;
	}

	public int getData_hmetis_partition_id() {
		return data_hmetis_partition_id;
	}

	public void setData_hmetis_partition_id(int data_hmetis_partition_id) {
		this.data_hmetis_partition_id = data_hmetis_partition_id;
	}

	public int getData_prototype_partition_id() {
		return data_prototype_partition_id;
	}

	public void setData_prototype_partition_id(int data_prototype_partition_id) {
		this.data_prototype_partition_id = data_prototype_partition_id;
	}

	public int getData_roaming_partition_id() {
		return data_roaming_partition_id;
	}

	public void setData_roaming_partition_id(int data_roaming_partition_id) {
		this.data_roaming_partition_id = data_roaming_partition_id;
	}

	public boolean isData_isRoaming() {
		return data_isRoaming;
	}

	public void setData_isRoaming(boolean data_isRoaming) {
		this.data_isRoaming = data_isRoaming;
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

	public String getData_current_trId() {
		return data_current_trId;
	}

	public void setData_current_trId(String data_current_trId) {
		this.data_current_trId = data_current_trId;
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
		if(this.isData_isRoaming())
			return (this.data_label+"[P"+this.data_partition_id+"("+this.data_roaming_partition_id+")|N"+this.data_node_id+"]");
		else
			return (this.data_label+"[P"+this.data_partition_id+"|N"+this.data_node_id+"]");
		//return (this.data_label+"["+this.data_current_trId+"]");
	}

	@Override
	public int compareTo(Data data) {		
		int compare = ((int)this.data_id < (int)data.data_id) ? -1: ((int)this.data_id > (int)data.data_id) ? 1:0;
		return compare;
	}
}