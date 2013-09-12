/**
 * @author Joarder Kamal
 */

package jkamal.prototype.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class Partition implements Comparable<Partition> {
	private int partition_id;
	private String partition_label;
	private int partition_capacity;
	private int partition_node_id;	
	private List<Data> partition_data_items;
	private Map<Integer, Integer> roaming_data_items;
	private List<Data> foreign_data_items;
	public final static int MAX_DATA_ITEMS = 1000; // 1GB Data (in Size) Or, equivalently 1000 Data Items can be stored in a single partition.
	public final static int MAX_ALLOWED_DATA_ITEMS = (int)(1000*0.7); // 70% of the Partition is allowed to be filled up.
	public final static int MAX_FOREIGN_DATA_ITEMS = (int)(1000*0.2); // 20% of the Partition is allowed to be filled up with Foreign Data Items.
	public final static int MAX_THRESHOLD_DATA_ITEMS = 900;
	
	public Partition(int pid, String label, int nid) {
		this.setPartition_id(pid);
		this.setPartition_label("P"+label);
		this.setPartition_capacity(0); // Initially no data items present in a partition. Current partition capacity will be determined by the number of data items it is holding.
		this.setPartition_node_id(nid);
		this.setPartition_data_items(new ArrayList<Data>());
		this.setRoaming_data_items(new TreeMap<Integer, Integer>());
		this.setForeign_data_items(new ArrayList<Data>());
	}	

	// Copy Constructor
	public Partition(Partition partition) {
		this.partition_id = partition.getPartition_id();
		this.partition_label = partition.getPartition_label();
		this.partition_capacity = partition.getPartition_capacity();
		this.partition_node_id = partition.getPartition_node_id();
		
		List<Data> clonePartitionDataItems = new ArrayList<Data>();
		Data cloneData;
		for(Data data : partition.getPartition_data_items()) {
			cloneData = new Data(data);
			clonePartitionDataItems.add(cloneData);
		}
		this.partition_data_items = clonePartitionDataItems;
		
		Map<Integer, Integer> cloneRoamingDataItems = new TreeMap<Integer, Integer>();
		for(Entry<Integer, Integer> entry : partition.getRoaming_data_items().entrySet()) {
			cloneRoamingDataItems.put(entry.getKey(), entry.getValue());
		}
		this.roaming_data_items = cloneRoamingDataItems;
		
		List<Data> cloneForeignDataItems = new ArrayList<Data>();
		Data cloneForeignData;
		for(Data data : partition.getForeign_data_items()) {
			cloneForeignData = new Data(data);
			cloneForeignDataItems.add(cloneForeignData);
		}
		this.foreign_data_items = cloneForeignDataItems;
	}
	
	public int getPartition_id() {
		return partition_id;
	}

	public void setPartition_id(int partition_id) {
		this.partition_id = partition_id;
	}

	public String getPartition_label() {
		return partition_label;
	}

	public void setPartition_label(String partition_label) {
		this.partition_label = partition_label;
	}

	public int getPartition_capacity() {
		return partition_capacity;
	}

	public void setPartition_capacity(int partition_capacity) {
		this.partition_capacity = partition_capacity;
	}

	public int getPartition_node_id() {
		return partition_node_id;
	}

	public void setPartition_node_id(int partition_node_id) {
		this.partition_node_id = partition_node_id;
	}

	public List<Data> getPartition_data_items() {
		return partition_data_items;
	}

	public void setPartition_data_items(List<Data> partition_data_items) {
		this.partition_data_items = partition_data_items;
	}

	public Map<Integer, Integer> getRoaming_data_items() {
		return roaming_data_items;
	}

	public void setRoaming_data_items(Map<Integer, Integer> roaming_data_items) {
		this.roaming_data_items = roaming_data_items;
	}

	public List<Data> getForeign_data_items() {
		return foreign_data_items;
	}

	public void setForeign_data_items(List<Data> foreign_data_items) {
		this.foreign_data_items = foreign_data_items;
	}

	@Override
	public String toString() {
		if(this.getRoaming_data_items().size() != 0 || this.getForeign_data_items().size() !=0)
			return (this.partition_label+"["+this.getPartition_data_items().size()
					+"|R*"+this.getRoaming_data_items().size()
					+"|F*"+this.getForeign_data_items().size()
					+"]");
		else	
			return (this.partition_label+"["+this.partition_data_items.size()+"]");
	}

	@Override
	public int compareTo(Partition partition) {		
		int compare = ((int)this.partition_id < (int)partition.partition_id) ? -1: ((int)this.partition_id > (int)partition.partition_id) ? 1:0;
		return compare;
	}
}