/**
 * @author Joarder Kamal
 */

package jkamal.prototype.db;

import java.util.ArrayList;
import java.util.List;

public class Partition  implements Comparable<Partition> {
	private int partition_id;
	private String partition_label;
	private int partition_capacity;
	private int partition_node_id;	
	private List<Data> partition_data_items;	
	private static int MAX_DATA_ITEMS;
	
	public Partition(int pid, String label, int nid) {
		this.setPartition_id(pid);
		this.setPartition_label("P"+label);
		this.setPartition_capacity(0); // Initially no data items present in a partition. Current partition capacity will be determined by the number of data items it is holding.
		this.setPartition_node_id(nid);
		this.setPartition_data_items(new ArrayList<Data>());
		Partition.setMAX_DATA_ITEMS(1000); // 1GB Data (in Size) Or, equivalently 1000 Data Items can be stored in a single partition.
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

	public static int getMAX_DATA_ITEMS() {
		return MAX_DATA_ITEMS;
	}

	public static void setMAX_DATA_ITEMS(int max) {
		MAX_DATA_ITEMS = max;
	}

	@Override
	public String toString() {
		return (this.partition_label);
	}

	@Override
	public int compareTo(Partition partition) {		
		int compare = ((int)this.partition_id < (int)partition.partition_id) ? -1: ((int)this.partition_id > (int)partition.partition_id) ? 1:0;
		return compare;
	}
}