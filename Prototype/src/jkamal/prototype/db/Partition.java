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
	private int partition_size;	// 1000 means 1GB is the defined partition size
	private int partition_size_normal; //  partition_size * 0.8 = 800 means 800MB is the partition size in the normal operational margin
	private int partition_size_overloaded; // partition_size * 0.9 = 900 means 900MB is the partition size in the overloading margin
	private List<Data> partition_data_objects;
	private Map<Integer, Integer> roaming_data_objects;
	private List<Integer> foreign_data_objects;
	private double partition_percentage_main;
	private double partition_percentage_roaming;
	private double partition_percentage_foreign;
	private double partition_current_load;
	private boolean partition_overloaded;
	
	public Partition(int pid, String label, int nid, int psize) {
		this.setPartition_id(pid);
		this.setPartition_label("P"+label);
		this.setPartition_capacity(0); // Initially no data items present in a partition. Current partition capacity will be determined by the number of data items it is holding.
		this.setPartition_node_id(nid);
		this.setPartition_size(psize);
		this.setPartition_size_normal((int)(this.getPartition_size() * 0.8));
		this.setPartition_size_overloaded((int)(this.getPartition_size() * 0.9));
		this.setPartition_dataObjects(new ArrayList<Data>());
		this.setRoaming_dataObjects(new TreeMap<Integer, Integer>());
		this.setForeign_dataObjects(new ArrayList<Integer>());
		this.setPartition_percentage_main(0.0d);
		this.setPartition_percentage_roaming(0.0d);
		this.setPartition_current_load(0.0d);
		this.setPartition_overloaded(false);
	}	

	// Copy Constructor
	public Partition(Partition partition) {
		this.setPartition_id(partition.getPartition_id());
		this.setPartition_label(partition.getPartition_label());
		this.setPartition_capacity(partition.getPartition_capacity());
		this.setPartition_node_id(partition.getPartition_nodeId());
		this.setPartition_size(partition.getPartition_size());
		this.setPartition_size_normal(partition.getPartition_size_normal());
		this.setPartition_size_overloaded(partition.getPartition_size_overloaded());
		
		List<Data> clonePartitionDataItems = new ArrayList<Data>();
		Data cloneData;
		for(Data data : partition.getPartition_dataObjects()) {
			cloneData = new Data(data);
			clonePartitionDataItems.add(cloneData);
		}
		this.partition_data_objects = clonePartitionDataItems;
		
		Map<Integer, Integer> cloneRoamingDataItems = new TreeMap<Integer, Integer>();
		for(Entry<Integer, Integer> entry : partition.getRoaming_dataObjects().entrySet()) {
			cloneRoamingDataItems.put(entry.getKey(), entry.getValue());
		}
		this.roaming_data_objects = cloneRoamingDataItems;
		
		List<Integer> cloneForeignDataItems = new ArrayList<Integer>();
		for(Integer data_id : partition.getForeign_dataObjects()) {			
			cloneForeignDataItems.add(data_id);
		}
		this.foreign_data_objects = cloneForeignDataItems;
		
		this.setPartition_percentage_main(partition.getPartition_percentageMain());
		this.setPartition_percentage_roaming(partition.getPartition_percentageRoaming());
		this.setPartition_current_load(partition.getPartition_current_load());
		this.setPartition_overloaded(partition.isPartition_overloaded());
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

	public int getPartition_nodeId() {
		return partition_node_id;
	}

	public void setPartition_node_id(int partition_node_id) {
		this.partition_node_id = partition_node_id;
	}

	public int getPartition_size() {
		return partition_size;
	}

	public void setPartition_size(int partition_size) {
		this.partition_size = partition_size;
	}

	public int getPartition_size_normal() {
		return partition_size_normal;
	}

	public void setPartition_size_normal(int partition_size_normal) {
		this.partition_size_normal = partition_size_normal;
	}

	public int getPartition_size_overloaded() {
		return partition_size_overloaded;
	}

	public void setPartition_size_overloaded(int partition_size_overloaded) {
		this.partition_size_overloaded = partition_size_overloaded;
	}

	public List<Data> getPartition_dataObjects() {
		return partition_data_objects;
	}

	public void setPartition_dataObjects(List<Data> partition_data_objects) {
		this.partition_data_objects = partition_data_objects;
	}

	public Map<Integer, Integer> getRoaming_dataObjects() {
		return roaming_data_objects;
	}

	public void setRoaming_dataObjects(Map<Integer, Integer> roaming_data_objects) {
		this.roaming_data_objects = roaming_data_objects;
	}

	public List<Integer> getForeign_dataObjects() {
		return foreign_data_objects;
	}

	public void setForeign_dataObjects(List<Integer> foreign_data_items) {
		this.foreign_data_objects = foreign_data_items;
	}

	public double getPartition_percentageMain() {
		return partition_percentage_main;
	}

	public void setPartition_percentage_main(double partition_percentage_main) {
		this.partition_percentage_main = partition_percentage_main;
	}

	public double getPartition_percentageRoaming() {
		return partition_percentage_roaming;
	}

	public void setPartition_percentage_roaming(double partition_percentage_roaming) {
		this.partition_percentage_roaming = partition_percentage_roaming;
	}

	public double getPartition_percentageForeign() {
		return partition_percentage_foreign;
	}

	public void setPartition_percentage_foreign(double partition_percentage_foreign) {
		this.partition_percentage_foreign = partition_percentage_foreign;
	}
	
	public double getPartition_current_load() {
		return partition_current_load;
	}

	public void setPartition_current_load(double partition_current_load) {
		this.partition_current_load = partition_current_load;
	}

	public boolean isPartition_overloaded() {
		return partition_overloaded;
	}

	public void setPartition_overloaded(boolean partition_overloaded) {
		this.partition_overloaded = partition_overloaded;
	}

	public void calculateMainOccupied() {
		double percentage = ((double)this.getPartition_dataObjects().size()/this.getPartition_size())*100.0;
		percentage = Math.round(percentage*100.0)/100.0;
		this.setPartition_percentage_main(percentage);
	}
	
	public void calculateRoamingOccupied() {
		double percentage = ((double)this.getRoaming_dataObjects().size()/this.getPartition_size())*100.0;
		percentage = Math.round(percentage*100.0)/100.0;
		this.setPartition_percentage_roaming(percentage);
	}

	public void calculateForeignOccupied() {
		double percentage = ((double)this.getForeign_dataObjects().size()/this.getPartition_size())*100.0;
		percentage = Math.round(percentage*100.0)/100.0;
		this.setPartition_percentage_foreign(percentage);		
	}
	
	public void calculateCurrentLoad() {
		int totalData = this.getPartition_dataObjects().size();
		
		if(totalData > this.getPartition_size_overloaded())
			this.setPartition_overloaded(true);
		else 
			this.setPartition_overloaded(false);
		
		double percentage = ((double)totalData/this.getPartition_size()) * 100.0;
		percentage = Math.round(percentage * 100.0) / 100.0;
		this.setPartition_current_load(percentage);
	}
	
	public void printContents() {
		int comma = this.getPartition_dataObjects().size();
		System.out.print("{");
		for(Data data : this.getPartition_dataObjects()) {
			System.out.print(data.toString());
			
			if(comma != 1)
				System.out.print(", ");
			
			--comma;
		}
		System.out.println("}");
	}

	@Override
	public String toString() {
		if(this.getRoaming_dataObjects().size() != 0 || this.getForeign_dataObjects().size() !=0)
			return (this.getPartition_label()+"[Capacity: "+this.getPartition_size()+"|Load: "+this.getPartition_current_load()+"%]"
					+" >> [H("+this.getPartition_dataObjects().size()+")|"+this.getPartition_percentageMain()+"%, "					
					+"F("+this.getForeign_dataObjects().size()+")|"+this.getPartition_percentageForeign()+"%], "
					+"[R("+this.getRoaming_dataObjects().size()+")|"+this.getPartition_percentageRoaming()+"%)"
					+"]");
		else	
			return (this.getPartition_label()+"[Capacity: "+getPartition_size()+"|Load: "+this.getPartition_current_load()+"%]"
					+" >> [H("+this.getPartition_dataObjects().size()+")]");
	}

	@Override
	public int compareTo(Partition partition) {		
		int compare = ((int)this.partition_id < (int)partition.partition_id) ? -1: ((int)this.partition_id > (int)partition.partition_id) ? 1:0;
		return compare;
	}
}