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
	private double partition_percentage_main;
	private double partition_percentage_roaming;
	private double partition_percentage_foreign;
	private double partition_current_load;
	private boolean partition_overloaded;
	
	public Partition(int pid, String label, int nid) {
		this.setPartition_id(pid);
		this.setPartition_label("P"+label);
		this.setPartition_capacity(0); // Initially no data items present in a partition. Current partition capacity will be determined by the number of data items it is holding.
		this.setPartition_node_id(nid);
		this.setPartition_data_items(new ArrayList<Data>());
		this.setRoaming_data_items(new TreeMap<Integer, Integer>());
		this.setForeign_data_items(new ArrayList<Data>());
		this.setPartition_percentage_main(0.0d);
		this.setPartition_percentage_roaming(0.0d);
		this.setPartition_percentage_foreign(0.0d);
		this.setPartition_current_load(0.0d);
		this.setPartition_overloaded(false);
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
		
		this.setPartition_percentage_main(partition.getPartition_percentage_main());
		this.setPartition_percentage_roaming(partition.getPartition_percentage_roaming());
		this.setPartition_percentage_foreign(partition.getPartition_percentage_foreign());
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

	public double getPartition_percentage_main() {
		return partition_percentage_main;
	}

	public void setPartition_percentage_main(double partition_percentage_main) {
		this.partition_percentage_main = partition_percentage_main;
	}

	public double getPartition_percentage_roaming() {
		return partition_percentage_roaming;
	}

	public void setPartition_percentage_roaming(double partition_percentage_roaming) {
		this.partition_percentage_roaming = partition_percentage_roaming;
	}

	public double getPartition_percentage_foreign() {
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
		double percentage = ((double)this.getPartition_data_items().size()/Partition.MAX_DATA_ITEMS)*100.0;
		percentage = Math.round(percentage*100.0)/100.0;
		this.setPartition_percentage_main(percentage);
	}
	
	public void calculateRoamingOccupied() {
		double percentage = ((double)this.getRoaming_data_items().size()/Partition.MAX_DATA_ITEMS)*100.0;
		percentage = Math.round(percentage*100.0)/100.0;
		this.setPartition_percentage_roaming(percentage);
	}

	public void calculateForeignOccupied() {
		double percentage = ((double)this.getForeign_data_items().size()/Partition.MAX_DATA_ITEMS)*100.0;
		percentage = Math.round(percentage*100.0)/100.0;
		this.setPartition_percentage_foreign(percentage);		
	}
	
	public void calculateCurrentLoad() {
		int totalData = this.getPartition_data_items().size()+this.getForeign_data_items().size();
		
		if(totalData >= Partition.MAX_THRESHOLD_DATA_ITEMS)
			this.setPartition_overloaded(true);
		else 
			this.setPartition_overloaded(false);
		
		double percentage = ((double)totalData/Partition.MAX_DATA_ITEMS)*100.0;
		percentage = Math.round(percentage*100.0)/100.0;
		this.setPartition_current_load(percentage);
	}

	@Override
	public String toString() {
		if(this.getRoaming_data_items().size() != 0 || this.getForeign_data_items().size() !=0)
			return (this.getPartition_label()+"("+this.getPartition_current_load()+"%)"
					+"[H*"+this.getPartition_data_items().size()+"("+this.getPartition_percentage_main()+"%)"
					+"|R*"+this.getRoaming_data_items().size()+"("+this.getPartition_percentage_roaming()+"%)"
					+"|F*"+this.getForeign_data_items().size()+"("+this.getPartition_percentage_foreign()+"%)"
					+"]");
		else	
			return (this.getPartition_label()+"("+this.getPartition_current_load()+"%)"
					+"[H*"+this.getPartition_data_items().size()+"]");
	}

	@Override
	public int compareTo(Partition partition) {		
		int compare = ((int)this.partition_id < (int)partition.partition_id) ? -1: ((int)this.partition_id > (int)partition.partition_id) ? 1:0;
		return compare;
	}
}