/**
 * @author Joarder Kamal
 * 
 * Routing/Look-Up Table will determine which Data resides in which Partition
 */

package jkamal.prototype.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class RoutingTable {
	private Map<Integer, ArrayList<Data>> data_items;
	
	public RoutingTable() {
		setData_items(new HashMap<Integer, ArrayList<Data>>());
	}
	
	// Copy Constructor
	public RoutingTable(RoutingTable routingTable) {
		// Cloning Routing Table
		Map<Integer, ArrayList<Data>> cloneDataItems = new HashMap<Integer, ArrayList<Data>>();
		ArrayList<Data> cloneDataList = new ArrayList<Data>();
		Data cloneData;
		for(Entry<Integer, ArrayList<Data>> entry : routingTable.getData_items().entrySet()) {
			for(Data data : entry.getValue()) {
				cloneData = new Data(data);
				cloneDataList.add(cloneData);
			}
			cloneDataItems.put(entry.getKey(), cloneDataList);
		}
	}
		
	public Map<Integer, ArrayList<Data>> getData_items() {
		return data_items;
	}

	public void setData_items(Map<Integer, ArrayList<Data>> data_items) {
		this.data_items = data_items;
	}
	
	// Returns the Data for the looked-up Data Id. If the Data is Not Found then returns NULL
	public Data getData(Database db, int data_id) {
		for(Entry<Integer, ArrayList<Data>> entry : db.getDb_routing_table().getData_items().entrySet()) {
			for(Data data : entry.getValue()) {
				if(data.getData_id() == data_id) {
					return data;
				}
			}
		}
		
		return null;
	}
	
	// Returns the Partition Id for the looked-up Data
	public int lookup(Database db, int data_id) {
		int partition_id = -1;
		Data data = db.getDb_routing_table().getData(db, data_id);
				
		// if the Data has been moved to (Roaming) another Partition
		if(data.isData_isRoaming())
			partition_id = data.getData_partitionId();
		else
			partition_id = data.getData_homePartitionId();
		
		return partition_id;
	}
}