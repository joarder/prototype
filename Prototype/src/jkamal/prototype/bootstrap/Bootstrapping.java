/**
 * @author Joarder Kamal
 */

package jkamal.prototype.bootstrap;

import java.util.ArrayList;
import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;
import jkamal.prototype.db.DatabaseServer;
import jkamal.prototype.db.GlobalDataMap;
import jkamal.prototype.db.Partition;
import jkamal.prototype.db.PartitionTable;
import jkamal.prototype.db.RoutingTable;

public class Bootstrapping {
	public Bootstrapping() {}
	
	// Synthetic Data Generation 
	public void bootstrapping(DatabaseServer dbs, Database db, int DATA_OBJECTS) {		
		GlobalDataMap globalDataList = db.getDb_dataMap();
		RoutingTable routingTable = db.getDb_routing_table();		
		Partition partition;
		ArrayList<Data> dataList;
		Data data;												
		int node_id = 0;
		int partition_capacity = 0;
		int partition_nums = (int) Math.ceil((double) DATA_OBJECTS/Partition.MAX_ALLOWED_DATA_ITEMS);
		int data_id = 0;
		int data_nums = Partition.MAX_ALLOWED_DATA_ITEMS;		
		
		// i -- partition
		for(int partition_id = 0; partition_id < partition_nums; partition_id++) {	
			if(node_id == dbs.getDbs_nodes().size())
				node_id = 0;
			
			// Create a new Partition and attach it to the Database			
			partition = new Partition(partition_id, String.valueOf(partition_id), node_id);			
			db.getDb_partitions().add(partition);						
			System.out.print("[ACT] Creating Partition "+partition.getPartition_label());			
			
			// Create an ArrayList for placing into the Routing Table for each i-th Partition entry
			dataList = new ArrayList<Data>();																											
			for(int k = 0; k < data_nums && data_id < DATA_OBJECTS; k++) {
				// Create a new Data Item within the Partition
				data = new Data(data_id, String.valueOf(data_id), partition_id, node_id);
				partition.getPartition_data_items().add(data);
				partition_capacity = partition.getPartition_capacity();
				partition.setPartition_capacity(partition_capacity+1);	 // Partition capacity is determined by the number of data items				
				
				// Put Data entry into the Global Data List for generating random data items for Workload generation
				globalDataList.getData_items().put(data_id, data);
								
				dataList.add(data);				
				data_id++;
			} // end -- for
			
			// Put Data entry into the routing table for Look-Up operations
			routingTable.getData_items().put(partition_id, dataList);	
			
			// Adding partition to the Node
			dbs.getDbs_node(node_id).getNode_partitions().add(partition);			
			System.out.print(" and placing it into N"+partition.getPartition_node_id());
			System.out.println();
			
			++node_id;
		} // end -- for				
		
		// Generating Partitioning Table
		PartitionTableGeneration partitionTableGeneration = new PartitionTableGeneration();
		PartitionTable partitionTable = partitionTableGeneration.generatePartitionTable(dbs, db);
		
		// Attaching the Partition Table with the Database
		db.setDb_partition_table(partitionTable);
		// Attach Partition and Routing Tables to the Database
		db.setDb_routing_table(routingTable);
		// Attaching the Global Data Map with the Database
		db.setDb_dataMap(globalDataList);
				
		System.out.println("[MSG] Total Data Items: "+globalDataList.getData_items().size());
		System.out.println("[MSG] Total Partitions: "+routingTable.getData_items().size());
	}
}
