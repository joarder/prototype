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
	
	public void bootstrapping(DatabaseServer dbs, Database db, int DATA_OBJECTS) {
		// Synthetic Data Generation 
		GlobalDataMap dataMap = db.getDb_dataMap();
		RoutingTable routingTable = db.getDb_routing_table();
		
		Partition partition;
		Data data;
		ArrayList<Data> dataList;
		
		System.out.println();
		System.out.println(">> Generating "+ DATA_OBJECTS +" synthetic data items ...");				
		
		// j -- node
		int j = 0;
		int node_capacity = 0;
		int partition_capacity = 0;
		int id = 0;
		
		// i -- partition
		for(int i=0; i<(double)DATA_OBJECTS/(double)Partition.getMAX_DATA_ITEMS(); i++) {						
			// Create a new Partition within the Database			
			partition = new Partition(i, String.valueOf(i), 0);
			partition.setPartition_node_id(j);
			db.getDb_partitions().add(partition);			
			
			System.out.print(" Creating Partition "+partition.getPartition_label());			
			
			// Create an ArrayList for placing into the Routing Table for each i-th Partition entry
			dataList = new ArrayList<Data>();
			
			if(j == dbs.getDbs_node_numbers())
				j = 0;																					
			
			for(int k = 0; k < Partition.getMAX_DATA_ITEMS() && id < DATA_OBJECTS; k++) {
				// Create a new Data Item within the Partition
				data = new Data(id, String.valueOf(id), i, j);
				partition.getPartition_data_items().add(data);
				partition_capacity = partition.getPartition_capacity();
				partition.setPartition_capacity(partition_capacity+1);	 // Partition capacity is determined by the number of data items				
				
				// Put Data entry into the Global Data Map for generating random data items for Workload generation
				dataMap.getData_items().put(id, data);
				
				// Put Data entry into the routing table for Look-Up operations
				dataList.add(data);
				routingTable.getData_items().put(i, dataList);
				id++;
			} // end -- for
						
			// Setting Node capacity -- Node capacity is determined by the number of partitions			
			node_capacity = dbs.getDbs_nodes().get(j).getNode_capacity();
			dbs.getDbs_nodes().get(j).setNode_capacity(node_capacity+1);
			
			// Adding partition to the Node
			dbs.getDbs_nodes().get(j).getNode_partitions().add(partition);
			
			System.out.print(" and placing it into N"+partition.getPartition_node_id());
			System.out.println();
			
			++j;
		} // end -- for				
		
		// Generating Partitioning Table
		PartitionTableGeneration partitionTableGeneration = new PartitionTableGeneration();
		PartitionTable partitionTable = partitionTableGeneration.generatePartitionTable(dbs, db);
		
		// Attaching the Partition Table with the Database
		db.setDb_partition_table(partitionTable);
		// Attach Partition and Routing Tables to the Database
		db.setDb_routing_table(routingTable);
		// Attaching the Global Data Map with the Database
		db.setDb_dataMap(dataMap);
		
		System.out.println();
		System.out.println(">> Total Data Items: "+dataMap.getData_items().size());
		System.out.println(">> Total Partitions: "+routingTable.getData_items().size());
	}
}
