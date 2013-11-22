/**
 * @author Joarder Kamal
 */

package jkamal.prototype.bootstrap;

import java.util.ArrayList;
import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;
import jkamal.prototype.db.DatabaseServer;
import jkamal.prototype.db.Partition;

public class Bootstrapping {
	public Bootstrapping() {}
	
	public void rangePartitioning() {}
	public void hashPartitioning() {}
	public void saltPartitioning() {}
	public void consistentHashPartitioning() {}	
	
	// Synthetic Data Generation
	// Options: Range, Salting, Hash (Random), Consistent-Hash (Random)
	public void bootstrapping(DatabaseServer dbs, Database db, int DATA_OBJECTS) {		
		Partition partition;
		ArrayList<Data> dataList;
		Data data;												
		int node_id = 1; //1
		int partition_capacity = 0;
		int partition_nums = (int) Math.ceil((double) DATA_OBJECTS/(db.getDb_partition_size() * 0.8));
		int data_id = 1; //0
		int data_nums = 0;
		int global_data = 0;
		
		// i -- partition
		for(int partition_id = 1; partition_id <= partition_nums; partition_id++) {	
			if(node_id > dbs.getDbs_nodes().size())
				node_id = 1;
			
			// Create a new Partition and attach it to the Database			
			partition = new Partition(partition_id, String.valueOf(partition_id), node_id, db.getDb_partition_size());			
			db.getDb_partitions().add(partition);
			data_nums = (int) ((int)(partition.getPartition_capacity())*0.8);						
			
			System.out.print("[ACT] Creating Partition "+partition.getPartition_label());			
			
			// Create an ArrayList for placing into the Routing Table for each i-th Partition entry
			dataList = new ArrayList<Data>();																											
			for(int k = 1; k <= data_nums && data_id <= DATA_OBJECTS; k++) {
				// Create a new Data Item within the Partition
				data = new Data(data_id, String.valueOf(data_id), partition_id, node_id, false);
				partition.getPartition_dataSet().add(data);
				partition_capacity = partition.getPartition_capacity();
				partition.setPartition_capacity(partition_capacity+1);	 // Partition capacity is determined by the number of data items				
				
				// Put an entry into the Partition Data lookup table
				partition.getPartition_dataLookupTable().put(data.getData_id(), partition.getPartition_id());
								
				dataList.add(data);				
				++data_id;
				++global_data;
			} // end -- for
			
			System.out.print(" with "+dataList.size()+" Data objects");
			
			// Calculate current load
			partition.getCurrentLoad();
			
			// Adding partition to the Node
			dbs.getDbs_node(node_id).getNode_partitions().add(partition);			

			System.out.print(" and placing it into node N"+partition.getPartition_nodeId());
			System.out.println();
			
			++node_id;
		} // end -- for						
				
		System.out.println("[MSG] Total Data Items: "+global_data);
		System.out.println("[MSG] Total Partitions: "+db.getDb_partitions().size());
	}
}
