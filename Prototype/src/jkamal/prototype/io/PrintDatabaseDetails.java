/**
 * @author Joarder Kamal
 */

package jkamal.prototype.io;

import java.util.Map.Entry;
import java.util.Set;

import jkamal.prototype.db.*;

public class PrintDatabaseDetails {
	
	Node node;
	Partition partition;
	Data data;
	
	public PrintDatabaseDetails() {}
	
	public void printDetails(DatabaseServer dbs, Database db) {
		// DBS Details
		System.out.println();
		System.out.println("===Database Server Details========================");
		System.out.println(" Database Server: "+dbs.getDbs_name());
		System.out.println(" Number of Nodes: "+dbs.getDbs_node_numbers());
		System.out.println();
		
		// Node Details
		System.out.println("===Node Details========================");
		for(Node node : dbs.getDbs_nodes()) {						
			System.out.print(" "+node.getNode_label());
			System.out.print(" with ");
			System.out.print(node.getNode_capacity()+" Partitions.");
			System.out.print(" Current Load "+((double)node.getNode_capacity()/(double)Node.getNODE_MAX_CAPACITY())*100+"%");
			System.out.println();
		}
		System.out.println();
		
		// DB Details
		System.out.println("===Database Details========================");
		System.out.println(" Database: "+db.getDb_name());
		System.out.println(" Number of Partitions: "+db.getDb_partitions().size());
		System.out.println();
		
		// Partition Table Details
		System.out.println("===Partition Table Details========================");
		int comma = -1;
		for(Entry<Integer, Set<Partition>> entry : db.getDb_partition_table().getPartition_table().entrySet()) {
			System.out.print(" N"+entry.getKey()+"{");
			
			comma = entry.getValue().size();
			for(Partition partition : entry.getValue()) {
				System.out.print(partition.toString());
				
				if(comma != 1)
					System.out.print(", ");
			
				--comma;
			}
			
			System.out.print("}");
			System.out.println();
		}
		System.out.println();
		
		// Partition Details
		/*System.out.println("===Partition Details========================");
		for(Partition partition : db.getDb_partitions()) {
			System.out.println("#Partition: "+partition.getPartition_label());
			System.out.println("Number of Data Items in "+partition.getPartition_label()+": "+partition.getPartition_capacity());
			System.out.println("Partition Capacity Occupied: "+((double)partition.getPartition_capacity()/(double)Partition.getMAX_DATA_ITEMS())*100+"%");
			System.out.println();
		}
		System.out.println();*/
		
		// Routing Table Details
		System.out.println("===Routing Table Details========================");
		System.out.println();
	}
}