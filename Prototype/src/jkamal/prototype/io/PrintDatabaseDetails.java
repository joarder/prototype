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
		System.out.print("\n===Database Server Details========================");
		System.out.print("\n Database Server: "+dbs.getDbs_name());
		System.out.print("\n Number of Nodes: "+dbs.getDbs_node_numbers());
		
		// Node Details
		System.out.println();
		System.out.print("\n===Node Details========================");
		for(Node node : dbs.getDbs_nodes()) {						
			System.out.print("\n "+node.getNode_label()
			+" with "
			+node.getNode_capacity()+" Partitions."
			+" Current Load "
			+((double)node.getNode_capacity()/(double)Node.getNODE_MAX_CAPACITY())*100+"%");			
		}
		System.out.print("\n");
		
		// DB Details
		System.out.print("\n===Database Details========================");
		System.out.print("\n Database: "+db.getDb_name());
		System.out.print("\n Number of Partitions: "+db.getDb_partitions().size());
		
		// Partition Table Details
		System.out.println();
		System.out.print("\n===Partition Table Details========================");
		int comma = -1;
		for(Entry<Integer, Set<Partition>> entry : db.getDb_partition_table().getPartition_table().entrySet()) {
			System.out.print("\n N"+entry.getKey()+"{");
			
			comma = entry.getValue().size();
			for(Partition partition : entry.getValue()) {
				System.out.print(partition.toString());
				
				if(comma != 1)
					System.out.print(", ");
			
				--comma;
			}
			
			System.out.print("}");			
		}
		
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
		System.out.print("\n===Routing Table Details========================\n");		
	}
}