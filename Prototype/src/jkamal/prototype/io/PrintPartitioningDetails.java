/**
 * @author Joarder Kamal
 */

package jkamal.prototype.io;

import java.util.ArrayList;
import java.util.Map.Entry;
import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;

public class PrintPartitioningDetails {

	public PrintPartitioningDetails() {
		
	}
	
	public void printDetails(Database db) {
		int comma = -1;
		
		// Pre-Partitioning Details
		System.out.println();
		System.out.println("===Data Pre-partitioning Details========================");		
		
		for(Entry<Integer, ArrayList<Data>> entry : db.getDb_partition_table().getPrePartitionTable().getPrePartitionTable().entrySet()) {
			System.out.print("P"+entry.getKey()+"["+entry.getValue().size()+"]: {");			
			
			comma = entry.getValue().size();
			for(Data data : entry.getValue()) {
				System.out.print(data.toString());
				
				if(comma != 1)
					System.out.print(", ");
			
				--comma;						
			} // end -- for() loop
			
			System.out.println("}");
		} // end -- for() loop
		System.out.println();
		
		// Post-Partitioning Details
		System.out.println();
		System.out.println("===Data Post-partitioning Details========================");		
		
		for(Entry<Integer, ArrayList<Data>> entry : db.getDb_partition_table().getPostPartitionTable().getPostPartitionTable().entrySet()) {
			System.out.print("P"+entry.getKey()+"["+entry.getValue().size()+"]: {");			
			
			comma = entry.getValue().size();
			for(Data data : entry.getValue()) {
				System.out.print(data.toString());
				
				if(comma != 1)
					System.out.print(", ");
			
				--comma;						
			} // end -- for() loop
			
			System.out.println("}");
		} // end -- for() loop
		System.out.println();
	}
}