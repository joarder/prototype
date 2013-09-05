/**
 * @author Joarder Kamal
 */

package jkamal.prototype.io;

import java.util.ArrayList;
import java.util.Map.Entry;
import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;
import jkamal.prototype.workload.DataPostPartitionTable;
import jkamal.prototype.workload.DataPrePartitionTable;

public class PrintPartitioningDetails {

	public PrintPartitioningDetails() {
		
	}
	
	public void printDetails(Database db) {
		int comma = -1;
		
		// Pre-Partitioning Details
		System.out.print("\n===Data Pre-partitioning Details========================\n");		
		
		DataPrePartitionTable dataPrePartitionTable = db.getDb_partition_table().getDataPrePartitionTable();
		for(Entry<Integer, ArrayList<Data>> entry : dataPrePartitionTable.getDataPrePartitionTable().entrySet()) {
			System.out.print("P"+entry.getKey()+"["+entry.getValue().size()+"]: {");			
			
			comma = entry.getValue().size();
			for(Data data : entry.getValue()) {
				System.out.print(data.toString());
				
				if(comma != 1)
					System.out.print(", ");
			
				--comma;						
			} // end -- for() loop
			
			System.out.print("}\n");
		} // end -- for() loop
		System.out.print("\n");
		
		// Post-Partitioning Details
		System.out.print("\n===Data Post-partitioning Details========================\n");		
		
		DataPostPartitionTable dataPostPartitionTable = db.getDb_partition_table().getDataPostPartitionTable();
		for(Entry<Integer, ArrayList<Data>> entry : dataPostPartitionTable.getDataPostPartitionTable().entrySet()) {
			System.out.print("P"+entry.getKey()+"["+entry.getValue().size()+"]: {");			
			
			comma = entry.getValue().size();
			for(Data data : entry.getValue()) {
				System.out.print(data.toString());
				
				if(comma != 1)
					System.out.print(", ");
			
				--comma;						
			} // end -- for() loop
			
			System.out.print("}\n");
		} // end -- for() loop
		System.out.print("\n");
	}
}