/**
 * @author Joarder Kamal
 * 
 * Partition Table determines which Partition resides in which Node (Physical Machine)
 */

package jkamal.prototype.db;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.TreeSet;

public class PartitionTable {
	private Map<Integer, Set<Partition>> partition_table;
	
	public PartitionTable() {
		this.setPartition_table(new TreeMap<Integer, Set<Partition>>());
	}
	
	// Copy Constructor
	public PartitionTable(PartitionTable partitionTable) {
		// Cloning Partition Table
		Map<Integer, Set<Partition>> clonePartitionTable = new TreeMap<Integer, Set<Partition>>();
		Set<Partition> clonePartitionSet;
		Partition clonePartition;
		for(Entry<Integer, Set<Partition>> entry : partitionTable.getPartition_table().entrySet()) {
			clonePartitionSet = new TreeSet<Partition>();
			for(Partition partition : entry.getValue()) {
				clonePartition = new Partition(partition);
				clonePartitionSet.add(clonePartition);
			}
			clonePartitionTable.put(entry.getKey(), clonePartitionSet);
		}
		this.setPartition_table(clonePartitionTable);
	}

	public Map<Integer, Set<Partition>> getPartition_table() {
		return partition_table;
	}

	public void setPartition_table(Map<Integer, Set<Partition>> partition_table) {
		this.partition_table = partition_table;
	}

	// Returns the Partition for the looked-up Partition Id. If the Partition is Not Found then returns NULL
	public Partition getPartition(int partition_id) {		
		for(Entry<Integer, Set<Partition>> entry : this.getPartition_table().entrySet()) {
			for(Partition partition : entry.getValue()) {
				if(partition.getPartition_id() == partition_id)
					return partition;
			}		
		}
		
		return null;
	}
	
	// Returns the Node Id for the looked-up Partition
	public int lookup(int partition_id) {
		int node_id = -1;
		Partition partition = this.getPartition(partition_id);
		
		if(partition.getPartition_id() == partition_id)
			return partition.getPartition_nodeId();
		
		return node_id;
	}
	
	public void print() {
		System.out.print("\n===Partition Table Details========================\n");
		int comma = -1;
		for(Entry<Integer, Set<Partition>> entry : this.getPartition_table().entrySet()) {
			System.out.print(" N"+entry.getKey()+"{");
			
			comma = entry.getValue().size();
			for(Partition partition : entry.getValue()) {
				System.out.print(partition.toString());
				
				if(comma != 1)
					System.out.print(", ");
			
				--comma;
			}
			
			System.out.print("}\n");			
		}
	}
}