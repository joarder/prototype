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

import jkamal.prototype.workload.DataPostPartitionTable;
import jkamal.prototype.workload.DataPrePartitionTable;

public class PartitionTable {
	private Map<Integer, Set<Partition>> partition_table;
	private DataPrePartitionTable prePartitionTable;
	private DataPostPartitionTable postPartitionTable;
	
	public PartitionTable() {
		this.setPartition_table(new TreeMap<Integer, Set<Partition>>());
	}
	
	// Copy Constructor
	public PartitionTable(PartitionTable partitionTable) {
		// Cloning Partition Table
		Map<Integer, Set<Partition>> clonePartitionTable = new TreeMap<Integer, Set<Partition>>();
		Set<Partition> clonePartitionSet = new TreeSet<Partition>();
		Partition clonePartition;
		for(Entry<Integer, Set<Partition>> entry : partitionTable.getPartition_table().entrySet()) {
			for(Partition partition : entry.getValue()) {
				clonePartition = new Partition(partition);
				clonePartitionSet.add(clonePartition);
			}
			clonePartitionTable.put(entry.getKey(), clonePartitionSet);
		}
		
		// Cloning Pre/Post-Partition Table
		//this.prePartitionTable = new DataPrePartitionTable(partitionTable.getPrePartitionTable());
		//this.postPartitionTable = new DataPostPartitionTable(partitionTable.getPostPartitionTable());
	}

	public Map<Integer, Set<Partition>> getPartition_table() {
		return partition_table;
	}

	public void setPartition_table(Map<Integer, Set<Partition>> partition_table) {
		this.partition_table = partition_table;
	}
	
	public DataPrePartitionTable getPrePartitionTable() {
		return prePartitionTable;
	}

	public void setPrePartitionTable(DataPrePartitionTable prePartitionTable) {
		this.prePartitionTable = prePartitionTable;
	}

	public DataPostPartitionTable getPostPartitionTable() {
		return postPartitionTable;
	}

	public void setPostPartitionTable(DataPostPartitionTable postPartitionTable) {
		this.postPartitionTable = postPartitionTable;
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
			return partition.getPartition_node_id();
		
		return node_id;
	}
}