/**
 * @author Joarder Kamal
 */

package jkamal.prototype.bootstrap;

import java.util.Set;
import jkamal.prototype.db.Database;
import jkamal.prototype.db.DatabaseServer;
import jkamal.prototype.db.Partition;
import jkamal.prototype.db.PartitionTable;

public class PartitionTableGeneration {
	public PartitionTableGeneration() {}
		
	public PartitionTable generatePartitionTable(DatabaseServer dbs, Database db) {
		PartitionTable partitionTable = db.getDb_partition_table();
		Set<Partition> partitionSet;
				
		for(int i = 0; i < dbs.getDbs_nodes().size(); i++) {
			partitionSet = dbs.getDbs_node(i).getNode_partitions();			
			partitionTable.getPartition_table().put(i, partitionSet);
		}
		
		return partitionTable;
	}
}