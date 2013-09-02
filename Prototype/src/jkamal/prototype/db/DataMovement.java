/**
 * @author Joarder Kamal
 * 
 * Perform Data Movement after analysing Workload using HyperGraph Partitioning 
 */

package jkamal.prototype.db;

import java.util.Iterator;
import jkamal.prototype.transaction.TransactionDataSet;
import jkamal.prototype.util.Matrix;
import jkamal.prototype.workload.Workload;

public class DataMovement {
	public DataMovement() {}
	
	public void move(Database db, Workload workload, Matrix M) {
		TransactionDataSet transactionDataSet = workload.getWrl_transactionDataSet();
		int original_id = -1;
		int hmetis_id = -1;
		Data data;
		Data roaming_data;
		Partition partition; 
		
		Iterator<Data> iterator = transactionDataSet.getTransactionDataSet().iterator();
		while(iterator.hasNext()) {
			data = iterator.next();
			original_id = data.getData_partition_id();
			hmetis_id = data.getData_hmetis_partition_id();
			
			if(original_id != hmetis_id && !data.isData_isRoaming()) {				
				data.setData_roaming_partition_id(hmetis_id);
				data.setData_isRoaming(true);												
				
				roaming_data = new Data(data); // Calling Copy Constructor
				
				partition = db.getDb_partition_table().getPartition(db, hmetis_id);
				partition.getPartition_data_items().add(roaming_data);
			}
		}
	}
}