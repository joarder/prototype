/**
 * @author Joarder Kamal
 * 
 * Perform Data Movement after analysing Workload using HyperGraph Partitioning 
 */

package jkamal.prototype.db;

import java.util.Iterator;

import jkamal.prototype.transaction.Transaction;
import jkamal.prototype.transaction.TransactionDataSet;
import jkamal.prototype.workload.IdeaTable;
import jkamal.prototype.workload.Workload;

public class DataMovement {
	public DataMovement() {}
	
	public void move(Database db, Workload workload) {				
		TransactionDataSet transactionDataSet = workload.getWrl_transactionDataSet();
		int original_id = -1;
		int hmetis_id = -1;
		Data data;
		Data roaming_data;
		Partition partition; 
		int moves = 0;
		
		Iterator<Data> iterator = transactionDataSet.getTransactionDataSet().iterator();
		while(iterator.hasNext()) {
			data = iterator.next();
			original_id = data.getData_partition_id();
			hmetis_id = data.getData_hmetis_cluster_id();
			
			if(original_id != hmetis_id && !data.isData_isRoaming()) {				
				data.setData_roaming_partition_id(hmetis_id);
				data.setData_isRoaming(true);												
				
				roaming_data = new Data(data); // Calling Copy Constructor
				
				partition = db.getDb_partition_table().getPartition(hmetis_id);
				partition.getPartition_data_items().add(roaming_data);
				moves++;
			}
		}		
		
		//System.out.print("\n>> Total Data Moments Required using hMetis: "+moves);
		
		// Recalculate the Costs of Distributed Transactions (CDT)
		for(Transaction transaction : workload.getWrl_transactionList())
			transaction.generateTransactionCost(db);				
	}

	// This move() will utilize the Idea Matrix to perform Data movements
	public void move(Database db, Workload workload, IdeaTable ideaTable) {		
		TransactionDataSet transactionDataSet = workload.getWrl_transactionDataSet();
		int original_id = -1;
		int hmetis_id = -1;
		int idea_id = -1;
		int diagonal_pos = 1;
		int moves = 0;
		Data data;
		Data roaming_data;
		Partition partition; 
		
		Iterator<Data> iterator = transactionDataSet.getTransactionDataSet().iterator();
		while(iterator.hasNext()) {
			data = iterator.next();
			original_id = data.getData_partition_id();
			hmetis_id = data.getData_hmetis_cluster_id();
			idea_id = ideaTable.getKeyMap().get(original_id);
			diagonal_pos = idea_id;
			
			//System.out.print("\n-#-Oid:"+original_id+"|Hid:"+hmetis_id+"|Iid:"+idea_id);
			if(original_id != hmetis_id && !data.isData_isRoaming() && diagonal_pos == hmetis_id) {
				//System.out.print("\n-@-Oid:"+original_id+"|Hid:"+hmetis_id+"|Iid:"+idea_id);
				data.setData_roaming_partition_id(idea_id);
				data.setData_isRoaming(true);												
			
				roaming_data = new Data(data); // Calling Copy Constructor
			
				partition = db.getDb_partition_table().getPartition(idea_id);
				partition.getPartition_data_items().add(roaming_data);
				++moves;								
			}
		}
		
		//System.out.print("\n>> Total Data Moments Required using Idea: "+moves);
		
		// Recalculate the Costs of Distributed Transactions (CDT)
		for(Transaction transaction : workload.getWrl_transactionList())
			transaction.generateTransactionCost(db);
	}
}