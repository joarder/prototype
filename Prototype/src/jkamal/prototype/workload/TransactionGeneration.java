/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.ArrayList;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;
import jkamal.prototype.db.GlobalDataMap;

public class TransactionGeneration {
	public TransactionGeneration(){}
	
	// This function will generate the required number of Transactions for a specific Workload with a specific Database
	public void generateTransaction(Database db, Workload workload) {
		GlobalDataMap dataMap = db.getDb_dataMap();						
		ArrayList<Transaction> transactionList;
		Transaction transaction;		
		Set<Data> trDataSet;
		Data data;		
		
		// Creating required local variables
		int data_id = 0;
		int data_weight = 0;
		
		int tr_id; 
		if(workload.getWrl_round() != 0)
			tr_id = workload.getWrl_totalTransaction();
		else 
			tr_id = 0;
		
		// Creating a Random Object for randomly chosen Data items
		Random random = new Random();
		// i -- Transaction types
		for(int i = 0; i < workload.getWrl_transactionTypes(); i++) {	
			transactionList = new ArrayList<Transaction>();
			
			// j -- a specific Transaction type in the Transaction proportion array
			for(int j = 0; j < (int)workload.getWrl_transactionProp()[i]; j++) {				
				trDataSet = new TreeSet<Data>();
				
				// k -- required numbers of Data items based on Transaction type
				for(int k = 0; k < i+2; k++) {
					data_id = random.nextInt(dataMap.getData_items().size());								
					data = dataMap.getData_items().get(data_id);
					data_weight = data.getData_weight();
					data.setData_weight(++data_weight);					
					trDataSet.add(data);					
				} // end--k for() loop
				
				++tr_id;				
				transaction = new Transaction(tr_id, trDataSet);				
				transaction.setTr_type(i);
				transaction.generateTransactionCost(db);
				
				if(workload.getWrl_transactionMap().containsKey(i))
					workload.getWrl_transactionMap().get(i).add(transaction);
				else
					transactionList.add(transaction);
			} // end--j for() loop
										
			if(workload.getWrl_round() == 0)
				workload.getWrl_transactionMap().put(i, transactionList);
		} // end--i for() loop
		
		workload.setWrl_totalTransaction(tr_id);
	}		
}