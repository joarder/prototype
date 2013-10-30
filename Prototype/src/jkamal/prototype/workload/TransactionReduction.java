/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.ArrayList;

import jkamal.prototype.db.Database;

public class TransactionReduction {
	public TransactionReduction() {}
	
	// This function will reduce the required number of Transactions for a specific Workload with a specific Database
	public void reduceTransaction(Database db, Workload workload) {
		ArrayList<Transaction> transactionList = null;
		int listSize = 0;
		
		// i -- Transaction types
		for(int i = 0; i < workload.getWrl_transactionTypes(); i++) {			
			transactionList = workload.getWrl_transactionMap().get(i);
			listSize = workload.getWrl_transactionMap().get(i).size();
			
			// j -- a specific Transaction type in the Transaction proportion array
			for(int j = 0; j < workload.getWrl_transactionDeathProp()[i]; j++) {				
				//System.out.println(" >> Removing "+transactionList.get(workload.getWrl_transactionMap().get(i).size() - 1)+" from the current workload ...");
				transactionList.remove(listSize - 1);
				--listSize;
				workload.decWrl_totalTransaction();				
				workload.decWrl_transactionPropVal(i);								
			} // end -- j			
			
			//System.out.println("@debug >> total TR = "+workload.getWrl_totalTransaction());
		} // end -- i		
	}
}