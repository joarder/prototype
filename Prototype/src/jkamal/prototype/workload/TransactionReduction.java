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
		int totalTransaction = workload.getWrl_totalTransaction();
		int varTransaction = workload.getWrl_transactionVariant();
		
		// i -- Transaction types
		for(int i = 0; i < workload.getWrl_transactionTypes(); i++) {			
			// j -- a specific Transaction type in the Transaction proportion array
			
			for(int j = 0; j < (int)workload.getWrl_transactionProp()[i]; j++) {
				if(workload.getWrl_transactionMap().containsKey(i)) {
					transactionList = workload.getWrl_transactionMap().get(i);
				
					if(transactionList.size() != 0) {
						//System.out.println(" >> Removing "+transactionList.get(workload.getWrl_transactionMap().get(i).size() - 1)+" from the current workload ...");
						transactionList.remove(workload.getWrl_transactionMap().get(i).size() - 1);
						--totalTransaction;						
						workload.setWrl_totalTransaction(totalTransaction);
					} else {
						System.out.println("@debug >> Transaction List size has underflown for type "+i+" !!!");
						System.out.println("@debug >> Adjusting the randmo reduction proportions for type "+i+" !!!");
						--varTransaction;
					}
				} else {
					System.out.println("@debug >> Transaction Type Not Found !!!");
					--varTransaction;
				}
			}
		}
		
		workload.setWrl_transactionVariant(varTransaction);
	}
}