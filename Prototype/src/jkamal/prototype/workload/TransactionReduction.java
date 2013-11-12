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
		
		verifyTransactionDeathProportions(workload);
		
		// i -- Transaction types
		for(int i = 0; i < workload.getWrl_transactionTypes(); i++) {			
			transactionList = workload.getWrl_transactionMap().get(i);
			listSize = workload.getWrl_transactionMap().get(i).size();
			
			// j -- a specific Transaction type in the Transaction proportion array
			for(int j = 0; j < workload.getWrl_transactionDeathProportions()[i]; j++) {				
				//System.out.println(" >> Removing "+transactionList.get(workload.getWrl_transactionMap().get(i).size() - 1)+" from the current workload ...");
				transactionList.remove(listSize - 1);
				--listSize;
				workload.decWrl_totalTransactions();				
				workload.decWrl_transactionProportions(i);								
			} // end -- j			
			
			//System.out.println("@debug >> total TR = "+workload.getWrl_totalTransaction());
		} // end -- i		
	}
	
	public void verifyTransactionDeathProportions(Workload workload) {
		int difference = 0;
		int transactionProportions[] = workload.getWrl_transactionProportions();
		int deathProportions[] = workload.getWrl_transactionDeathProportions();
		
		for(int i = 0; i < transactionProportions.length; i++) {
			if(deathProportions[i] > transactionProportions[i]) {
				difference = deathProportions[i] - transactionProportions[i];
				workload.getWrl_transactionDeathProportions()[i] -= difference;
				
				workload.setWrl_transactionDying(workload.getWrl_transactionDying() - difference);
				
				System.out.print("[DBG] Killing "+workload.getWrl_transactionDying()+" old transactions with a distribution of ");				
				workload.printWrl_transactionProp(workload.getWrl_transactionDeathProportions());
				System.out.println();
			}
		}
	}
}