/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

import jkamal.prototype.db.Database;
import jkamal.prototype.main.DBMSSimulator;

public class TransactionReducer {
	public TransactionReducer() {}
	
	// This function will reduce the required number of Transactions for a specific Workload with a specific Database
	public void reduceTransaction(Database db, Workload workload) {
		ArrayList<Transaction> transactionList = null;
		int listSize = 0;
		
		verifyTransactionDeathProportions(workload);
		
		// i -- Transaction types
		for(int i = 0; i < workload.getWrl_transactionTypes(); i++) {			
			transactionList = workload.getWrl_transactionMap().get(i);
			listSize = workload.getWrl_transactionMap().get(i).size();
			
			Set<Integer> random_transactions = this.randomSelection(listSize -1);
			for(int j : random_transactions) {
				transactionList.remove(j);
				
				workload.decWrl_totalTransactions();				
				workload.decWrl_transactionProportions(i);
			}
			
/*			// j -- a specific Transaction type in the Transaction proportion array
			for(int j = 0; j < workload.getWrl_transactionDeathProportions()[i]; j++) {				
				//System.out.println(" >> Removing "+transactionList.get(workload.getWrl_transactionMap().get(i).size() - 1)+" from the current workload ...");
				transactionList.remove(listSize - 1);
				--listSize;		
				
				workload.decWrl_totalTransactions();				
				workload.decWrl_transactionProportions(i);								
			} // end -- j			
*/			
			//System.out.println("@debug >> total TR = "+workload.getWrl_totalTransaction());
		} // end -- i		
	}
	
	// Randomly selects Transactions for deletion
	public Set<Integer> randomSelection(int nums) {
		Set<Integer> random_transactions = new TreeSet<Integer>();
		DBMSSimulator.random_data.reSeed(0);
		
		for(int i = 0; i < nums; i++)
			random_transactions.add((int) DBMSSimulator.random_data.nextUniform(0, nums));
		
		return random_transactions;
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