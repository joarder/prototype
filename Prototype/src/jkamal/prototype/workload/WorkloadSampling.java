/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import jkamal.prototype.db.Database;

public class WorkloadSampling {
	private Map<Integer, ArrayList<Transaction>> discardedWorkload;
	private int discardedTransaction;
	private int reseletedTransaction;
	
	public WorkloadSampling() {
		this.setDiscardedWorkload(new TreeMap<Integer, ArrayList<Transaction>>());
		this.setDiscardedTransaction(0);
		this.setReseletedTransaction(0);
	}
	
	public WorkloadSampling(WorkloadSampling workloadSampling) {
		this.setDiscardedTransaction(workloadSampling.getDiscardedTransaction());
		this.setReseletedTransaction(workloadSampling.getReseletedTransaction());
		
		Map<Integer, ArrayList<Transaction>> cloneDiscardedWorkload = new TreeMap<Integer, ArrayList<Transaction>>();
		int cloneTransactionType;		
		ArrayList<Transaction> cloneTransactionList;
		Transaction cloneTransaction;		
		for(Entry<Integer, ArrayList<Transaction>> entry : workloadSampling.getDiscardedWorkload().entrySet()) {
			cloneTransactionType = entry.getKey();
			cloneTransactionList = new ArrayList<Transaction>();
			for(Transaction tr : entry.getValue()) {
				cloneTransaction = new Transaction(tr);
				cloneTransactionList.add(cloneTransaction);
			}
			cloneDiscardedWorkload.put(cloneTransactionType, cloneTransactionList);
		}
		this.setDiscardedWorkload(cloneDiscardedWorkload);
	}
	
	public int getDiscardedTransaction() {
		return discardedTransaction;
	}

	public void setDiscardedTransaction(int discardedTransaction) {
		this.discardedTransaction = discardedTransaction;
	}

	public Map<Integer, ArrayList<Transaction>> getDiscardedWorkload() {
		return discardedWorkload;
	}

	public void setDiscardedWorkload(Map<Integer, ArrayList<Transaction>> discardedWorkload) {
		this.discardedWorkload = discardedWorkload;
	}

	public int getReseletedTransaction() {
		return reseletedTransaction;
	}

	public void setReseletedTransaction(int reseletedTransaction) {
		this.reseletedTransaction = reseletedTransaction;
	}

	public void performSampling(Workload workload) {		
		Set<Integer> trMarkers = new TreeSet<Integer>();		
		//for(Transaction transaction : workload.getWrl_transactionSet()) {
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				if(transaction.getTr_dtCost() == 0) { // Not DT
					trMarkers.add(transaction.getTr_id());
					//System.out.println("@debug >> Marking Tr("+transaction.getTr_id()+")");
				}
			} // end -- for()-Transaction
		} // end -- for()-Transaction Types	
				
		// Remove the Transaction from the Transaction Map and add to Discarded Transaction Map
		Transaction delTransaction = null;
		Transaction cloneTransaction = null;
		int trType = 0;
		int discardedTransaction = 0;
		ArrayList<Transaction> transactionList;
		
		for(Integer marker : trMarkers) {
			delTransaction = workload.findWrl_transaction(marker);
			trType = delTransaction.getTr_type();
			
			//System.out.println("@debug >> Discarding Tr("+delTransaction.getTr_id()+") type"+trType);
			
			cloneTransaction = new Transaction(delTransaction);						
			if(this.getDiscardedWorkload().containsKey(trType))
				this.getDiscardedWorkload().get(trType).add(cloneTransaction);
			else {
				transactionList = new ArrayList<Transaction>();
				transactionList.add(delTransaction);
				this.getDiscardedWorkload().put(trType, transactionList);
			}						
			
			workload.getWrl_transactionMap().get(trType).remove(delTransaction);
			
			++discardedTransaction;
			this.setDiscardedTransaction(discardedTransaction);
			
			workload.decWrl_transactionPropVal(trType);						
			workload.decWrl_totalTransaction();							
		}
		
		//@debug
		//System.out.print("*");
		//workload.printWrl_transactionProp();
		//System.out.println("");
	}
	
	public boolean includeDiscardedWorkload(Database db, Workload workload) {
		int trType = 0;
		int reselectedTransactions = 0;
		Set<Integer> delMarkers = new TreeSet<Integer>();
				
		for(Entry<Integer, ArrayList<Transaction>> entry : this.getDiscardedWorkload().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				transaction.generateTransactionCost(db);
				trType = transaction.getTr_type();
				
				if(transaction.getTr_dtCost() != 0) { // DT
					workload.getWrl_transactionMap().get(trType).add(transaction);
					
					workload.incWrl_transactionPropVal(trType);					
					workload.incWrl_totalTransaction();
										
					++reselectedTransactions;										
					this.setReseletedTransaction(reselectedTransactions);
				} else {
					// Not DT, therefore finally discarded
					delMarkers.add(transaction.getTr_id());						
				}
			}
		}				
		
		Transaction delTransaction;
		int discardedTransaction = 0;
		for(Integer marker : delMarkers) {
			delTransaction = this.findTransaction(marker);
			
			//System.out.println("@debug >> Finally discarding Tr("+delTransaction.getTr_id()+")");
			this.getDiscardedWorkload().get(trType).remove(delTransaction);
			
			discardedTransaction = this.getDiscardedTransaction();
			--discardedTransaction;
			this.setDiscardedTransaction(discardedTransaction);
		}	
		
		if(workload.getWrl_totalTransaction() <= 0) {
			return true;
		}
		
		//@debug		
		//System.out.print("*[");
		//workload.printWrl_transactionProp();
		//System.out.println("]");
		
		return false;
	}
	
	
	public Transaction findTransaction(int tr_id) {		
		for(Entry<Integer, ArrayList<Transaction>> entry : this.getDiscardedWorkload().entrySet()) {			
			for(Transaction transaction : entry.getValue()) {
				if(transaction.getTr_id() == tr_id)
					return transaction;
			}
		}
		
		return null;
	}
}