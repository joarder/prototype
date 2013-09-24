/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.Set;
import java.util.TreeSet;

public class WorkloadSampling {
	public WorkloadSampling() {}
	
	public void performSampling(Workload workload) {
		System.out.println();
		Set<Integer> trMarkers = new TreeSet<Integer>();		
		for(Transaction transaction : workload.getWrl_transactionList()) {
			if(transaction.getTr_dtCost() == 0) { 
				trMarkers.add(transaction.getTr_id());
				//System.out.println("@debug >> Marking Tr("+transaction.getTr_id()+")");
			}
		}	
		
		System.out.println();
		// Remove the Transaction from the Transaction list and DataMap Tree
		for(Integer marker : trMarkers) {
			Transaction transaction = workload.findWrl_transaction(marker);			
			
			//System.out.println("@debug >> Deleting Tr("+transaction.getTr_id()+")");
			workload.getWrl_transactionList().remove(transaction);
			workload.getWrl_trDataMap().remove(marker);
		}
	}
}