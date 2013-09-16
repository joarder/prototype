/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

public class WorkloadSampling {
	public WorkloadSampling() {}
	
	public Workload performSampling(Workload workload) {
		Workload cloneWorkload = new Workload(workload);
		int tr_id;
		
		for(Transaction transaction : cloneWorkload.getWrl_transactionList()) {
			if(transaction.getTr_dtCost() == 0) {
				tr_id = transaction.getTr_id();
				
				Transaction tr = workload.findWrl_transaction(tr_id);
				if(tr_id == tr.getTr_id())
					workload.getWrl_transactionList().remove(tr);
			}
		}							
		
		return workload;
	}
}