/**
 * @author Joarder Kamal
 */

package jkamal.prototype.io;

import jkamal.prototype.workload.Workload;

public class PrintWorkloadDetails {
	public PrintWorkloadDetails() {}
	
	public void printDetails(Workload workload) {
		//System.out.println();
		System.out.println("===Workload Details========================");
		//System.out.println("Total number of transactions: "+workload.getWrl_total_transactions());
		System.out.println("Total number of transaction types: "+workload.getWrl_type());
		System.out.print("Transaction proportions: {");				
		
		
				
		System.out.println("\n");
		System.out.println("===Transaction Details========================");
		for(int i = 0; i < workload.getWrl_type(); i++) {
			System.out.println("===Type["+i+"] Transactions===");
			
			if(workload.getWrl_transactionProp()[i] == 0)
				System.out.println("! No Transactions");
						
			//for(int j = 0; j < workload.getWrl_transactionProp()[i]; j++) {
				//Transaction tr = workload.getWrl_transactionList().get(j);
				//tr.print();				
			//}			
			
			System.out.println();
		}
	}
}