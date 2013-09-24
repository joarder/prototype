/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.Map.Entry;
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
		int wrl_type = workload.getWrl_type();
		double[] wrl_prop = workload.getWrl_transactionProp();		
		Transaction transaction;		
		Set<Data> trDataSet;
		Data data;		
		
		// Creating required local variables
		int data_id = 0;
		int data_weight = 0;		
		int tr_id = workload.getWrl_totalTransaction();
		int total_data = 0;
		
		// Creating a Random Object for randomly chosen Data items
		Random random = new Random();
		// i -- Transaction types
		for(int i = 0; i < wrl_type; i++) {			
			// j -- a specific Transaction type in the Transaction proportion array
			for(int j = 0; j < (int)wrl_prop[i]; j++) {				
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
				transaction.generateTransactionCost(db);

				workload.getWrl_transactionList().add(transaction);
				workload.getWrl_trDataMap().put(tr_id, trDataSet);
			} // end--j for() loop
		} // end--i for() loop
		workload.setWrl_totalTransaction(tr_id);

		// Workload Sampling
		//System.out.println(">> Performing workload sampling ...");
		//WorkloadSampling workloadSampling = new WorkloadSampling();
		//workloadSampling.performSampling(workload);			
		
		// Assign unique shadow hmetis id to individual data items
		int hmetis_shadow_data_id = 1;
		for(Entry<Integer, Set<Data>> entry : workload.getWrl_trDataMap().entrySet()) {
			for(Data trData : entry.getValue()) {
				if(!trData.isData_hasShadowHMetisId()) {					
					trData.setData_shadow_hmetis_id(hmetis_shadow_data_id);
					trData.setData_hasShadowHMetisId(true);
					//System.out.println("@debug >> hData("+trData.toString()+") | hkey: "+trData.getData_shadow_hmetis_id());
					
					++total_data;
					++hmetis_shadow_data_id;					
				} else {
					//System.out.println("@debug >> *Repeated Data ("+trData.toString()+") | hkey: "+trData.getData_shadow_hmetis_id());					
				}
			}
		}

		workload.setWrl_totalData(total_data);
	}
}