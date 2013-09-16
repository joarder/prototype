/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.List;
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
		// Retrieving corresponding Workload Type, Workload Transaction Proportions, GlobalDataMap and TransactionDataSet
		GlobalDataMap dataMap = db.getDb_dataMap();
		int wrl_type = workload.getWrl_type();
		double[] wrl_prop = workload.getWrl_transactionProp();		
		List<Data> wrlDataList = workload.getWrl_dataList();
		Transaction transaction;		
		Set<Data> trDataSet;
		Data data;
		
		// Creating required local variables
		int data_id = 0;
		int data_weight = 0;
		int hmetis_shadow_data_id = 0;
		int tr_id = workload.getWrl_transactionList().size();
		
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
					wrlDataList.add(data);
				} // end--k for() loop
				
				transaction = new Transaction(++tr_id, trDataSet);
				transaction.generateTransactionCost(db);
				workload.getWrl_transactionList().add(transaction);
			} // end--j for() loop
		} // end--i for() loop
		
		// Assign unique shadow hmetis id to individual data items		
		for(Transaction tr : workload.getWrl_transactionList()) {
			for(Data d : tr.getTr_dataSet()) {
				// Set shadow id to use in the hMetis HyperGraph partitioning tool
				if(!d.isData_hasShadowHMetisId()) {
					d.setData_shadow_hmetis_id(++hmetis_shadow_data_id);
					d.setData_hasShadowHMetisId(true);	
				} else {
					System.out.println("@debug >> *** "+d.getData_shadow_hmetis_id()+"|"+d.toString());
				}
			}
		}
	}
}