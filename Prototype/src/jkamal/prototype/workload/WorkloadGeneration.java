/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;
import jkamal.prototype.db.GlobalDataMap;

public class WorkloadGeneration {	
	private List<Workload> workloadList;
	
	public WorkloadGeneration() {
		this.setWorkloadList(new ArrayList<Workload>());		
	}	

	public List<Workload> getWorkloadList() {
		return workloadList;
	}

	public void setWorkloadList(List<Workload> workloadList) {
		this.workloadList = workloadList;
	}
	
	public Workload initWorkload(Database db, String workload_name) {
		switch(workload_name) {
		case "AuctionMark":
			Workload workload = new Workload(0, 10, db.getDb_id());
			this.getWorkloadList().add(workload);
			return workload;
		}
		
		return null;
	}

	public Workload generateWorkload(Database db, String workload_name, int transaction_nums, String DIR_LOCATION) {		
		Workload workload = initWorkload(db, workload_name);

		// Generating Random proportions for different Transaction types based on Workload type
		int wrl_type = workload.getWrl_type();
		workload.setWrl_transactionProp(generateTransactionProp(transaction_nums, wrl_type));

		// Retrieving corresponding GlobalDataMap and TransactionDataSet
		GlobalDataMap dataMap = db.getDb_dataMap();
		List<Data> wrlDataList = workload.getWrl_dataList();
		Transaction transaction;		
		Set<Data> trDataSet;
		Data data;
		
		// Creating required local variables
		int data_id = 0;
		int data_weight = 0;
		int hmetis_shadow_data_id = 0;
		int tr_id = -1;
		
		// Creating a Random Object for randomly chosen Data items
		Random random = new Random();
		// i -- Transaction types
		for(int i = 0; i < wrl_type; i++) {			
			// j -- a specific Transaction type in the Transaction proportion array
			for(int j = 0; j < (int)workload.getWrl_transactionProp()[i]; j++) {				
				trDataSet = new TreeSet<Data>();
				// k -- required numbers of Data items based on Transaction type
				for(int k = 0; k < i+2; k++) {
					data_id = random.nextInt(dataMap.getData_items().size());								
					data = dataMap.getData_items().get(data_id);
					data_weight = data.getData_weight();
					data.setData_weight(++data_weight);					
					
					// Set shadow id to use in the hMetis HyperGraph partitioning tool
					if(!data.isData_hasShadowHMetisId()) {
						data.setData_shadow_hmetis_id(++hmetis_shadow_data_id);
						data.setData_hasShadowHMetisId(true);
					}
					
					trDataSet.add(data);
					wrlDataList.add(data);
				}
				
				transaction = new Transaction(++tr_id, trDataSet);
				transaction.generateTransactionCost(db);
				workload.getWrl_transactionList().add(transaction);
			}
		}		

		// Generating Workload and FixFile for HyperGraph Partitioning			
		workload.generateWorkloadFile(DIR_LOCATION);
		workload.generateFixFile(DIR_LOCATION);
		
		//
		workload.generateDataPartitionTable();
		workload.generateDataNodeTable();
		
		return workload;
	}

	public static int randInt(int min, int max) {
	    Random random = new Random();
	    int randomNum = random.nextInt((max - min) + 1) + min;

	    return randomNum;
	}	
	
	public double[] generateTransactionProp(int nums, int type) {
		double array[] = new double[type];
        double sum = 0.0d;
        double rounding_result = 0.0d;
        //double rounding_correction = 0.0d;
        Random random = new Random();
        
        for (int i = 0; i < type; i++) {
        	array[i] = random.nextDouble();
        	sum += array[i];
        }
                
        double roundings = nums;
        for (int i = 0; i < type; i++) {
        	array[i] = (double)Math.round((array[i] / sum) * roundings);
        	rounding_result += array[i];
        }       
                        
        //System.out.println("Rounding Result = "+rounding_result);        
        
        if(rounding_result > roundings) {
        	array[type-1] -= (rounding_result - roundings);
        	//rounding_correction -= rounding_result - roundings;
        }
        
        if(rounding_result < roundings) {
        	array[type-1] += (roundings - rounding_result);
        	//rounding_correction += roundings - rounding_result;
        }
        
        //System.out.println("Rounding Correction = "+rounding_correction);
        
        return array;
	}
}