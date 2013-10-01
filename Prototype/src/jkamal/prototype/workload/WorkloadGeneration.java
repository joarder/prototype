/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.ArrayList;
import java.util.Random;
import java.util.Map.Entry;

import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;
import jkamal.prototype.db.DatabaseServer;

public class WorkloadGeneration {	
	public WorkloadGeneration() {}	
	
	public Workload init(Database db, String workload_name, int workload_id) {
		// Workload Details : http://oltpbenchmark.com/wiki/index.php?title=Workloads
		switch(workload_name) {
			case "AuctionMark":
				return(new Workload(workload_id, 10, db.getDb_id()));			
			case "Epinions":
				return(new Workload(workload_id, 9, db.getDb_id()));
			case "SEATS":
				return(new Workload(workload_id, 6, db.getDb_id()));
			case "TPC-C":
				return(new Workload(workload_id, 5, db.getDb_id()));	
			}		
		
		return null;
	}
	
	public Workload generateWorkload(DatabaseServer dbs, Database db, Workload workload, String DIR_LOCATION) {								
		if(workload.getWrl_round() != 0) {
			//workload.setWrl_transactionVarProp(generateTransactionProp(workload));
			workload.setWrl_transactionVarProp(generateWorkloadVar(workload));
			this.incrTransactionWeights(workload);
			workload.printWrl_transactionVarProp();
			System.out.println();
		} else {			
			workload.setWrl_transactionProp(generateTransactionProp(workload));
			workload.printWrl_transactionProp();
			System.out.println();
		}		
		
		if(workload.isWrl_mode()) {
			// Generating Workload Transactions
			TransactionGeneration trGen = new TransactionGeneration();
			trGen.generateTransaction(db, workload);
			if(workload.getWrl_round() != 0) {
				System.out.println("[MSG] "+workload.getWrl_transactionVariant()+" new transaction are added to the workload as a result of workload variation.");
				WorkloadGeneration.print(workload);
			}
			
			this.workloadEvaluation(db, workload);
			this.assignHMetisId(workload);
		} else {
			// Reducing Workload Transactions
			TransactionReduction trRed = new TransactionReduction();
			trRed.reduceTransaction(db, workload);
			if(workload.getWrl_round() != 0) {
				System.out.println("[MSG] "+workload.getWrl_transactionVariant()+" old transaction are removed from the workload as a result of workload variation.");
				WorkloadGeneration.print(workload);			
			}
			
			this.workloadEvaluation(db, workload);
			this.assignHMetisId(workload);
		}
		
		if(!workload.isWorkloadEmpty()) {
			// Generating Workload's Data Partition and Node Distribution Details
			workload.generateDataPartitionTable();
			workload.generateDataNodeTable();		
			
			// Generating Workload and FixFile for HyperGraph Partitioning			
			workload.generateWorkloadFile(dbs, workload, DIR_LOCATION);
			workload.generateFixFile(DIR_LOCATION);				
			
			// Calculate the percentage of DT
			workload.calculateDTPercentage();
		} else {
			// Workload is empty
		}
				
		return workload;
	}
	
	public void incrTransactionWeights(Workload workload) {
		int tr_weight = 0;
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				tr_weight = transaction.getTr_weight();
				transaction.setTr_weight(++tr_weight);
			}
		}
	}
	
	public static int randInt(int min, int max) {
	    Random rand = new Random();
	    int randomNumber = rand.nextInt((max - min) + 1) + min;	    

	    return randomNumber;
	}
	
	public double[] generateWorkloadVar(Workload workload) {
		int array_size = workload.getWrl_transactionTypes();
		double array[] = new double[array_size];
		int maxRange = 0;
		int minRange = 1;		
		int sum = 0;
		
		for(int i = 0; i < array_size; i++) {
			maxRange = (int)workload.getWrl_transactionProp()[i];
			if(maxRange == 0) {
				maxRange = 0;
				minRange = 0;
			} else {
				minRange = 1;
			}
			
			array[i] = (int)WorkloadGeneration.randInt(minRange, maxRange);
			//System.out.println("Max = "+maxRange+" | array[i]="+array[i]);
			sum += array[i];
		}
		
		 int roundings = 0;
		 int rounding_result = 0;
		 
		 if(workload.getWrl_round() != 0)
			 roundings = workload.getWrl_transactionVariant();
		 else
			 roundings = workload.getWrl_totalTransaction();
	        
		 int val = 0;
		 int remain = 0;
		 for (int i = 0; i < array_size; i++) {
	        	val = (int)Math.round((array[i] / sum) * roundings);
	        	val += remain;
	        	
	        	if(val >= workload.getWrl_transactionProp()[i]) {
	        		array[i] = workload.getWrl_transactionProp()[i];
	        		remain = val - (int)workload.getWrl_transactionProp()[i];
	        	} else {
	        		array[i] = val;
	        	}
	        	
	        	rounding_result += array[i];        	
		 }        
		
		//System.out.println("@debug >> Rounding Result = "+rounding_result+" | Roundings = "+roundings);
		 
		 int trNums = 0;
		 if(rounding_result > roundings) {
	        	if(workload.getWrl_round() != 0)
	        		trNums = workload.getWrl_transactionVariant();
	        	else
	        		trNums = workload.getWrl_totalTransaction();
	        	
	        	for(int i = 0; i < array_size; i++) {        		
	        		trNums -= array[i];
	        		if(trNums <= 1)
	        			array[i] -= Math.abs((rounding_result - roundings));
	        	}
		 }		 
	        
		 if(rounding_result < roundings) {
	        	if(workload.getWrl_round() != 0)
	        		trNums = workload.getWrl_transactionVariant();
	        	else
	        		trNums = workload.getWrl_totalTransaction();
	        	
	        	for(int i = 0; i < array_size; i++) {        		
	        		trNums -= array[i];        	        		
	        		if(trNums <= 1)
	        			array[i] += Math.abs((rounding_result - roundings));
	            }
		 }   
		 
		 return array;
	}
	
	public double[] generateTransactionProp(Workload workload) {
		int array_size = workload.getWrl_transactionTypes();
		double array[] = new double[array_size];
        double sum = 0.0d;
        double rounding_result = 0.0d;
        Random random = new Random();
        
        
        for (int i = 0; i < array_size; i++) {
        	array[i] = random.nextDouble();        
        	sum += array[i];
        }        
            
        double roundings = 0.0d;
        if(workload.getWrl_round() != 0)
        	roundings = workload.getWrl_transactionVariant();
        else
        	roundings = workload.getWrl_totalTransaction();
        
        for (int i = 0; i < array_size; i++) {
        	array[i] = (double)Math.round((array[i] / sum) * roundings);        
        	rounding_result += array[i];        	
        }       
                        
        //System.out.println("@debug >> Rounding Result = "+rounding_result+" | Roundings = "+roundings);        
                
        int trNums = 0;
        if(rounding_result > roundings) {
        	if(workload.getWrl_round() != 0)
        		trNums = workload.getWrl_transactionVariant();
        	else
        		trNums = workload.getWrl_totalTransaction();
        	
        	for(int i = 0; i < array_size; i++) {        		
        		trNums -= array[i];
        		if(trNums <= 1)
        			array[i] -= Math.abs((rounding_result - roundings));
            }
        }
        
        if(rounding_result < roundings) {
        	if(workload.getWrl_round() != 0)
        		trNums = workload.getWrl_transactionVariant();
        	else
        		trNums = workload.getWrl_totalTransaction();
        	
        	for(int i = 0; i < array_size; i++) {        		
        		trNums -= array[i];        	        		
        		if(trNums <= 1)
        			array[i] += Math.abs((rounding_result - roundings));
            }
        }        
        
        return array;
	}
	
	public void assignHMetisId(Workload workload) {
		int total_dataItems = 0;
		int hmetis_shadow_data_id = 1;
		
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				for(Data trData : transaction.getTr_dataSet()) {
					if(!trData.isData_hasShadowHMetisId()) {					
						trData.setData_shadow_hmetis_id(hmetis_shadow_data_id);
						trData.setData_hasShadowHMetisId(true);
						//System.out.println("@debug >> hData("+trData.toString()+") | hkey: "+trData.getData_shadow_hmetis_id());
						
						++total_dataItems;
						++hmetis_shadow_data_id;					
					} else {
						//System.out.println("@debug >> *Repeated Data ("+trData.toString()+") | hkey: "+trData.getData_shadow_hmetis_id());					
					}
				} // end -- for()-Data
			} // end -- for()-Transaction
		} // end -- for()-Transaction Types
		
		workload.setWrl_totalData(total_dataItems);
	}
	
	public void workloadEvaluation(Database db, Workload workload) {
		WorkloadSampling workloadSampling = workload.getWorkloadSampling();
		boolean emptyWorkload = false;
		
		// Workload Sampling
		System.out.println("[ACT] Sampling Workload ...");		
		workloadSampling.performSampling(workload);
		System.out.println("[MSG] Total "+workloadSampling.getDiscardedTransaction()+" non-distributed transactions are discarded from the workload.");
		WorkloadGeneration.print(workload);
						
		// Re-evaluate Discarded Transactions
		if(workload.getWrl_round() != 0) {
			System.out.println("[ACT] Re-evaluating previously discarded workload ...");
			emptyWorkload = workloadSampling.includeDiscardedWorkload(db, workload);
			System.out.println("[MSG] Total "+workloadSampling.getReseletedTransaction()+" newly distributed transactions are included from the previously discarded workload.");
			WorkloadGeneration.print(workload);
		}
		
		if(emptyWorkload) {
			System.out.println("[ALM] Empty workload !!! No transactions included !!!");
			workload.setWorkloadEmpty(true);
		}
	}
	
	public static void print(Workload workload) {
		System.out.print("[MSG] Total "+workload.getWrl_totalTransaction()+" transactions of "
				+workload.getWrl_transactionTypes()+" types having a distribution of ");										
				workload.printWrl_transactionProp();
				System.out.println(" are currently in the workload.");
	}	
}