/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.ArrayList;
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
		int transaction_types = workload.getWrl_transactionTypes();
		
		if(workload.getWrl_round() != 0) {
			// === Death Management
			System.out.println("[MSG] Old Transaction Death Rate: "+workload.getWrl_transactionDeathRate());
			System.out.print("[ACT] Killing "+workload.getWrl_transactionDying()+" old transactions with a distribution of ");			
			
			int[] deathArray = transactionPropGen(transaction_types, workload.getWrl_transactionDying());
			workload.setWrl_transactionDeathProp(deathArray);
			
			workload.printWrl_transactionProp(workload.getWrl_transactionDeathProp());
			System.out.println();																		
			
			// Reducing Old Workload Transactions			
			WorkloadGeneration.print(workload);
			TransactionReduction trRed = new TransactionReduction();
			trRed.reduceTransaction(db, workload);
			
			WorkloadGeneration.print(workload);			
			
			// === Birth Management
			System.out.println("[MSG] New Transaction Birth Rate: "+workload.getWrl_transactionBirthRate());
			System.out.print("[ACT] Creating "+workload.getWrl_transactionBorning()+" new transactions with a distribution of ");																	
			
			int[] birthArray = transactionPropGen(transaction_types, workload.getWrl_transactionBorning());
			workload.setWrl_transactionBirthProp(birthArray);
			
			workload.printWrl_transactionProp(workload.getWrl_transactionBirthProp());			
			System.out.println();
			
			// Generating New Workload Transactions						
			TransactionGeneration trGen = new TransactionGeneration();
			trGen.generateTransaction(db, workload);			
			
			WorkloadGeneration.print(workload);						
			
			// Other Stuffs
			this.incrTransactionWeights(workload);
			this.workloadEvaluation(db, workload);
			this.assignHMetisId(workload);
		} else {
			// Initial Round			
			workload.setWrl_transactionProp(transactionPropGen(transaction_types, workload.getWrl_initTotalTransactions()));			
			workload.printWrl_transactionProp(workload.getWrl_transactionProp());
			System.out.println();
			
			// Generating New Workload Transactions
			TransactionGeneration trGen = new TransactionGeneration();
			trGen.generateTransaction(db, workload);
			this.assignHMetisId(workload);		
		}																		
		
		// Generating Workload's Data Partition and Node Distribution Details
		workload.generateDataPartitionTable();
		workload.generateDataNodeTable();		
		
		// Generating Workload and FixFile for HyperGraph Partitioning			
		workload.generateWorkloadFile(dbs, workload, DIR_LOCATION);
		workload.generateFixFile(DIR_LOCATION);				
		
		// Calculate the percentage of DT
		workload.calculateDTPercentage();			
				
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
	
	// new
	public int[] transactionPropGen(int ranks, int elements) {		
		int propArray[] = new int[ranks];
		int rankArray[] = zipfLawProportionGen(ranks, elements);
		
		// TR Rankings {T1, T2, T3, T4, T5} = {4, 5, 1, 2, 3}; 1 = Higher, 5 = Lower
		int begin = 0;
		int end = rankArray.length-1;
		for(int i = 0; i < propArray.length; i++) {
			if(i < 2) {
				propArray[i] = rankArray[end];
				-- end;
			} else {
				propArray[i] = rankArray[begin];
				++ begin;
			}			
			//System.out.println("@debug >> TR-"+(i+1)+" | Counts = "+propArray[i]);
		}
		
		return propArray;
	}	
	
	//new
	public int[] zipfLawProportionGen(int ranks, int elements) {
		double prop[] = new double[ranks];
		int finalProp[] = new int[ranks];
		
		double sum = 0.0d;
		for(int rank = 0; rank < ranks; rank++) {
			prop[rank] = elements / (rank+1); // exponent value is always 1
			sum += prop[rank];
		}
		
		//System.out.println();		
		double amplification = elements/sum;		
		int finalSum = 0;
		for(int rank = 0; rank < ranks; rank++) {
			finalProp[rank] = (int) (prop[rank] * amplification);
			finalSum += finalProp[rank];
			
			//System.out.println("@debug >> Rank-"+(rank+1)+" | Counts = "+finalProp[rank]);
		}		
		
		//System.out.println("@debug >> Sum = "+finalSum+" | Difference = "+(elements - finalSum));
		
		finalProp[0] += (elements - finalSum); // Adjusting the difference by adding it to the highest rank proportion
		finalSum += (elements - finalSum);
		
		//System.out.println("@debug >> * Rank-1 | Counts = "+finalProp[0]);
		//System.out.println("@debug >> Sum = "+finalSum+" | Difference = "+(elements - finalSum));
		
		return finalProp;
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
		System.out.println("[MSG] Total "+workloadSampling.getDiscardedTransaction()+" non-distributed transactions are discarded from current workload" +
				"and left for re-analysing for the next round.");
		WorkloadGeneration.print(workload);
						
		// Re-evaluate Discarded Transactions
		if(workload.getWrl_round() != 0) {
			System.out.println("[ACT] Re-analysing previously discarded workload ...");
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
		System.out.print("[MSG] Total "+workload.getWrl_totalTransaction()+" transactions of "+workload.getWrl_transactionTypes()
				+" types having a distribution of ");										
		workload.printWrl_transactionProp(workload.getWrl_transactionProp());
		System.out.println(" are currently in the workload.");
	}	
}