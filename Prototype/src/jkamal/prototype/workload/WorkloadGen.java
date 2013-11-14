/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;
import jkamal.prototype.db.DatabaseServer;
import jkamal.prototype.main.DBMSSimulator;

public class WorkloadGen {
	DatabaseServer database_server = null;
	Database database = null;
	
	public WorkloadGen(DatabaseServer dbs, Database db) {
		this.database_server = dbs;
		this.database = db;
	}
	
	public Transaction createTransaction(int transaction_id, int transaction_type, int data_numbers, Database db) {
		Transaction transaction = null;
		Set<Data> transaction_data_set = new TreeSet<Data>();
		Data data;
		int data_id = 0;
		int data_weight = 0;		
		
		for(int i = 0; i < data_numbers; i++) {
			data_id = (int) DBMSSimulator.dataRand.nextUniform(0, DBMSSimulator.DATA_OBJECTS, false);
			data = dataLookup(data_id, db);
			data_weight = data.getData_weight();
			data.setData_weight(++data_weight);					
			transaction_data_set.add(data);
		}
		
		transaction = new Transaction(transaction_id, transaction_data_set);				
		transaction.setTr_type(transaction_type);		
		
		return transaction;
	}
	
	public Data dataLookup(int data_id, Database db) {
		return (db.getDb_dataMap().getData_items().get(data_id));				
	}
	
	//
	public void generateWorkload(String DIR_LOCATION) {		
		Workload workload = null;		
		WorkloadReplay workloadReplay = new WorkloadReplay();				
		Transaction transaction = null;
		int[] transaction_proportion_array = null;
		int transaction_id = 0;
		
		for(int i = 0; i < DBMSSimulator.SIMULATION_RUN_NUMBERS; i++) { // i = simulation round
			workload = new Workload(i, 5, this.database.getDb_id()); // 5 = 5 types of transactions
			transaction_proportion_array = generateTransactionProportion(workload.getWrl_transactionTypes(), DBMSSimulator.TRANSACTION_NUMS);
				
			for(int j = 0; j < workload.getWrl_transactionTypes(); j++) { // j = transaction type
				for(int k = 0; k < transaction_proportion_array[j]; k++) { // k = number of transactions in a specific j
					transaction = createTransaction(++transaction_id, j, j+2, this.database);
					transaction.generateTransactionCost(this.database);
					
					workload.getWrl_transactionMap().get(j).add(transaction);
					
				} // end--for(k)
			} // end--for(j)
			
			workloadReplay.getWrl_replayMap().put(i, workload);
		} // end--for(i)		
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
	public int[] generateTransactionProportion(int ranks, int elements) {		
		int propArray[] = new int[ranks];
		int rankArray[] = zipfLawDistributionGeneration(ranks, elements);
		
		// TR Rankings {T1, T2, T3, T4, T5} = {5, 4, 1, 2, 3}; 1 = Higher, 5 = Lower
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
	public int[] zipfLawDistributionGeneration(int ranks, int elements) {
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
	
	public void assignShadowHMetisDataId(Workload workload) {
		int total_dataItems = 0;
		int shadow_hmetis_data_id = 1;
		//int r = 0;
		
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				for(Data data : transaction.getTr_dataSet()) {
					if(!data.isData_hasShadowHMetisId()) {					
						data.setData_shadowHMetisId(shadow_hmetis_data_id);
						data.setData_hasShadowHMetisId(true);
						//System.out.println("@debug >> hData("+trData.toString()+") | hkey: "+trData.getData_shadow_hmetis_id());						
						
						++total_dataItems;
						++shadow_hmetis_data_id;					
						//System.out.println("@debug >> Total Data Tagged: "+total_dataItems);
					} else {
						//System.out.println("@debug >> *Repeated Data ("+trData.toString()+") | hkey: "+trData.getData_shadow_hmetis_id());
						//r++;
						//System.out.println("@debug >> *r(before) = "+r);
					}
				} // end -- for()-Data
			} // end -- for()-Transaction
		} // end -- for()-Transaction Types
		
		workload.setWrl_totalData(total_dataItems);
	}
	
	public void workloadEvaluation(Database db, Workload workload) {
		WorkloadSampling workloadSampling = workload.getWorkloadSampling();
		boolean emptyWorkload = false;
		
		// Re-evaluate Discarded Transactions
		if(workload.getWrl_simulationRound() > 1) {
			System.out.println("[ACT] Re-analysing previously discarded workload ...");
			emptyWorkload = workloadSampling.includeDiscardedWorkload(db, workload);
			System.out.println("[MSG] Total "+workloadSampling.getReseletedTransaction()
					+" newly distributed transactions are included from the previously discarded workload.");
			WorkloadGeneration.print(workload);
		}
		
		if(emptyWorkload) {
			System.out.println("[ALM] Empty workload !!! No transactions included !!!");
			workload.setWorkloadEmpty(true);
		}
		
		workload.removeDuplicates();
		
		// Workload Sampling
		System.out.println("[ACT] Sampling Workload ...");		
		workloadSampling.performSampling(workload);
		System.out.println("[MSG] Total "+workloadSampling.getDiscardedTransaction()
				+" non-distributed transactions are discarded from current workload and left for re-analysing for the next round.");
		WorkloadGeneration.print(workload);						
	}
	
	public void workloadRestoration(Database db, Workload workload) {
		WorkloadSampling workloadSampling = workload.getWorkloadSampling();		
		
		System.out.println("[ACT] Restoring previously discarded transactions from last simulation round ...");
		workloadSampling.restoreDiscardedWorkload(db, workload);
		System.out.println("[OUT] Total "+workload.getWrl_restoredTransactions()+" transactions have been restored from last simulation round."); 
		WorkloadGeneration.print(workload);
	}
	
	public static void print(Workload workload) {
		System.out.print("[MSG] Total "+workload.getWrl_totalTransactions()+" transactions of "+workload.getWrl_transactionTypes()
				+" types having a distribution of ");										
		workload.printWrl_transactionProp(workload.getWrl_transactionProportions());
		System.out.println(" are currently in the workload.");
	}
}