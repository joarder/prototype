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
import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;
import jkamal.prototype.db.Partition;
import jkamal.prototype.main.DBMSSimulator;

public class TransactionGeneration {
	private Map<Double, Map<Integer, Integer>> normalised_cumulative_probability_to_data_map;
	
	public TransactionGeneration(){
		normalised_cumulative_probability_to_data_map = new TreeMap<Double, Map<Integer, Integer>>();
	}
	
	// This function will generate the required number of Transactions for a specific Workload with a specific Database
	public void generateTransaction(Database db, Workload workload) {				
		ArrayList<Transaction> transactionList;
		Transaction transaction;		
		Set<Data> trDataSet;
		ArrayList<Integer> trDataList;
		Data data;
		Double rand = 0.0;
		int[] prop;
		int data_id = 0;
		int data_frequency = 0;
		int data_ranking = 0;
		int data_weight = 0;		
		
		//Selecting Transaction Prop
		if(workload.getWrl_simulationRound() != 0)
			prop = workload.getWrl_transactionBirthProp();
		else 
			prop = workload.getWrl_transactionProportions();		
				
		int tr_id = 0; 
		if(workload.getWrl_simulationRound() != 0)
			tr_id = workload.getWrl_globalTrId();
		
		// Creating a Random Object for randomly chosen Data items
		DBMSSimulator.random_data.reSeed(0);
		this.prepareRandomData(db);
		
		// i -- Transaction types
		for(int i = 0; i < workload.getWrl_transactionTypes(); i++) {	
			transactionList = new ArrayList<Transaction>();
			
			int typedTransactions = 0;
			// j -- a specific Transaction type in the Transaction proportion array
			for(int j = 0; j < prop[i]; j++) {
				++tr_id;
				trDataSet = new TreeSet<Data>();
				trDataList = new ArrayList<Integer>();
				
				// k -- required numbers of Data items based on Transaction type
				for(int k = 0; k < i+2; k++) {
					rand = Math.round(DBMSSimulator.random_data.nextUniform(0.0, 1.0, true) * 100.0)/100.0;
					data_id = this.getRandomData(rand);									
										
					if(trDataList.contains(data_id) && k > 0) {
						--k;
					} else {
						trDataList.add(data_id);
						
						data = db.search(data_id);
						
						data_frequency = data.getData_frequency();
						data_ranking = data.getData_ranking();
						data_weight = (data_frequency * data_ranking);
						
						data.setData_frequency(++data_frequency);	
						data.setData_weight(data_weight);
								
						data.getData_transaction_involved().add(tr_id);
						
						trDataSet.add(data);						
					}					
				} // end--k for() loop
																
				transaction = new Transaction(tr_id, trDataSet);				
				transaction.setTr_type(i+1);
				transaction.generateTransactionCost(db);
				workload.incWrl_totalTransaction();
				
				if(workload.getWrl_transactionMap().containsKey(i)) {
					workload.getWrl_transactionMap().get(i).add(transaction);
					++typedTransactions;
				} else
					transactionList.add(transaction);
			} // end--j for() loop
										
			if(workload.getWrl_simulationRound() == 0)
				workload.getWrl_transactionMap().put(i, transactionList);
			else
				workload.incWrl_transactionProportions(i, typedTransactions);			
		} // end--i for() loop
		
		workload.setWrl_globalTrId(tr_id);
	}	
	
	public void prepareRandomData(Database db) {		
		Map<Integer, Integer> unsortedMap = null;
		int d = 0;
		
		for(Partition partition : db.getDb_partitions()) {
			for(Data data : partition.getPartition_dataSet()) {
				d = 0;
				
				double key = data.getData_normalisedCumulativeProbability();
				
				if(!this.normalised_cumulative_probability_to_data_map.containsKey(key)) {
					unsortedMap = new TreeMap<Integer, Integer>();												
					unsortedMap.put(d, data.getData_id());		
					
					this.normalised_cumulative_probability_to_data_map.put(key, unsortedMap);
				} else {
					++d;
					this.normalised_cumulative_probability_to_data_map.get(key).put(d, data.getData_id());
				}
			}
		}			
	}
	
	public int getRandomData(double random_value) {
		int data_id = 0;
		
		if(this.normalised_cumulative_probability_to_data_map.containsKey(random_value)) {									
			Object[] values = this.normalised_cumulative_probability_to_data_map.get(random_value).values().toArray();
			
			if(values.length > 1)
				data_id = (int) values[DBMSSimulator.random_data.nextInt(0, (values.length - 1))];
			else
				data_id = (int) values[0];
		} else {		
			double smallest_key_encountered = 0.0;
			for(Entry<Double, Map<Integer, Integer>> entry : this.normalised_cumulative_probability_to_data_map.entrySet()) {
				if(random_value > entry.getKey()) {
					smallest_key_encountered = entry.getKey();					
				} else if(random_value >= 0.0 && random_value < entry.getKey()) {
					smallest_key_encountered = entry.getKey();
					break;
				} else {
					// skip to the next
				}
			}
			
			Object[] values = this.normalised_cumulative_probability_to_data_map.get(smallest_key_encountered).values().toArray();			
			if(values.length > 1)
				data_id = (int) values[DBMSSimulator.random_data.nextInt(0, (values.length - 1))];
			else
				data_id = (int) values[0];						
		}
		
		return data_id;
	}
}