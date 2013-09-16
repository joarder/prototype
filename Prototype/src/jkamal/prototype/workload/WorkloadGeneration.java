/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.Random;
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

	public Workload generate(DatabaseServer dbs, Database db, Workload workload, int transaction_nums, String DIR_LOCATION) {
		// Generating Random proportions for different Transaction types based on Workload type
		workload.setWrl_transactionProp(generateTransactionProp(workload, transaction_nums));

		TransactionGeneration trGen = new TransactionGeneration();
		trGen.generateTransaction(db, workload);		

		// Generating Workload's Data Partition and Node Distribution Details
		workload.generateDataPartitionTable();
		workload.generateDataNodeTable();
		
		// Generating Workload and FixFile for HyperGraph Partitioning			
		workload.generateWorkloadFile(dbs, DIR_LOCATION);
		workload.generateFixFile(DIR_LOCATION);		
		
		// Calculate the percentage of DT
		workload.calculateDTPercentage();

		return workload;
	}
	
	public Workload generateRepeatedWorkload(DatabaseServer dbs, Database db, String workload_name, int workload_transaction_nums, String DIR_LOCATION, 
			Workload workload, int workload_round, boolean workload_mode) {
		TransactionGeneration trGen = new TransactionGeneration();
		WorkloadSampling workloadSampling = new WorkloadSampling();
		Workload sampledWorkload = null;
		
		if(workload_round != 0) {
			workload.setWrl_transactionProp(generateRepeatedTransactionProp(workload, workload_mode));
			workload.setWrl_round(workload_round);
		} else {
			workload.setWrl_transactionProp(generateTransactionProp(workload, workload_transaction_nums));
			workload.setWrl_round(workload_round);
		}
				
		trGen.generateTransaction(db, workload);

		System.out.println("\n>> Performing workload sampling ...");		
		sampledWorkload = workloadSampling.performSampling(workload);				
		
		// Generating Workload's Data Partition and Node Distribution Details
		sampledWorkload.generateDataPartitionTable();
		sampledWorkload.generateDataNodeTable();		
		
		// Generating Workload and FixFile for HyperGraph Partitioning			
		sampledWorkload.generateWorkloadFile(dbs, DIR_LOCATION);
		sampledWorkload.generateFixFile(DIR_LOCATION);				
		
		// Calculate the percentage of DT
		workload.calculateDTPercentage();
		
		return sampledWorkload;
	}
	
	public static int randInt(int min, int max) {
	    Random random = new Random();
	    int randomNum = random.nextInt((max - min) + 1) + min;

	    return randomNum;
	}	
	
	public double[] generateTransactionProp(Workload workload, int nums) {
		int type = workload.getWrl_type();		
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
	
	public double[] generateRepeatedTransactionProp(Workload workload, boolean workload_mode) {
		// Generating Random proportions for different Transaction types based on Workload type
		Random random = new Random();
		double randomProp = 0.0;
		double newProp = 0.0;
		//double sumProp = 0.0;
		double[] oldProp = new double[workload.getWrl_transactionProp().length];
		System.arraycopy(workload.getWrl_transactionProp(), 0, oldProp, 0, workload.getWrl_transactionProp().length);
		
		System.out.print("@debug >> OldProp: ");
		workload.printWrl_transactionProp();
		
		for(int i = 0; i < oldProp.length; i++) {
			double prop = oldProp[i];
			if(prop > 1) {
				randomProp = Math.round(random.nextDouble()*100.0)/100.0;
				newProp = Math.round(prop*randomProp);
				
				if(workload_mode)
					workload.getWrl_transactionProp()[i] += newProp;
				else
					workload.getWrl_transactionProp()[i] -= newProp;
				
				//sumProp += newProp;
				//System.out.println("newProp: "+newProp+"| prop: "+prop+"| randomProp: "+randomProp);
			} else {
				// 
			}
		}				
				
		System.out.print("\n@debug >> NewProp:");
		workload.printWrl_transactionProp();
				
		double[] difference = new double[workload.getWrl_type()];
		
		if(workload_mode) {
			for(int i = 0; i < difference.length; i++) {
				difference[i] = workload.getWrl_transactionProp()[i] - oldProp[i];
				//System.out.println("* difference[i]: "+difference[i]+"| NewProp: "+workload.getWrl_transactionProp()[i]+"| oldProp: "+oldProp[i]);
			}
		} else {
			for(int i = 0; i < difference.length; i++) {
				difference[i] = oldProp[i] - workload.getWrl_transactionProp()[i];
				//System.out.println("^ difference[i]: "+difference[i]+"| oldProp: "+oldProp[i]+"NewProp: "+workload.getWrl_transactionProp()[i]);
			}
		}
			
		System.out.print("\n@debug >> Difference: {");
		for(int i = 0; i < difference.length; i++)
			System.out.print(difference[i]+", ");
		System.out.print("}");

		
		return difference;
	}
}