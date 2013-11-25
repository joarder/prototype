/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;
import jkamal.prototype.db.DatabaseServer;
import jkamal.prototype.main.DBMSSimulator;

public class WorkloadGenerator {	
	private Map<Integer, Workload> workload_map;	
	private WorkloadDataPreparer workloadDataPreparer;
	
	public WorkloadGenerator() {
		this.setWorkload_map(new TreeMap<Integer, Workload>());
		this.setWorkloadDataPreparer(new WorkloadDataPreparer());
	}	
	
	public Map<Integer, Workload> getWorkload_map() {
		return workload_map;
	}

	public void setWorkload_map(Map<Integer, Workload> workload_map) {
		this.workload_map = workload_map;
	}

	public WorkloadDataPreparer getWorkloadDataPreparer() {
		return workloadDataPreparer;
	}

	public void setWorkloadDataPreparer(WorkloadDataPreparer workloadDataPreparer) {
		this.workloadDataPreparer = workloadDataPreparer;
	}

	// Workload Initialisation
	public Workload workloadInitialisation(Database db, String workload_name, int workload_id) {
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
			case "YCSB":
				return(new Workload(workload_id, 6, db.getDb_id()));
			}		
		
		return null;
	}
	
	// Generates Workloads for the entire simulation
	public void generateWorkloads(DatabaseServer dbs, Database db) {
		Workload workload = null;
		TransactionClassifier workloadClassifier = new TransactionClassifier();
		int workload_id = 0;
		
		// Prepare Workload Data based on Zipfian Ranking with Normalised Cumulative Probability
		this.getWorkloadDataPreparer().prepareWorkloadData(db);
		
		while(workload_id != DBMSSimulator.SIMULATION_RUN_NUMBERS) {
			if(workload_id != 0) {
				workload = this.getWorkload_map().get(workload_id -1);
				
				// === Death Management === 	
				workload.setWrl_transactionDying((int) ((int) workload.getWrl_totalTransactions() * 0.5));				
				workload.setWrl_transactionDeathRate(0.5);	
				workload.setWrl_transactionDeathProp(transactionPropGen(workload.getWrl_transactionTypes(), 
						workload.getWrl_transactionDying()));
				
				// Reducing Old Workload Transactions			
				TransactionReducer transactionReducer = new TransactionReducer();
				transactionReducer.reduceTransaction(db, workload);
				
				// === Birth Management ===				
				workload.setWrl_transactionBorning((int) ((int) workload.getWrl_totalTransactions() * 0.5));
				workload.setWrl_transactionBirthRate(0.5);
				workload.setWrl_transactionBirthProp(transactionPropGen(workload.getWrl_transactionTypes(), 
						workload.getWrl_transactionBorning()));
				
				// Generating New Workload Transactions						
				TransactionGenerator transactionGenerator = new TransactionGenerator();
				transactionGenerator.generateTransaction(db, workload);				
			} else {
				// === Workload Generation Round 0 ===
				workload = this.workloadInitialisation(db, DBMSSimulator.WORKLOAD_TYPE, workload_id);
				workload.setWrl_initTotalTransactions(DBMSSimulator.TRANSACTION_NUMS);
			}			
			
			// Classify the Workload Transactions based on whether they are Distributed or not (Red/Orange/Green List)
			workloadClassifier.classifyTransactions(workload);
			
			Workload cloneWorkload = new Workload(workload);			
			this.getWorkload_map().put(workload_id, cloneWorkload);
			
			workload.updateWrl_workloadFileName(Integer.toString(workload.getWrl_id()));
			
			this.generateWorkloadFile(workload);
			this.generateFixFile(workload);
			
			++workload_id;
		}
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
		int rankArray[] = zipfLawDistributionGeneration(ranks, elements);
		
		// TR Rankings {T1, T2, T3, T4, T5} = {5, 4, 1, 2, 3}; 1 = Higher, 5 = Lower
		int begin = 0;
		int end = (rankArray.length - 1);
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
	
	// Generates Workload File for Hypergraph partitioning
	public void generateWorkloadFile(Workload workload) {
		File workloadFile = new File(DBMSSimulator.DIR_LOCATION+"\\"+workload.getWrl_workload_file());
		Data trData;
		int totalHyperEdges = workload.getWrl_totalTransactions();// + dbs.getDbs_nodes().size();
		int totalDataItems = workload.getWrl_totalDataObjects();
		int hasTransactionWeight = 1;
		int hasDataWeight = 1;						
		
		try {
			workloadFile.createNewFile();
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(workloadFile), "utf-8"));
				writer.write(totalHyperEdges+" "+totalDataItems+" "+hasTransactionWeight+""+hasDataWeight+"\n");
				
				for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
					for(Transaction transaction : entry.getValue()) {
						if(transaction.getTr_class() != "green") {
							writer.write(transaction.getTr_weight()+" ");
							
							Iterator<Data> data =  transaction.getTr_dataSet().iterator();
							while(data.hasNext()) {
								trData = data.next();
								//System.out.println("@debug >> fData ("+trData.toString()+") | hkey: "+trData.getData_shadow_hmetis_id());
								writer.write(Integer.toString(trData.getData_shadowHMetisId()));							
								
								if(data.hasNext())
									writer.write(" "); 
							} // end -- while() loop
							
							writer.write("\n");						
						} // end -- if()-Transaction Class
					} // end -- for()-Transaction
				} // end -- for()-Transaction-Types
				
				// Adding a single HyperEdge for each Node containing Data items within the Workload
				/*for(Entry<Integer, Set<Data>> entry : this.getDataNodeTable().entrySet()) {
					writer.write("1"+" "); // 1 = Node HyperEdge Weight will be always equals to 1
					Iterator<Data> itr_node = entry.getValue().iterator();
					while(itr_node.hasNext()) {
						writer.write(Integer.toString(itr_node.next().getData_shadowHMetisId()));
						
						if(itr_node.hasNext())
							writer.write(" ");
					} // end -- while() loop
					writer.write("\n");
				} // end -- for() loop*/

				// Writing Data Weight
				for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
					for(Transaction tr : entry.getValue()) {
						for(Data data : tr.getTr_dataSet()) {
							writer.write(Integer.toString(data.getData_weight()));
							writer.write("\n");
						}
					}
				}
				
			} catch(IOException e) {
				e.printStackTrace();
			}finally {
				writer.close();
			}
		} catch (IOException e) {		
			e.printStackTrace();
		}										
	}
	
	// Generates Fix Files (Determines whether a Data is movable from its current Partition or not) 
	// for Hypergraph partitioning
	public void generateFixFile(Workload workload) {
		File fixFile = new File(DBMSSimulator.DIR_LOCATION+"\\"+workload.getWrl_fixfile());
		
		try {
			fixFile.createNewFile();
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fixFile), "utf-8"));
				
				for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
					for(Transaction transaction : entry.getValue()) {			
						for(Data data : transaction.getTr_dataSet()) {						
							if(data.isData_isMoveable())
								writer.write(Integer.toString(data.getData_partitionId()));
							else
								writer.write(Integer.toString(-1));
							
							writer.write("\n");
						}
					}					
				}
			} catch(IOException e) {
				e.printStackTrace();
			}finally {
				writer.close();
			}
		} catch (IOException e) {		
			e.printStackTrace();
		}		
	}
	
	public static void print(Workload workload) {
		System.out.print("[MSG] Total "+workload.getWrl_totalTransactions()+" transactions of "+workload.getWrl_transactionTypes()
				+" types having a distribution of ");										
		workload.printWrl_transactionProp(workload.getWrl_transactionProportions());
		System.out.println(" are currently in the workload.");
	}	
}