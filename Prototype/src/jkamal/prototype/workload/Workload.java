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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;
import jkamal.prototype.db.DatabaseServer;
import jkamal.prototype.db.Partition;

public class Workload implements Comparable<Workload> {
	private int wrl_id;
	private String wrl_label;
	private int wrl_round;
	private int wrl_capture;
	private boolean wrl_mode;
	private int wrl_transactionVariant;
	private int wrl_database_id;
	private int wrl_globalTrId;
	private WorkloadSampling workloadSampling;
	private boolean workloadEmpty;	
	private double wrl_percentageVariation;
	
	private int wrl_transactionTypes; // Represents the number of Transaction types. e.g. for AuctionMark it is 10
	private int[] wrl_transactionProp;
	
	private Map<Integer, ArrayList<Transaction>> wrl_transactionMap;
	private int wrl_initTotalTransactions;
	private int wrl_totalTransaction;
	private int wrl_restoredTransactions;
	
	private int wrl_transactionBorning;
	private int wrl_transactionDying;
	private double wrl_transactionBirthRate;
	private double wrl_transactionDeathRate;
	private int[] wrl_transactionBirthProp;
	private int[] wrl_transactionDeathProp;		
	
	private int wrl_totalData;	
	private int wrl_intraNodeDataMovements;
	private int wrl_interNodeDataMovements;
	
	private Map<Integer, Set<Data>> dataPartitionTable;
	private Map<Integer, Set<Data>> dataNodeTable;
	
	private String wrl_workload_file = null;
	private String wrl_fixfile = null;
	
	private double wrl_dt_impact;
	private int wrl_dt_nums;
	private double wrl_percentage_dt;
	private double wrl_percentage_pdmv;
	private double wrl_percentage_ndmv;
	
	private boolean wrl_hasDataMoved;
	private String message = null;
	
	public Workload(int id, int trTypes, int db_id) {
		this.setWrl_id(id);
		this.setWrl_label("WL-"+Integer.toString(this.getWrl_id()));
		this.setWrl_round(0);
		this.setWrl_capture(0);
		this.setWrl_mode(true);
		this.setWrl_transactionVariant(0);
		this.setWrl_database_id(db_id);
		this.setWrl_globalTrId(0);
		this.setWorkloadSampling(new WorkloadSampling());
		this.setWorkloadEmpty(false);
		this.setWrl_percentageVariation(0.0d);
		
		this.setWrl_transactionTypes(trTypes);
		this.setWrl_transactionProp(new int[this.getWrl_transactionTypes()]);		
		this.setWrl_transactionMap(new TreeMap<Integer, ArrayList<Transaction>>());
		this.setWrl_initTotalTransactions(0);
		this.setWrl_totalTransaction(0);
		
		this.setWrl_transactionBorning(0);
		this.setWrl_transactionDying(0);
		this.setWrl_transactionBirthRate(0.0d);
		this.setWrl_transactionDeathRate(0.0d);
		this.setWrl_transactionBirthProp(new int[this.getWrl_transactionTypes()]);
		this.setWrl_transactionDeathProp(new int[this.getWrl_transactionTypes()]);		
		
		this.setWrl_totalData(0);
		this.setWrl_intraNodeDataMovements(0);
		this.setWrl_interNodeDataMovements(0);
		
		this.setWrl_workload_file("jk-workload.txt");
		this.setWrl_fixfile("jk-fixfile.txt");
		
		this.setWrl_dt_impact(0.0);
		this.setWrl_dt_nums(0);
		this.setWrl_percentage_dt(0.0);
		this.setWrl_percentage_intra_ndmv(0.0);
		this.setWrl_percentage_inter_ndmv(0.0);
		
		this.setWrl_hasDataMoved(false);
		this.setMessage(" (Initial Stage) ");
	}
	
	// Copy Constructor
	public Workload(Workload workload) {
		this.setWrl_id(workload.getWrl_id());
		this.setWrl_label(workload.getWrl_label());
		this.setWrl_round(workload.getWrl_simulationRound());
		this.setWrl_capture(workload.getWrl_capture());
		this.setWrl_mode(workload.isWrl_mode());
		this.setWrl_transactionVariant(workload.getWrl_transactionVariant());
		this.setWrl_database_id(workload.getWrl_database_id());
		this.setWrl_globalTrId(workload.getWrl_globalTrId());
		this.setWorkloadSampling(workload.getWorkloadSampling());
		this.setWorkloadEmpty(workload.isWorkloadEmpty());
		this.setWrl_percentageVariation(workload.getWrl_percentageVariation());
		
		this.setWrl_transactionTypes(workload.getWrl_transactionTypes());
		
		int[] cloneTransactionProp = new int[this.wrl_transactionTypes];		
		System.arraycopy(workload.getWrl_transactionProportions(), 0, cloneTransactionProp, 0, workload.getWrl_transactionProportions().length);
		this.setWrl_transactionProp(cloneTransactionProp);
		
		this.setWrl_transactionBorning(workload.getWrl_transactionBorning());
		this.setWrl_transactionDying(workload.getWrl_transactionDying());		
		this.setWrl_transactionBirthRate(workload.getWrl_transactionBirthRate());
		this.setWrl_transactionDeathRate(workload.getWrl_transactionDeathRate());
		
		int[] cloneTransactionBirthProp = new int[this.wrl_transactionTypes];		
		System.arraycopy(workload.getWrl_transactionBirthProp(), 0, cloneTransactionBirthProp, 0, workload.getWrl_transactionBirthProp().length);
		this.setWrl_transactionBirthProp(cloneTransactionBirthProp);				
		
		int[] cloneTransactionDeathProp = new int[this.wrl_transactionTypes];		
		System.arraycopy(workload.getWrl_transactionDeathProportions(), 0, cloneTransactionDeathProp, 0, workload.getWrl_transactionDeathProportions().length);
		this.setWrl_transactionDeathProp(cloneTransactionDeathProp);
		
		Map<Integer, ArrayList<Transaction>> cloneTransactionMap = new TreeMap<Integer, ArrayList<Transaction>>();
		int cloneTransactionType;		
		ArrayList<Transaction> cloneTransactionList;
		Transaction cloneTransaction;		
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			cloneTransactionType = entry.getKey();
			cloneTransactionList = new ArrayList<Transaction>();
			for(Transaction tr : entry.getValue()) {
				cloneTransaction = new Transaction(tr);
				cloneTransactionList.add(cloneTransaction);
			}
			cloneTransactionMap.put(cloneTransactionType, cloneTransactionList);
		}
		this.setWrl_transactionMap(cloneTransactionMap);
		this.setWrl_initTotalTransactions(workload.getWrl_initTotalTransactions());
		this.setWrl_totalTransaction(workload.getWrl_totalTransactions());
						
		this.setWrl_totalData(workload.getWrl_totalDataObjects());
		this.setWrl_intraNodeDataMovements(workload.getWrl_intraNodeDataMovements());
		this.setWrl_interNodeDataMovements(workload.getWrl_interNodeDataMovements());
				
		this.setWrl_workload_file(workload.getWrl_workload_file());
		this.setWrl_fixfile(workload.getWrl_fixfile());
		
		this.setWrl_dt_impact(workload.getWrl_DtImpact());
		this.setWrl_dt_nums(workload.getWrl_DtNumbers());
		this.setWrl_percentage_dt(workload.getWrl_percentageDistributedTransactions());
		this.setWrl_percentage_intra_ndmv(workload.getWrl_percentageIntraNodeDataMovement());
		this.setWrl_percentage_inter_ndmv(workload.getWrl_percentageInterNodeDataMovement());
		
		this.setWrl_hasDataMoved(workload.isWrl_hasDataMoved());
		this.setMessage(workload.getMessage());
	}

	public int getWrl_id() {
		return wrl_id;
	}

	public void setWrl_id(int id) {
		this.wrl_id = id;
	}

	public int getWrl_simulationRound() {
		return wrl_round;
	}

	public void setWrl_round(int wrl_round) {
		this.wrl_round = wrl_round;
	}

	public int getWrl_capture() {
		return wrl_capture;
	}

	public void setWrl_capture(int wrl_capture) {
		this.wrl_capture = wrl_capture;
	}

	public boolean isWrl_mode() {
		return wrl_mode;
	}

	public void setWrl_mode(boolean wrl_mode) {
		this.wrl_mode = wrl_mode;
	}

	public int getWrl_transactionVariant() {
		return wrl_transactionVariant;
	}

	public void setWrl_transactionVariant(int wrl_transactionVariant) {
		this.wrl_transactionVariant = wrl_transactionVariant;
	}

	public int getWrl_database_id() {
		return wrl_database_id;
	}

	public void setWrl_database_id(int wrl_database_id) {
		this.wrl_database_id = wrl_database_id;
	}

	public int getWrl_globalTrId() {
		return wrl_globalTrId;
	}

	public void setWrl_globalTrId(int wrl_globalTrId) {
		this.wrl_globalTrId = wrl_globalTrId;
	}

	public WorkloadSampling getWorkloadSampling() {
		return workloadSampling;
	}

	public void setWorkloadSampling(WorkloadSampling workloadSampling) {
		this.workloadSampling = workloadSampling;
	}

	public boolean isWorkloadEmpty() {
		return workloadEmpty;
	}

	public void setWorkloadEmpty(boolean workloadEmpty) {
		this.workloadEmpty = workloadEmpty;
	}

	public double getWrl_percentageVariation() {
		return wrl_percentageVariation;
	}

	public void setWrl_percentageVariation(double wrl_percentageVariation) {
		this.wrl_percentageVariation = wrl_percentageVariation;
	}

	public String getWrl_label() {
		return wrl_label;
	}

	public void setWrl_label(String label) {
		this.wrl_label = label;
	}

	public int getWrl_transactionTypes() {
		return wrl_transactionTypes;
	}

	public void setWrl_transactionTypes(int wrl_type) {
		this.wrl_transactionTypes = wrl_type;
	}

	public Map<Integer, ArrayList<Transaction>> getWrl_transactionMap() {
		return wrl_transactionMap;
	}

	public void setWrl_transactionMap(Map<Integer, ArrayList<Transaction>> wrl_transactionMap) {
		this.wrl_transactionMap = wrl_transactionMap;
	}

	public int[] getWrl_transactionProportions() {
		return wrl_transactionProp;
	}

	public void setWrl_transactionProp(int[] wrl_transactionProp) {
		this.wrl_transactionProp = wrl_transactionProp;
	}

	public void incWrl_transactionProportions(int pos) {
		++this.getWrl_transactionProportions()[pos];
	}
	
	public void incWrl_transactionProportions(int pos, int val) {
		int value = this.getWrl_transactionProportions()[pos];
		value += val;
		this.getWrl_transactionProportions()[pos] = value;
	}
	
	public void decWrl_transactionProportions(int pos) {		
		--this.getWrl_transactionProportions()[pos];		
	}
	
	public void decWrl_transactionProportions(int pos, int val) {
		int value = this.getWrl_transactionProportions()[pos];
		value -= val;
		this.getWrl_transactionProportions()[pos] = value;
	}

	public int getWrl_transactionBorning() {
		return wrl_transactionBorning;
	}

	public void setWrl_transactionBorning(int wrl_transactionBorning) {
		this.wrl_transactionBorning = wrl_transactionBorning;
	}

	public int getWrl_transactionDying() {
		return wrl_transactionDying;
	}

	public void setWrl_transactionDying(int wrl_transactionDying) {
		this.wrl_transactionDying = wrl_transactionDying;
	}

	public double getWrl_transactionBirthRate() {
		return wrl_transactionBirthRate;
	}

	public void setWrl_transactionBirthRate(double wrl_transactionBirthRate) {
		this.wrl_transactionBirthRate = wrl_transactionBirthRate;
	}

	public double getWrl_transactionDeathRate() {
		return wrl_transactionDeathRate;
	}

	public void setWrl_transactionDeathRate(double wrl_transactionDeathRate) {
		this.wrl_transactionDeathRate = wrl_transactionDeathRate;
	}

	public int[] getWrl_transactionBirthProp() {
		return wrl_transactionBirthProp;
	}

	public void setWrl_transactionBirthProp(int[] wrl_transactionBirthProp) {
		this.wrl_transactionBirthProp = wrl_transactionBirthProp;
	}

	public int[] getWrl_transactionDeathProportions() {
		return wrl_transactionDeathProp;
	}

	public void setWrl_transactionDeathProp(int[] wrl_transactionDeathProp) {
		this.wrl_transactionDeathProp = wrl_transactionDeathProp;
	}

	public int getWrl_totalTransactions() {
		return wrl_totalTransaction;
	}

	public void setWrl_totalTransaction(int wrl_totalTransaction) {
		this.wrl_totalTransaction = wrl_totalTransaction;
	}
	
	public int getWrl_initTotalTransactions() {
		return wrl_initTotalTransactions;
	}

	public void setWrl_initTotalTransactions(int wrl_initTotalTransactions) {
		this.wrl_initTotalTransactions = wrl_initTotalTransactions;
	}

	public void incWrl_totalTransaction() {
		int totalTransaction = this.getWrl_totalTransactions();		
		++totalTransaction;
		this.setWrl_totalTransaction(totalTransaction);
	}
	
	public void decWrl_totalTransactions() {
		int totalTransaction = this.getWrl_totalTransactions();		
		--totalTransaction;
		this.setWrl_totalTransaction(totalTransaction);
	}

	public int getWrl_totalDataObjects() {
		return wrl_totalData;
	}

	public void setWrl_totalData(int wrl_totalData) {
		this.wrl_totalData = wrl_totalData;
	}

	public int getWrl_intraNodeDataMovements() {
		return wrl_intraNodeDataMovements;
	}

	public void setWrl_intraNodeDataMovements(int wrl_intraNodeDataMovements) {
		this.wrl_intraNodeDataMovements = wrl_intraNodeDataMovements;
	}

	public int getWrl_interNodeDataMovements() {
		return wrl_interNodeDataMovements;
	}

	public void setWrl_interNodeDataMovements(int wrl_interNodeDataMovements) {
		this.wrl_interNodeDataMovements = wrl_interNodeDataMovements;
	}

	public String getWrl_workload_file() {
		return wrl_workload_file;
	}

	public void setWrl_workload_file(String wrl_workload_file) {
		this.wrl_workload_file = wrl_workload_file;
	}

	public String getWrl_fixfile() {
		return wrl_fixfile;
	}

	public void setWrl_fixfile(String wrl_fixfile) {
		this.wrl_fixfile = wrl_fixfile;
	}
	
	public double getWrl_DtImpact() {
		return wrl_dt_impact;
	}

	public void setWrl_dt_impact(double wrl_dt_impact) {
		this.wrl_dt_impact = wrl_dt_impact;
	}

	public Map<Integer, Set<Data>> getDataPartitionTable() {
		return dataPartitionTable;
	}

	public void setWrl_dataPartitionTable(Map<Integer, Set<Data>> dataPartitionTable) {
		this.dataPartitionTable = dataPartitionTable;
	}

	public Map<Integer, Set<Data>> getDataNodeTable() {
		return dataNodeTable;
	}

	public void setWrl_dataNodeTable(Map<Integer, Set<Data>> dataNodeTable) {
		this.dataNodeTable = dataNodeTable;
	}
	
	public Transaction findWrl_transaction(int tr_id) {		
		for(Entry<Integer, ArrayList<Transaction>> entry : this.getWrl_transactionMap().entrySet()) {			
			for(Transaction transaction : entry.getValue()) {
				if(transaction.getTr_id() == tr_id)
					return transaction;
			}
		}
		
		return null;
	}
	
	public Data findWrl_transactionData(Transaction transaction, int data_id) {
		Data data;
		Iterator<Data> iterator = transaction.getTr_dataSet().iterator();
		while(iterator.hasNext()) {
			data = iterator.next();
			if(data.getData_id() == data_id)
				return data;
		}		
		
		return null;
	}

	public void generateWorkloadFile(DatabaseServer dbs, Workload workload, String dir) {
		File workloadFile = new File(dir+"\\"+this.getWrl_workload_file());
		Data trData;
		int totalHyperEdges = this.getWrl_totalTransactions() + dbs.getDbs_nodes().size();
		int totalDataItems = this.getWrl_totalDataObjects();
		int hasTransactionWeight = 1;
		int hasDataWeight = 1;						
		
		try {
			workloadFile.createNewFile();
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(workloadFile), "utf-8"));
				writer.write(totalHyperEdges+" "+totalDataItems+" "+hasTransactionWeight+""+hasDataWeight+"\n");
				
				for(Entry<Integer, ArrayList<Transaction>> entry : this.getWrl_transactionMap().entrySet()) {
					for(Transaction transaction : entry.getValue()) {
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
					} // end -- for()-Transaction
				} // end -- for()-Transaction-Types
				
				// Adding a single HyperEdge for each Node containing Data items within the Workload
				for(Entry<Integer, Set<Data>> entry : this.getDataNodeTable().entrySet()) {
					writer.write("1"+" "); // 1 = Node HyperEdge Weight will be always equals to 1
					Iterator<Data> itr_node = entry.getValue().iterator();
					while(itr_node.hasNext()) {
						writer.write(Integer.toString(itr_node.next().getData_shadowHMetisId()));
						
						if(itr_node.hasNext())
							writer.write(" ");
					} // end -- while() loop
					writer.write("\n");
				} // end -- for() loop

				// Writing Data Weight
				for(Entry<Integer, ArrayList<Transaction>> entry : this.getWrl_transactionMap().entrySet()) {
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
	
	public void generateFixFile(String dir) {
		File fixFile = new File(dir+"\\"+this.getWrl_fixfile());
		
		try {
			fixFile.createNewFile();
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fixFile), "utf-8"));
				
				for(Entry<Integer, ArrayList<Transaction>> entry : this.getWrl_transactionMap().entrySet()) {
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

	public int getWrl_DtNumbers() {
		return wrl_dt_nums;
	}

	public void setWrl_dt_nums(int wrl_dt_nums) {
		this.wrl_dt_nums = wrl_dt_nums;
	}

	public double getWrl_percentageDistributedTransactions() {
		return wrl_percentage_dt;
	}

	public void setWrl_percentage_dt(double wrl_percentage_dt) {
		this.wrl_percentage_dt = wrl_percentage_dt;
	}

	public double getWrl_percentageIntraNodeDataMovement() {
		return wrl_percentage_pdmv;
	}

	public void setWrl_percentage_intra_ndmv(double wrl_percentage_pdmv) {
		this.wrl_percentage_pdmv = wrl_percentage_pdmv;
	}
		
	public double getWrl_percentageInterNodeDataMovement() {
		return wrl_percentage_ndmv;
	}

	public void setWrl_percentage_inter_ndmv(double wrl_percentage_ndmv) {
		this.wrl_percentage_ndmv = wrl_percentage_ndmv;
	}

	public boolean isWrl_hasDataMoved() {
		return wrl_hasDataMoved;
	}

	public void setWrl_hasDataMoved(boolean wrl_hasDataMoved) {
		this.wrl_hasDataMoved = wrl_hasDataMoved;
	}

	public int getWrl_restoredTransactions() {
		return wrl_restoredTransactions;
	}

	public void setWrl_restoredTransactions(int wrl_restoredTransactions) {
		this.wrl_restoredTransactions = wrl_restoredTransactions;
	}

	// Calculate DT Impacts for the Workload
	public void calculateDTImapct() {
		int total_impact = 0;
		int total_trFreq = 0;
		
		for(Entry<Integer, ArrayList<Transaction>> entry : this.getWrl_transactionMap().entrySet()) 
			for(Transaction transaction : entry.getValue()) {
				total_impact += transaction.getTr_dtCost() * transaction.getTr_weight();
				total_trFreq += transaction.getTr_weight();
			}
				
		//double dt_impact = (double) total_impact/this.getWrl_totalTransaction();
		double dt_impact = (double) total_impact/total_trFreq;
		dt_impact = Math.round(dt_impact * 100.0)/100.0;
		this.setWrl_dt_impact(dt_impact);
	}

	// Calculate the percentage of Distributed Transactions within the Workload (before and after the Data movements)
	public void calculateDTPercentage() {
		int counts = 0; 
		
		for(Entry<Integer, ArrayList<Transaction>> entry : this.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				if(transaction.getTr_dtCost() >= 1)
					++counts;			
			} // end -- for()-Transaction		
		} // end -- for()-
				
		double percentage = ((double)counts/(double)this.getWrl_totalTransactions())*100.0;
		percentage = Math.round(percentage * 100.0)/100.0;
		this.setWrl_dt_nums(counts);
		this.setWrl_percentage_dt(percentage);	
	}
	
	// Calculate the percentage of Data movements within the Workload (after running Strategy-1 and 2)
	public void calculateIntraNodeDataMovementPercentage(int intra_node_movements) {
		double percentage = ((double)intra_node_movements/this.getWrl_totalDataObjects())*100.0;
		percentage = Math.round(percentage*100.0)/100.0;
		this.setWrl_percentage_intra_ndmv(percentage);
	}
	
	public void calculateInterNodeDataMovementPercentage(int inter_node_movements) {
		int counts = this.getWrl_totalDataObjects();		
		double percentage = ((double)inter_node_movements/counts)*100.0;
		percentage = Math.round(percentage*100.0)/100.0;
		this.setWrl_percentage_inter_ndmv(percentage);
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	// Generates Workload Data Partition Table
	public void generateDataPartitionTable() {
		this.setWrl_dataPartitionTable(new TreeMap<Integer, Set<Data>>());
		TreeSet<Data> dataSet;
				
		for(Entry<Integer, ArrayList<Transaction>> entry : this.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				for(Data data : transaction.getTr_dataSet()) {
					if(!data.isData_isRoaming()) {			
						if(this.getDataPartitionTable().containsKey(data.getData_homePartitionId())) {
							this.getDataPartitionTable().get(data.getData_homePartitionId()).add(data);
						} else {
							dataSet = new TreeSet<Data>();
							dataSet.add(data);
							this.getDataPartitionTable().put(data.getData_homePartitionId(), dataSet);
						}
					} else {
						if(this.getDataPartitionTable().containsKey(data.getData_partitionId())) {
							this.getDataPartitionTable().get(data.getData_partitionId()).add(data);
						} else {
							dataSet = new TreeSet<Data>();
							dataSet.add(data);
							this.getDataPartitionTable().put(data.getData_partitionId(), dataSet);
						}
					}
				} // end -- for()-Data
			} // end -- for()-Transaction
		} // end -- for()-Transaction Types
	}
	
	// Generates Workload Data Node Table
	public void generateDataNodeTable() {		
		this.setWrl_dataNodeTable(new TreeMap<Integer, Set<Data>>());
		Set<Data> dataSet;
		
		for(Entry<Integer, ArrayList<Transaction>> entry : this.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {	
				for(Data data : transaction.getTr_dataSet()) {
					if(!data.isData_isRoaming()) {
						if(this.getDataNodeTable().containsKey(data.getData_homeNodeId())) {
							this.getDataNodeTable().get(data.getData_homeNodeId()).add(data);
						} else {
							dataSet = new TreeSet<Data>();
							dataSet.add(data);
							this.getDataNodeTable().put(data.getData_homeNodeId(), dataSet);
						}
					} else {
						if(this.getDataNodeTable().containsKey(data.getData_nodeId())) {
							this.getDataNodeTable().get(data.getData_nodeId()).add(data);
						} else {
							dataSet = new TreeSet<Data>();
							dataSet.add(data);
							this.getDataNodeTable().put(data.getData_nodeId(), dataSet);
						}
					}
				} // end -- for()-Data
			} // end -- for()-Transaction
		} // end -- for()-Transaction Types
	}
	
	public void printWrl_transactionProp(int[] array) {
		int size = array.length;
		
		System.out.print("{");
		for(int val : array) {			
			System.out.print(Integer.toString(val));
			
			--size;			
			if(size != 0)
				System.out.print(", ");
		}		
		System.out.print("}");		
	}
	
	public void printDataPartitionTable() {
		int comma = -1;
		int roaming_data = 0;
		
		// Workload's Data - Partition Details		
		System.out.println("      # Workload's Data-Partition Details");						
		for(Entry<Integer, Set<Data>> entry : this.getDataPartitionTable().entrySet()) {
			System.out.print("        P"+entry.getKey()+"["+entry.getValue().size()+"]");
			
			//System.out.print(": {");						
			roaming_data = 0;
			comma = entry.getValue().size();
			for(Data data : entry.getValue()) {
				//System.out.print(data.toString());
				if(data.isData_isRoaming())
					++roaming_data;
				
				if(comma != 1)
					//System.out.print(", ");
			
				--comma;						
			} // end -- for() loop
			
			//System.out.print("} || ");
			System.out.print(" Roaming["+roaming_data+"]\n");
		} // end -- for() loop		
	}
	
	public void printDataNodeTable() {
		int comma = -1;
		int roaming_data = 0;
		
		// Workload's Data - Node Details
		System.out.println("      # Workload's Data-Node Details");						
		for(Entry<Integer, Set<Data>> entry : this.getDataNodeTable().entrySet()) {
			System.out.print("        N"+entry.getKey()+"["+entry.getValue().size()+"] ");
			
			//System.out.print(": {");			
			roaming_data = 0;
			comma = entry.getValue().size();
			for(Data data : entry.getValue()) {
				//System.out.print(data.toString());
				if(data.isData_isRoaming())
					++roaming_data;
				
				if(comma != 1)
					//System.out.print(", ");
			
				--comma;						
			} // end -- for() loop
			
			//System.out.print("} || ");
			System.out.print(" Roaming["+roaming_data+"]\n");
		} // end -- for() loop		
	}
	
	public void printPartitionTable(Database db) {		
		// Partition Table Details
		System.out.println("      # Database Partition Table Details");
		ArrayList<Integer> overloadedPartition = new ArrayList<Integer>();
		
		int comma = -1;
		for(Entry<Integer, Set<Partition>> entry : db.getDb_partitionTable().getPartition_table().entrySet()) {
			System.out.println("      --N"+entry.getKey());//+"{");
						
			for(Partition partition : entry.getValue()) {				
				System.out.println("       -"+partition.toString());				
				//System.out.print("       ");	
				//partition.printContents();
				
				if(partition.isPartition_overloaded())
					overloadedPartition.add(partition.getPartition_id());															
			}			
		}
		
		if(overloadedPartition.size() != 0) {
			System.out.println();
			System.out.print("[ALM] Overloaded Partition: ");
			comma = overloadedPartition.size();
			for(Integer pid : overloadedPartition) {
				System.out.print("P"+pid);
				
				if(comma != 1)
					System.out.print(", ");
			
				--comma;
			}
			System.out.print("\n");
		}			
	}
	
	public void print(Database db) {				
		System.out.println("[OUT] "+this.getMessage()+" Workload Details for Simulation Round-"+this.getWrl_simulationRound());
		System.out.print("      "+this.toString() +" having a distribution of ");				
		this.printWrl_transactionProp(this.getWrl_transactionProportions());
				
		System.out.println("\n      -----------------------------------------------------------------------------------------------------------------");
		for(Entry<Integer, ArrayList<Transaction>> entry : this.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				transaction.generateTransactionCost(db);
				System.out.print("     ");
				//transaction.print();
			} // end -- for()-Transaction
		} // end -- for()-Transaction Types						
				
		this.calculateDTPercentage();	
		this.calculateDTImapct();
		
		System.out.println();
		System.out.println("      # Distributed Transactions: "+this.getWrl_DtNumbers()
				+" ("+this.getWrl_percentageDistributedTransactions()+"% of " 
				+"Total "+this.getWrl_totalTransactions()+" Workload Transactions)");
		System.out.println("      # Impact of Distributed Transactions: "+this.getWrl_DtImpact()
				+" (for a particular workload round)");
		
		if(this.isWrl_hasDataMoved()) {
			System.out.println("      # Intra-Node Data Movements (Avoided): "+this.getWrl_intraNodeDataMovements()
					+" ("+this.getWrl_percentageIntraNodeDataMovement()+"% of "
					+"Total "+this.getWrl_totalDataObjects()+" Workload Data)");
			System.out.println("      # Inter-Node Data Movements (Performed): "+this.getWrl_interNodeDataMovements()
					+" ("+this.getWrl_percentageInterNodeDataMovement()+"% of "
					+"Total "+this.getWrl_totalDataObjects()+" Workload Data)");
		}
		
		this.printPartitionTable(db);
		//this.printDataPartitionTable();
		//this.printDataNodeTable();
		System.out.println("      -----------------------------------------------------------------------------------------------------------------");
	}
	
	public void removeDuplicates() {
		List<Transaction> duplicates = new ArrayList<Transaction>();
		int duplicate = 0;
		int trType = 0;
				
		System.out.println("[ACT] Searching for duplicates in the workload ..."); 
		for(Entry<Integer, ArrayList<Transaction>> entry : this.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
				int tr_id = transaction.getTr_id();
				
				if(this.getWrl_transactionMap().entrySet().contains(tr_id)) {
					duplicates.add(transaction);
					++duplicate;
				}
			}
		}
		
		System.out.println("[OUT] Total "+duplicate+" were transactions found in the workload !!!");
		
		int removed = 0;
		for(Transaction transaction : duplicates) {
			trType = transaction.getTr_type();
			this.getWrl_transactionMap().get(trType).remove(transaction);
			++removed;
		}
		
		System.out.println("[OUT] Total "+removed+" duplicate transactions have been removed from the workload.");
	}
	
	@Override
	public String toString() {	
		return (this.wrl_label+" ["+this.getWrl_totalTransactions()+" Transactions (containing "+this.getWrl_totalDataObjects()
				+" Data) ");//+this.getWrl_transactionTypes());//+" types having a distribution of ");//+this.printWrl_transactionProp()+"]");
	}

	@Override
	public int compareTo(Workload workload) {
		int compare = ((int)this.wrl_id < (int)workload.wrl_id) ? -1: ((int)this.wrl_id > (int)workload.wrl_id) ? 1:0;
		return compare;
	}
}