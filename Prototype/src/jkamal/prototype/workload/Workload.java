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
	private double[] wrl_transactionProp;
	private double[] wrl_transactionVarProp;
	private Map<Integer, ArrayList<Transaction>> wrl_transactionMap;
	private int wrl_initTotalTransactions;
	private int wrl_totalTransaction;
		
	private int wrl_totalData;	
	private int wrl_interPartitionDataMovements;
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
		this.setWrl_transactionProp(new double[this.getWrl_transactionTypes()]);
		this.setWrl_transactionVarProp(new double[this.getWrl_transactionTypes()]);
		this.setWrl_transactionMap(new TreeMap<Integer, ArrayList<Transaction>>());
		this.setWrl_initTotalTransactions(0);
		this.setWrl_totalTransaction(0);
		
		this.setWrl_totalData(0);
		this.setWrl_interPartitionDataMovements(0);
		this.setWrl_interNodeDataMovements(0);
		
		this.setWrl_workload_file("jk-workload.txt");
		this.setWrl_fixfile("jk-fixfile.txt");
		
		this.setWrl_dt_impact(0.0);
		this.setWrl_dt_nums(0);
		this.setWrl_percentage_dt(0.0);
		this.setWrl_percentage_pdmv(0.0);
		this.setWrl_percentage_ndmv(0.0);
		
		this.setWrl_hasDataMoved(false);
		this.setMessage(" (Initial Stage) ");
	}
	
	// Copy Constructor
	public Workload(Workload workload) {
		this.setWrl_id(workload.getWrl_id());
		this.setWrl_label(workload.getWrl_label());
		this.setWrl_round(workload.getWrl_round());
		this.setWrl_capture(workload.getWrl_capture());
		this.setWrl_mode(workload.isWrl_mode());
		this.setWrl_transactionVariant(workload.getWrl_transactionVariant());
		this.setWrl_database_id(workload.getWrl_database_id());
		this.setWrl_globalTrId(workload.getWrl_globalTrId());
		this.setWorkloadSampling(workload.getWorkloadSampling());
		this.setWorkloadEmpty(workload.isWorkloadEmpty());
		this.setWrl_percentageVariation(workload.getWrl_percentageVariation());
		
		this.setWrl_transactionTypes(workload.getWrl_transactionTypes());
		
		double[] cloneTransactionProp = new double[this.wrl_transactionTypes];		
		System.arraycopy(workload.getWrl_transactionProp(), 0, cloneTransactionProp, 0, workload.getWrl_transactionProp().length);
		this.setWrl_transactionProp(cloneTransactionProp);
		
		double[] cloneTransactionVarProp = new double[this.wrl_transactionTypes];		
		System.arraycopy(workload.getWrl_transactionVarProp(), 0, cloneTransactionVarProp, 0, workload.getWrl_transactionVarProp().length);
		this.setWrl_transactionVarProp(cloneTransactionVarProp);
		
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
		this.setWrl_totalTransaction(workload.getWrl_totalTransaction());
						
		this.setWrl_totalData(workload.getWrl_totalData());
		this.setWrl_interPartitionDataMovements(workload.getWrl_interPartitionDataMovements());
		this.setWrl_interNodeDataMovements(workload.getWrl_interNodeDataMovements());
				
		this.setWrl_workload_file(workload.getWrl_workload_file());
		this.setWrl_fixfile(workload.getWrl_fixfile());
		
		this.setWrl_dt_impact(workload.getWrl_dt_impact());
		this.setWrl_dt_nums(workload.getWrl_dt_nums());
		this.setWrl_percentage_dt(workload.getWrl_percentage_dt());
		this.setWrl_percentage_pdmv(workload.getWrl_percentage_pdmv());
		this.setWrl_percentage_ndmv(workload.getWrl_percentage_ndmv());
		
		this.setWrl_hasDataMoved(workload.isWrl_hasDataMoved());
		this.setMessage(workload.getMessage());
	}

	public int getWrl_id() {
		return wrl_id;
	}

	public void setWrl_id(int id) {
		this.wrl_id = id;
	}

	public int getWrl_round() {
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

	public double[] getWrl_transactionProp() {
		return wrl_transactionProp;
	}

	public void setWrl_transactionProp(double[] wrl_transactionProp) {
		this.wrl_transactionProp = wrl_transactionProp;
	}
	
	public double[] getWrl_transactionVarProp() {
		return wrl_transactionVarProp;
	}

	public void setWrl_transactionVarProp(double[] wrl_transactionVarProp) {
		this.wrl_transactionVarProp = wrl_transactionVarProp;
	}

	public void incWrl_transactionPropVal(int pos) {
		++this.getWrl_transactionProp()[pos];
	}
	
	public void incWrl_transactionPropVal(int pos, int val) {
		Double value = this.getWrl_transactionProp()[pos];
		value += val;
		this.getWrl_transactionProp()[pos] = value;
	}
	
	public void decWrl_transactionPropVal(int pos) {		
		--this.getWrl_transactionProp()[pos];		
	}
	
	public void decWrl_transactionPropVal(int pos, int val) {
		Double value = this.getWrl_transactionProp()[pos];
		value -= val;
		this.getWrl_transactionProp()[pos] = value;
	}

	public int getWrl_totalTransaction() {
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
		int totalTransaction = this.getWrl_totalTransaction();		
		++totalTransaction;
		this.setWrl_totalTransaction(totalTransaction);
	}
	
	public void decWrl_totalTransaction() {
		int totalTransaction = this.getWrl_totalTransaction();		
		--totalTransaction;
		this.setWrl_totalTransaction(totalTransaction);
	}

	public int getWrl_totalData() {
		return wrl_totalData;
	}

	public void setWrl_totalData(int wrl_totalData) {
		this.wrl_totalData = wrl_totalData;
	}

	public int getWrl_interPartitionDataMovements() {
		return wrl_interPartitionDataMovements;
	}

	public void setWrl_interPartitionDataMovements(int wrl_interPartitionDataMovements) {
		this.wrl_interPartitionDataMovements = wrl_interPartitionDataMovements;
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
	
	public double getWrl_dt_impact() {
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
		int totalHyperEdges = this.getWrl_totalTransaction() + dbs.getDbs_nodes().size();
		int totalDataItems = this.getWrl_totalData();
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
							writer.write(Integer.toString(trData.getData_shadow_hmetis_id()));							
							
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
						writer.write(Integer.toString(itr_node.next().getData_shadow_hmetis_id()));
						
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
								writer.write(Integer.toString(data.getData_partition_id()));
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

	public int getWrl_dt_nums() {
		return wrl_dt_nums;
	}

	public void setWrl_dt_nums(int wrl_dt_nums) {
		this.wrl_dt_nums = wrl_dt_nums;
	}

	public double getWrl_percentage_dt() {
		return wrl_percentage_dt;
	}

	public void setWrl_percentage_dt(double wrl_percentage_dt) {
		this.wrl_percentage_dt = wrl_percentage_dt;
	}

	public double getWrl_percentage_pdmv() {
		return wrl_percentage_pdmv;
	}

	public void setWrl_percentage_pdmv(double wrl_percentage_pdmv) {
		this.wrl_percentage_pdmv = wrl_percentage_pdmv;
	}
		
	public double getWrl_percentage_ndmv() {
		return wrl_percentage_ndmv;
	}

	public void setWrl_percentage_ndmv(double wrl_percentage_ndmv) {
		this.wrl_percentage_ndmv = wrl_percentage_ndmv;
	}

	public boolean isWrl_hasDataMoved() {
		return wrl_hasDataMoved;
	}

	public void setWrl_hasDataMoved(boolean wrl_hasDataMoved) {
		this.wrl_hasDataMoved = wrl_hasDataMoved;
	}

	// Calculate DT Impacts for the Workload
	public void calculateDTImapct() {
		int total_impact = 0;
		int total_trFreq = 0;
		
		for(Entry<Integer, ArrayList<Transaction>> entry : this.getWrl_transactionMap().entrySet()) 
			for(Transaction transaction : entry.getValue()) {
				total_impact += transaction.getTr_dtCost()*transaction.getTr_weight();
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
				
		double percentage = ((double)counts/(double)this.getWrl_totalTransaction())*100.0;
		percentage = Math.round(percentage * 100.0)/100.0;
		this.setWrl_dt_nums(counts);
		this.setWrl_percentage_dt(percentage);	
	}
	
	// Calculate the percentage of Data movements within the Workload (after running Strategy-1 and 2)
	public void calculatePDMVPercentage(int inter_partition_movements) {
		double percentage = ((double)inter_partition_movements/this.getWrl_totalData())*100.0;
		percentage = Math.round(percentage*100.0)/100.0;
		this.setWrl_percentage_pdmv(percentage);
	}
	
	public void calculateNDMVPercentage(int inter_node_movements) {
		int counts = this.getWrl_totalData();		
		double percentage = ((double)inter_node_movements/counts)*100.0;
		percentage = Math.round(percentage*100.0)/100.0;
		this.setWrl_percentage_ndmv(percentage);
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
					if(!data.isData_isPartitionRoaming()) {			
						if(this.getDataPartitionTable().containsKey(data.getData_partition_id())) {
							this.getDataPartitionTable().get(data.getData_partition_id()).add(data);
						} else {
							dataSet = new TreeSet<Data>();
							dataSet.add(data);
							this.getDataPartitionTable().put(data.getData_partition_id(), dataSet);
						}
					} else {
						if(this.getDataPartitionTable().containsKey(data.getData_roaming_partition_id())) {
							this.getDataPartitionTable().get(data.getData_roaming_partition_id()).add(data);
						} else {
							dataSet = new TreeSet<Data>();
							dataSet.add(data);
							this.getDataPartitionTable().put(data.getData_roaming_partition_id(), dataSet);
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
					if(!data.isData_isNodeRoaming()) {
						if(this.getDataNodeTable().containsKey(data.getData_node_id())) {
							this.getDataNodeTable().get(data.getData_node_id()).add(data);
						} else {
							dataSet = new TreeSet<Data>();
							dataSet.add(data);
							this.getDataNodeTable().put(data.getData_node_id(), dataSet);
						}
					} else {
						if(this.getDataNodeTable().containsKey(data.getData_roaming_node_id())) {
							this.getDataNodeTable().get(data.getData_roaming_node_id()).add(data);
						} else {
							dataSet = new TreeSet<Data>();
							dataSet.add(data);
							this.getDataNodeTable().put(data.getData_roaming_node_id(), dataSet);
						}
					}
				} // end -- for()-Data
			} // end -- for()-Transaction
		} // end -- for()-Transaction Types
	}
	
	public void printWrl_transactionProp() {
		int size = this.getWrl_transactionProp().length;
		
		System.out.print("{");
		for(double prop : this.getWrl_transactionProp()) {			
			//System.out.print(prop+"|");
			System.out.print(Integer.toString((int)Math.round(prop)));
			
			--size;			
			if(size != 0)
				System.out.print(", ");
		}		
		System.out.print("}");
	}
		
	public void printWrl_transactionVarProp() {
		int size = this.getWrl_transactionVarProp().length;
		
		System.out.print("{");
		for(double prop : this.getWrl_transactionVarProp()) {
			//System.out.print(prop+"|");
			System.out.print(Integer.toString((int)Math.round(prop)));
			
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
				if(data.isData_isPartitionRoaming())
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
				if(data.isData_isNodeRoaming())
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
		for(Entry<Integer, Set<Partition>> entry : db.getDb_partition_table().getPartition_table().entrySet()) {
			System.out.print("        N"+entry.getKey()+"{");
			
			comma = entry.getValue().size();
			for(Partition partition : entry.getValue()) {				
				System.out.print(partition.toString());
				
				if(partition.isPartition_overloaded())
					overloadedPartition.add(partition.getPartition_id());
				
				if(comma != 1)
					System.out.print(", ");
			
				--comma;
			}
			
			System.out.print("}\n");			
		}
		
		if(overloadedPartition.size() != 0) {
			System.out.print("        [ALM] Overloaded Partition: ");
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
		System.out.println("[OUT] Capture-"+this.getWrl_capture()+" | Round-"+this.getWrl_round()+" ("+this.getMessage()+") :: Workload Details");
		System.out.print("      "+this.toString() +" having a distribution of ");				
		this.printWrl_transactionProp();
				
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
		System.out.println("      # Distributed Transactions: "+this.getWrl_dt_nums()
				+" ("+this.getWrl_percentage_dt()+"% of " 
				+"Total "+this.getWrl_totalTransaction()+" Workload Transactions)");
		System.out.println("      # Impact of Distributed Transactions: "+this.getWrl_dt_impact()
				+" (for a particular workload round)");
		
		if(this.isWrl_hasDataMoved()) {
			System.out.println("      # Inter-Partition Data Movements: "+this.getWrl_interPartitionDataMovements()
					+" ("+this.getWrl_percentage_pdmv()+"% of "
					+"Total "+this.getWrl_totalData()+" Workload Data)");
			System.out.println("      # Inter-Node Data Movements: "+this.getWrl_interNodeDataMovements()
					+" ("+this.getWrl_percentage_ndmv()+"% of "
					+"Total "+this.getWrl_totalData()+" Workload Data)");
		}
		
		this.printPartitionTable(db);
		this.printDataPartitionTable();
		this.printDataNodeTable();
		//System.out.println();
		System.out.println("      -----------------------------------------------------------------------------------------------------------------");
	}
	
	@Override
	public String toString() {	
		return (this.wrl_label+" ["+this.getWrl_totalTransaction()+" Transactions (containing "+this.getWrl_totalData()
				+" Data) of "+this.getWrl_transactionTypes());//+" types having a distribution of ");//+this.printWrl_transactionProp()+"]");
	}

	@Override
	public int compareTo(Workload workload) {
		int compare = ((int)this.wrl_id < (int)workload.wrl_id) ? -1: ((int)this.wrl_id > (int)workload.wrl_id) ? 1:0;
		return compare;
	}
}