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
import java.util.Map.Entry;
import java.util.TreeMap;

import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;

public class Workload implements Comparable<Workload> {
	private int wrl_id;
	private int wrl_database_id;
	private String wrl_label;
	private int wrl_type; // Represents the number of Transaction types. e.g. for AuctionMark it is 10
	private List<Transaction> wrl_transactionList;
	private List<Data> wrl_dataList;
	private double[] wrl_transactionProp;	
	private String wrl_workload_file = null;
	private String wrl_fixfile = null;
	private Map<Integer, ArrayList<Data>> dataPartitionTable;
	private Map<Integer, ArrayList<Data>> dataNodeTable;
	
	public Workload(int id, int type, int db_id) {
		this.setWrl_id(id);
		this.setWrl_database_id(db_id);
		this.setWrl_label("WL-"+Integer.toString(this.getWrl_id()));
		this.setWrl_type(type);
		this.setWrl_transactionList(new ArrayList<Transaction>());
		this.setWrl_transactionProp(new double[this.getWrl_type()]);
		this.setWrl_dataList(new ArrayList<Data>());
		this.setWrl_workload_file("workload.txt");
		this.setWrl_fixfile("fixfile.txt");
		//this.setWrl_dataPartitionTable(new TreeMap<Integer, ArrayList<Data>>());
		//this.setWrl_dataNodeTable(new TreeMap<Integer, ArrayList<Data>>());
	}
	
	// Copy Constructor (Need to handle the Custom Type variables !!)
	public Workload(Workload workload) {
		this.wrl_id = workload.getWrl_id();
		this.wrl_database_id = workload.getWrl_database_id(); 
		this.wrl_label = workload.getWrl_label();
		this.wrl_type = workload.getWrl_type();		
		
		List<Transaction> cloneTransactionList = new ArrayList<Transaction>();
		Transaction cloneTransaction;
		for(Transaction transaction : workload.getWrl_transactionList()) {
			cloneTransaction = new Transaction(transaction);
			cloneTransactionList.add(cloneTransaction);
		}
		this.wrl_transactionList = cloneTransactionList;
		
		List<Data> cloneWrlDataSet = new ArrayList<Data>();
		Data cloneData;
		for(Data data : workload.getWrl_dataList()) {
			cloneData = new Data(data);
			cloneWrlDataSet.add(cloneData);
		}
		this.wrl_dataList = cloneWrlDataSet;
				
		double[] cloneTransactionProp = new double[this.wrl_type];		
		System.arraycopy(workload.getWrl_transactionProp(), 0, cloneTransactionProp, 0, workload.getWrl_transactionProp().length);
		this.wrl_transactionProp = cloneTransactionProp;								
				
		this.wrl_workload_file = workload.getWrl_workload_file();
		this.wrl_fixfile = workload.getWrl_fixfile();
		
		//this.setWrl_dataPartitionTable(new TreeMap<Integer, ArrayList<Data>>());
		//this.setWrl_dataNodeTable(new TreeMap<Integer, ArrayList<Data>>());
	}

	public int getWrl_id() {
		return wrl_id;
	}

	public void setWrl_id(int id) {
		this.wrl_id = id;
	}

	public int getWrl_database_id() {
		return wrl_database_id;
	}

	public void setWrl_database_id(int wrl_database_id) {
		this.wrl_database_id = wrl_database_id;
	}

	public String getWrl_label() {
		return wrl_label;
	}

	public void setWrl_label(String label) {
		this.wrl_label = label;
	}

	public int getWrl_type() {
		return wrl_type;
	}

	public void setWrl_type(int wrl_type) {
		this.wrl_type = wrl_type;
	}

	public List<Transaction> getWrl_transactionList() {
		return wrl_transactionList;
	}

	public void setWrl_transactionList(List<Transaction> wrl_transactionList) {
		this.wrl_transactionList = wrl_transactionList;
	}

	public double[] getWrl_transactionProp() {
		return wrl_transactionProp;
	}

	public void setWrl_transactionProp(double[] wrl_transactionProp) {
		this.wrl_transactionProp = wrl_transactionProp;
	}

	public List<Data> getWrl_dataList() {
		return wrl_dataList;
	}

	public void setWrl_dataList(List<Data> wrl_dataList) {
		this.wrl_dataList = wrl_dataList;
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
	
	public Map<Integer, ArrayList<Data>> getDataPartitionTable() {
		return dataPartitionTable;
	}

	public void setWrl_dataPartitionTable(Map<Integer, ArrayList<Data>> dataPartitionTable) {
		this.dataPartitionTable = dataPartitionTable;
	}

	public Map<Integer, ArrayList<Data>> getDataNodeTable() {
		return dataNodeTable;
	}

	public void setWrl_dataNodeTable(Map<Integer, ArrayList<Data>> dataNodeTable) {
		this.dataNodeTable = dataNodeTable;
	}

	public void generateWorkloadFile(String dir) {
		List<Data> dataSet = this.getWrl_dataList();
		Transaction transaction;
		File workloadFile = new File(dir+"\\"+this.getWrl_workload_file());
		
		int totalTransactions = this.getWrl_transactionList().size();
		int totalDataItems = dataSet.size();		
		int hasTransactionWeight = 1;
		int hasDataWeight = 1;						
		
		try {
			workloadFile.createNewFile();
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(workloadFile), "utf-8"));
				writer.write(totalTransactions+" "+totalDataItems+" "+hasTransactionWeight+""+hasDataWeight+"\n");
							
				Iterator<Transaction> itr_tr = this.getWrl_transactionList().iterator();
				while(itr_tr.hasNext()) {
					transaction = itr_tr.next();
					writer.write(transaction.getTr_weight()+" "+String.valueOf(transaction.getTr_id()+1)+" ");
						
						Iterator<Data> itr_data =  transaction.getTr_dataSet().iterator();
						while(itr_data.hasNext()) {
							writer.write(Integer.toString(itr_data.next().getData_shadow_hmetis_id()));
							
							if(itr_data.hasNext())
								writer.write(" "); 
						} // end -- while() loop
						
						writer.write("\n");
				} // end -- while() loop
				
				// Writing Data weight.
				Iterator<Data> iterator = dataSet.iterator();
				while(iterator.hasNext()) {
					writer.write(Integer.toString(iterator.next().getData_weight()));
					
					if(iterator.hasNext())
						writer.write("\n");
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
		List<Data> dataSet = this.getWrl_dataList();
		File fixFile = new File(dir+"\\"+this.getWrl_fixfile());
		Data data;
		
		try {
			fixFile.createNewFile();
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fixFile), "utf-8"));
				
				Iterator<Data> iterator = dataSet.iterator();
				while(iterator.hasNext()) {
					data = iterator.next();
					
					if(data.isData_isMoveable())
						writer.write(Integer.toString(data.getData_partition_id()));
					else
						writer.write(Integer.toString(-1));
					
					if(iterator.hasNext())
						writer.write("\n");
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

	// Generates Workload Data Partition Table
	public void generateDataPartitionTable() {
		this.setWrl_dataPartitionTable(new TreeMap<Integer, ArrayList<Data>>());
		List<Data> trDataList = this.getWrl_dataList();
		Data data;
		ArrayList<Data> dataList;
		
		Iterator<Data> iterator = trDataList.iterator();
		while(iterator.hasNext()) {
			data = iterator.next();			
			
			if(!data.isData_isPartitionRoaming()) {			
				if(this.getDataPartitionTable().containsKey(data.getData_partition_id())) {
					this.getDataPartitionTable().get(data.getData_partition_id()).add(data);
				} else {
					dataList = new ArrayList<Data>();
					dataList.add(data);
					this.getDataPartitionTable().put(data.getData_partition_id(), dataList);
				}
			} else {
				if(this.getDataPartitionTable().containsKey(data.getData_roaming_partition_id())) {
					this.getDataPartitionTable().get(data.getData_roaming_partition_id()).add(data);
				} else {
					dataList = new ArrayList<Data>();
					dataList.add(data);
					this.getDataPartitionTable().put(data.getData_roaming_partition_id(), dataList);
				}
			}
		}			
	}
	
	// Generates Workload Data Partition Table
	public void generateDataNodeTable() {
		this.setWrl_dataNodeTable(new TreeMap<Integer, ArrayList<Data>>());
		List<Data> trDataList = this.getWrl_dataList();
		Data data;
		ArrayList<Data> dataList;
		
		Iterator<Data> iterator = trDataList.iterator();
		while(iterator.hasNext()) {
			data = iterator.next();			
			
			if(!data.isData_isNodeRoaming()) {
				if(this.getDataNodeTable().containsKey(data.getData_node_id())) {
					this.getDataNodeTable().get(data.getData_node_id()).add(data);
				} else {
					dataList = new ArrayList<Data>();
					dataList.add(data);
					this.getDataNodeTable().put(data.getData_node_id(), dataList);
				}
			} else {
				if(this.getDataNodeTable().containsKey(data.getData_roaming_node_id())) {
					this.getDataNodeTable().get(data.getData_roaming_node_id()).add(data);
				} else {
					dataList = new ArrayList<Data>();
					dataList.add(data);
					this.getDataNodeTable().put(data.getData_roaming_node_id(), dataList);
				}
			}
		}			
	}
	
	public void printWrl_transactionProp() {
		int size = this.getWrl_transactionProp().length;
		
		System.out.print("{");
		for(double prop : this.getWrl_transactionProp()) {
			System.out.print(prop);
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
		System.out.println();
		System.out.print("\n===Workload's Data - Partition Details========================\n");						
		for(Entry<Integer, ArrayList<Data>> entry : this.getDataPartitionTable().entrySet()) {
			System.out.print(" P"+entry.getKey()+"["+entry.getValue().size()+"]: {");			
			
			roaming_data = 0;
			comma = entry.getValue().size();
			for(Data data : entry.getValue()) {
				System.out.print(data.toString());
				if(data.isData_isPartitionRoaming())
					++roaming_data;
				
				if(comma != 1)
					System.out.print(", ");
			
				--comma;						
			} // end -- for() loop
			
			System.out.print("} >> Roaming["+roaming_data+"]\n");
		} // end -- for() loop		
	}
	
	public void printDataNodeTable() {
		int comma = -1;
		int roaming_data = 0;
		
		// Workload's Data - Node Details
		System.out.println();
		System.out.print("===Workload's Data - Node Details========================\n");						
		for(Entry<Integer, ArrayList<Data>> entry : this.getDataNodeTable().entrySet()) {
			System.out.print(" N"+entry.getKey()+"["+entry.getValue().size()+"]: {");			
			
			roaming_data = 0;
			comma = entry.getValue().size();
			for(Data data : entry.getValue()) {
				System.out.print(data.toString());
				if(data.isData_isPartitionRoaming())
					++roaming_data;
				
				if(comma != 1)
					System.out.print(", ");
			
				--comma;						
			} // end -- for() loop
			
			System.out.print("} >> Roaming["+roaming_data+"]\n");
		} // end -- for() loop		
	}
	
	public void print(Database db) {
		System.out.print("\n===Workload Details========================");
		System.out.print("\n "+this.toString()+" having a distribution of ");				
		this.printWrl_transactionProp();
		
		for(Transaction transaction : this.getWrl_transactionList()) {
			transaction.generateTransactionCost(db);
			transaction.print();
		}						
		
		this.printDataPartitionTable();
		this.printDataNodeTable();
	}
	
	@Override
	public String toString() {	
		return (this.wrl_label+"["+this.getWrl_transactionList().size()+" Transactions of "+this.getWrl_type()+" types");
	}

	@Override
	public int compareTo(Workload workload) {
		int compare = ((int)this.wrl_id < (int)workload.wrl_id) ? -1: ((int)this.wrl_id > (int)workload.wrl_id) ? 1:0;
		return compare;
	}
}