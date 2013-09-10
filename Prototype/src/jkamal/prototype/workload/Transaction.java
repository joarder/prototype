/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;
import jkamal.prototype.db.Partition;

public class Transaction implements Comparable<Transaction> {
	private int tr_id;
	private String tr_label;
	private int tr_type;	
	private int tr_weight;
	private Set<Data> tr_dataSet;
	private int tr_dtCost; // Node Span Cost or, Distributed Transaction Cost
	private int tr_psCost; // Partition Span Cost
	
	public Transaction(int id, Set<Data> dataSet) {
		this.setTr_id(id);
		this.setTr_label("TR-"+Integer.toString(this.getTr_id()));
		this.setTr_type(-1);
		this.setTr_weight(1); // 1 means that the transaction has occurred for a single time in the Workload
		this.setTr_dataSet(dataSet);
		this.setTr_dtCost(0);
		this.setTr_psCost(0);				
	}
	
	// Copy Constructor
	public Transaction(Transaction transaction) {
		this.tr_id = transaction.getTr_id();
		this.tr_label = transaction.getTr_label();
		this.tr_type = transaction.getTr_type();
		this.tr_weight = transaction.getTr_weight();		
		
		Data cloneData;
		Set<Data> cloneDataSet = new TreeSet<Data>();
		for(Data data : transaction.getTr_dataSet()) {
			cloneData = new Data(data);
			cloneDataSet.add(cloneData);
		}		
		this.tr_dataSet = cloneDataSet;
		
		this.tr_dtCost = transaction.getTr_dtCost();
		this.tr_psCost = transaction.getTr_psCost();
	}

	public int getTr_id() {
		return tr_id;
	}

	public void setTr_id(int tr_id) {
		this.tr_id = tr_id;
	}

	public void setTr_label(String tr_label) {
		this.tr_label = tr_label;
	}

	public void setTr_weight(int tr_weight) {
		this.tr_weight = tr_weight;
	}

	public int getTr_type() {
		return tr_type;
	}

	public void setTr_type(int tr_type) {
		this.tr_type = tr_type;
	}

	public int getTr_weight() {
		return tr_weight;
	}
	public String getTr_label() {
		return tr_label;
	}

	public Set<Data> getTr_dataSet() {
		return tr_dataSet;
	}

	public void setTr_dataSet(Set<Data> tr_dataSet) {
		this.tr_dataSet = tr_dataSet;
	}	

	public int getTr_dtCost() {
		return tr_dtCost;
	}

	public void setTr_dtCost(int tr_dtCost) {
		this.tr_dtCost = tr_dtCost;
	}

	public int getTr_psCost() {
		return tr_psCost;
	}

	public void setTr_psCost(int tr_psCost) {
		this.tr_psCost = tr_psCost;
	}
	
	// This function will calculate the Node and Partition Span Cost for the representative Transaction
	public void generateTransactionCost(Database db) {
		int pid = -1;
		Partition partition;
		Set<Integer> nsCost = new TreeSet<Integer>();
		Set<Data> dataSet = this.getTr_dataSet();
		Data data;
	
		// Calculate Node Span Cost which is equivalent to the Distributed Transaction Cost
		Iterator<Data> ns = dataSet.iterator();
		while(ns.hasNext()) {
			data = ns.next();
			if(data.isData_isPartitionRoaming())
				pid = data.getData_roaming_partition_id();				
			else 
				pid = data.getData_partition_id();
				
			partition = db.getDb_partition_table().getPartition(pid);
			nsCost.add(partition.getPartition_node_id());
		}
		
		this.setTr_dtCost(nsCost.size()-1);
		
		// Calculate Partition Span Cost
		pid = -1;
		Set<Integer> psCost = new TreeSet<Integer>();
	
		Iterator<Data> ps = dataSet.iterator();
		while(ps.hasNext()) {
			data = ps.next();
			if(data.isData_isPartitionRoaming())
				pid = data.getData_roaming_partition_id();
			else 
				pid = data.getData_partition_id();				
			
			psCost.add(pid);
		}
	
		this.setTr_psCost(psCost.size()-1);		
	}
	
	// Given a Data Id this function will return the corresponding Data Object from the Transaction
	public Data lookup(int id) {
		for(Data data : this.tr_dataSet) {
			if(data.getData_id() == id)
				return data;
		}
		
		return null;
	}
	
	// This function will print all of the contents of the representative Transaction
	public void print() {
		//System.out.println("===Transaction Details========================");
		System.out.print("\n "+this.getTr_label()+"[CDT("+this.getTr_dtCost()+"), " +
				"PS("+this.getTr_psCost()+"), Data("+this.getTr_dataSet().size()+")]-{");
		
		Iterator<Data> data =  this.getTr_dataSet().iterator();
		while(data.hasNext()) {
			System.out.print(data.next().toString());
			if(data.hasNext())
				System.out.print(", ");
		}				
		
		System.out.print("}");		
	}
	
	@Override
	public String toString() {	
		return (this.tr_label+"[Type: "+this.tr_type+", Weight: "+this.tr_weight+"]");
	}

	@Override
	public int compareTo(Transaction transaction) {
		int compare = ((int)this.tr_id < (int)transaction.tr_id) ? -1: ((int)this.tr_id > (int)transaction.tr_id) ? 1:0;
		return compare;
	}
}