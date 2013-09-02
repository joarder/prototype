/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.ArrayList;
import java.util.List;

import jkamal.prototype.db.Database;
import jkamal.prototype.transaction.Transaction;
import jkamal.prototype.transaction.TransactionDataSet;

public class Workload implements Comparable<Workload> {
	private int wrl_id;
	private String wrl_label;
	private int wrl_type; // Represents the number of Transaction types. e.g. for AuctionMark it is 10
	private List<Transaction> wrl_transactionList;
	private double[] wrl_transactionProp;
	private TransactionDataSet wrl_transactionDataSet;
	private Database wrl_database;
	private WorkloadFile wrl_workload_file;
	private FixFile wrl_fixfile;
	
	public Workload(Database db, int id, int type) {
		this.setWrl_id(id);
		this.setWrl_label("WL-"+Integer.toString(this.getWrl_id()));
		this.setWrl_type(type);
		this.setWrl_transactionList(new ArrayList<Transaction>());
		this.setWrl_transactionProp(new double[this.getWrl_type()]);
		this.setWrl_transactionDataSet(new TransactionDataSet());
		this.setWrl_database(db);
	}

	public int getWrl_id() {
		return wrl_id;
	}

	public void setWrl_id(int id) {
		this.wrl_id = id;
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

	public TransactionDataSet getWrl_transactionDataSet() {
		return wrl_transactionDataSet;
	}

	public void setWrl_transactionDataSet(TransactionDataSet wrl_transactionDataSet) {
		this.wrl_transactionDataSet = wrl_transactionDataSet;
	}

	public Database getWrl_database() {
		return wrl_database;
	}

	public void setWrl_database(Database database) {
		this.wrl_database = database;
	}

	public WorkloadFile getWrl_workload_file() {
		return wrl_workload_file;
	}

	public void setWrl_workload_file(WorkloadFile wrl_workload_file) {
		this.wrl_workload_file = wrl_workload_file;
	}

	public FixFile getWrl_fixfile() {
		return wrl_fixfile;
	}

	public void setWrl_fixfile(FixFile wrl_fixfile) {
		this.wrl_fixfile = wrl_fixfile;
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
	
	public void print(Database db) {
		System.out.println();
		System.out.println("===Workload Details========================");
		System.out.print(" "+this.toString());
		
		System.out.print(" having a distribution of ");
		this.printWrl_transactionProp();
		System.out.println();
		
		for(Transaction transaction : this.getWrl_transactionList()) {
			transaction.generateTransactionCost(db);
			transaction.print();
			System.out.println();
		}
		
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