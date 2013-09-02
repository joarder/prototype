/**
 * @author Joarder Kamal
 */

package jkamal.prototype.transaction;

import java.util.ArrayList;
import java.util.List;

import jkamal.prototype.db.Data;

public class TransactionDataSet {
	private List<Data> transactionDataSet;
	
	public TransactionDataSet() {
		this.setTransactionDataSet(new ArrayList<Data>());
	}
	
	// Copy Constructor
	public TransactionDataSet(TransactionDataSet dataSet) {
		transactionDataSet = dataSet.getTransactionDataSet();
	}

	public List<Data> getTransactionDataSet() {
		return transactionDataSet;
	}

	public void setTransactionDataSet(List<Data> transactionDataSet) {
		this.transactionDataSet = transactionDataSet;
	}
}