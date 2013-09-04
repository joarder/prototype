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
		
		List<Data> cloneTransactionDataSet = new ArrayList<Data>();
		Data cloneData;
		for(Data data : dataSet.getTransactionDataSet()) {
			cloneData = new Data(data);
			cloneTransactionDataSet.add(cloneData);
		}
		this.transactionDataSet = cloneTransactionDataSet;
		
	}

	public List<Data> getTransactionDataSet() {
		return transactionDataSet;
	}

	public void setTransactionDataSet(List<Data> transactionDataSet) {
		this.transactionDataSet = transactionDataSet;
	}
}