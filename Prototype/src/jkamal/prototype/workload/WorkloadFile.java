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
import java.util.Iterator;

import jkamal.prototype.db.Data;
import jkamal.prototype.transaction.Transaction;
import jkamal.prototype.transaction.TransactionDataSet;

public class WorkloadFile {
	private String workload_file = null;

	public WorkloadFile() {
		this.setWorkload_file("workload.txt");		
	}
		
	public String getWorkload_file() {
		return workload_file;
	}

	public void setWorkload_file(String workload_file) {
		this.workload_file = workload_file;
	}

	public void generateWorkloadFile(Workload workload, String workload_file_dir) {
		TransactionDataSet transactionDataSet = workload.getWrl_transactionDataSet();
		int totalTransactions = workload.getWrl_transactionList().size();
		int totalDataItems = transactionDataSet.getTransactionDataSet().size();
		
		Transaction transaction;
		int hasTransactionWeight = 1;
		int hasDataWeight = 1;
						
		File workloadFile = new File(workload_file_dir+"\\"+this.getWorkload_file());
		
		try {
			workloadFile.createNewFile();
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(workloadFile), "utf-8"));
				writer.write(totalTransactions+" "+totalDataItems+" "+hasTransactionWeight+""+hasDataWeight+"\n");
							
				Iterator<Transaction> itr_tr = workload.getWrl_transactionList().iterator();
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
				Iterator<Data> iterator = transactionDataSet.getTransactionDataSet().iterator();
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
		
		//return workloadFile;
	}
}