/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import jkamal.prototype.db.Database;
import jkamal.prototype.transaction.TransactionDataSet;

public class HGraphClusters {
	private String part_dir = "C:\\Users\\Joarder Kamal\\git\\Prototype\\Prototype\\exec\\native\\hMetis\\1.5.3-win32";
	
	public HGraphClusters() { }
	
	public TransactionDataSet readPartFile(Database db, Workload workload) throws IOException {		
		TransactionDataSet transactionDataSet = workload.getWrl_transactionDataSet();
		String workload_file = workload.getWrl_workload_file().getWorkload_file();
		
		String hgraph_part_file = workload_file+".part."+db.getDb_partitions().size();						
		File hgraph_data_input = new File(this.part_dir+"\\"+hgraph_part_file);
		int data_id = -1;
		int cluster_id = -1;
		
		Scanner scanner = new Scanner(hgraph_data_input);
		try {
			while(scanner.hasNextLine()) {
				cluster_id = Integer.valueOf(scanner.nextLine());
				data_id++;
				
				transactionDataSet.getTransactionDataSet().get(data_id).setData_hmetis_cluster_id(cluster_id); //* possible bug may resides in here
				
				/*data = new Data(transactionDataSet.getTransactionDataSet().get(data_id).getData_id(), 
						Integer.toString(transactionDataSet.getTransactionDataSet().get(data_id).getData_id()), 
						cluster_id, 
						transactionDataSet.getTransactionDataSet().get(data_id).getData_node_id());
				
				data.setData_hmetis_cluster_id(cluster_id);
				postTransactionDataSet.getTransactionDataSet().add(data);*/
			}
		} finally {
			scanner.close();
		}
		
		return transactionDataSet;
	}
}