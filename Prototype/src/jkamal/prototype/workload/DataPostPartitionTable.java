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
import java.util.Scanner;
import java.util.TreeMap;
import java.util.Map.Entry;

import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;
import jkamal.prototype.transaction.TransactionDataSet;

public class DataPostPartitionTable {
	private Map<Integer, ArrayList<Data>> postPartitionTable;
	private String postPartitionTableFileName = null;

	public DataPostPartitionTable() {
		this.setPostPartitionTable(new TreeMap<Integer, ArrayList<Data>>());
		this.setPostPartitionTableFileName("postpartition.txt");
	}
	
	public Map<Integer, ArrayList<Data>> getPostPartitionTable() {
		return postPartitionTable;
	}

	public void setPostPartitionTable(Map<Integer, ArrayList<Data>> postPartitionTable) {
		this.postPartitionTable = postPartitionTable;
	}

	public String getPostPartitionTableFileName() {
		return postPartitionTableFileName;
	}

	public void setPostPartitionTableFileName(String postPartitionTableFileName) {
		this.postPartitionTableFileName = postPartitionTableFileName;
	}
	
	public void generatePostPartitionTable(Database db, Workload workload, String DIR_LOCATION) throws IOException {
		TransactionDataSet transactionDataSet = readPartFile(db, workload, DIR_LOCATION);		
		Data data;
		ArrayList<Data> dataList;
		
		Iterator<Data> iterator = transactionDataSet.getTransactionDataSet().iterator();
		while(iterator.hasNext()) {
			data = iterator.next();			
			
			if(this.getPostPartitionTable().containsKey(data.getData_partition_id())) {
				this.getPostPartitionTable().get(data.getData_partition_id()).add(data);
			} else {
				dataList = new ArrayList<Data>();
				dataList.add(data);
				this.getPostPartitionTable().put(data.getData_partition_id(), dataList);
			}
		}
		
		generatePostPartitionTableFile(DIR_LOCATION);
	}
	
	public void generatePostPartitionTableFile(String file_dir) {
		File postPartitionTableFile = new File(file_dir+"\\"+this.getPostPartitionTableFileName());
		int space = -1;
		
		try {
			postPartitionTableFile.createNewFile();
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(postPartitionTableFile), "utf-8"));
								
				for(Entry<Integer, ArrayList<Data>> entry : this.getPostPartitionTable().entrySet()) {
					writer.write(Integer.toString(entry.getKey()));
					writer.write(" ");
					
					space = entry.getValue().size();
					for(Data data : entry.getValue()) {
						writer.write(Integer.toString(data.getData_id()));
						//writer.write("-");
						//writer.write(Integer.toString(data.getData_shadow_hmetis_id()));
						
						if(space != 1)
							writer.write(" ");
					
						--space;						
					}
					
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
		
		//return postPartitionTableFile;
	}
	
	public TransactionDataSet readPartFile(Database db, Workload workload, String part_dir) throws IOException {		
		TransactionDataSet transactionDataSet = workload.getWrl_transactionDataSet();
		String workload_file = workload.getWrl_workload_file().getWorkload_file();
		
		String hgraph_part_file = workload_file+".part."+db.getDb_partitions().size();						
		File hgraph_data_input = new File(part_dir+"\\"+hgraph_part_file);
		int data_id = -1;
		int partition_id = -1;
		
		TransactionDataSet postTransactionDataSet = new TransactionDataSet();
		Data data;
		
		Scanner scanner = new Scanner(hgraph_data_input);
		try {
			while(scanner.hasNextLine()) {
				partition_id = Integer.valueOf(scanner.nextLine());
				data_id++;
				
				transactionDataSet.getTransactionDataSet().get(data_id).setData_hmetis_partition_id(partition_id); //* possible bug may resides in here
				
				data = new Data(transactionDataSet.getTransactionDataSet().get(data_id).getData_id(), 
						Integer.toString(transactionDataSet.getTransactionDataSet().get(data_id).getData_id()), 
						partition_id, 
						transactionDataSet.getTransactionDataSet().get(data_id).getData_node_id());
				
				data.setData_hmetis_partition_id(partition_id);
				postTransactionDataSet.getTransactionDataSet().add(data);
			}
		} finally {
			scanner.close();
		}
		
		return postTransactionDataSet;
	}
}