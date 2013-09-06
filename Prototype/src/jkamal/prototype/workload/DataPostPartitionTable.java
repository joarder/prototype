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
		this.setDataPostPartitionTable(new TreeMap<Integer, ArrayList<Data>>());
		this.setDataPostPartitionTableFileName("postpartition.txt");
	}	
	
	// Copy Constructor
	public DataPostPartitionTable(DataPostPartitionTable dataPostPartitionTable) {
		// Cloning DataPostPartitionTable
		Map<Integer, ArrayList<Data>> clonePostPartitionTable = new TreeMap<Integer, ArrayList<Data>>();
		ArrayList<Data> cloneDataList;
		Data cloneData;
		for(Entry<Integer, ArrayList<Data>> entry : dataPostPartitionTable.getDataPostPartitionTable().entrySet()) {
			cloneDataList = new ArrayList<Data>();
			for(Data data : entry.getValue()) {
				cloneData = new Data(data);
				cloneDataList.add(cloneData);				
			}
			clonePostPartitionTable.put(entry.getKey(), cloneDataList);
		}
		this.setDataPostPartitionTable(clonePostPartitionTable);
	}
	
	public Map<Integer, ArrayList<Data>> getDataPostPartitionTable() {
		return postPartitionTable;
	}

	public void setDataPostPartitionTable(Map<Integer, ArrayList<Data>> postPartitionTable) {
		this.postPartitionTable = postPartitionTable;
	}

	public String getDataPostPartitionTableFileName() {
		return postPartitionTableFileName;
	}

	public void setDataPostPartitionTableFileName(String postPartitionTableFileName) {
		this.postPartitionTableFileName = postPartitionTableFileName;
	}
	
	public void generatePostPartitionTable(Database db, Workload workload, String DIR_LOCATION) throws IOException {
		//TransactionDataSet transactionDataSet = readPartFile(db, workload, DIR_LOCATION);
		TransactionDataSet transactionDataSet = workload.getWrl_transactionDataSet();
		Data data;
		ArrayList<Data> dataList;
		
		Iterator<Data> iterator = transactionDataSet.getTransactionDataSet().iterator();
		while(iterator.hasNext()) {
			data = iterator.next();			
			
			if(this.getDataPostPartitionTable().containsKey(data.getData_partition_id())) {
				this.getDataPostPartitionTable().get(data.getData_partition_id()).add(data);
			} else {
				dataList = new ArrayList<Data>();
				dataList.add(data);
				this.getDataPostPartitionTable().put(data.getData_partition_id(), dataList);
			}
		}
		
		generatePostPartitionTableFile(DIR_LOCATION);
	}
	
	public void generatePostPartitionTableFile(String file_dir) {
		File postPartitionTableFile = new File(file_dir+"\\"+this.getDataPostPartitionTableFileName());
		int space = -1;
		
		try {
			postPartitionTableFile.createNewFile();
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(postPartitionTableFile), "utf-8"));
								
				for(Entry<Integer, ArrayList<Data>> entry : this.getDataPostPartitionTable().entrySet()) {
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
		
	public void print() {
		int comma = -1;
		
		// Post-Partitioning Details
		System.out.print("\n===Data Post-partitioning Details========================\n");						
		for(Entry<Integer, ArrayList<Data>> entry : this.getDataPostPartitionTable().entrySet()) {
			System.out.print("P"+entry.getKey()+"["+entry.getValue().size()+"]: {");			
			
			comma = entry.getValue().size();
			for(Data data : entry.getValue()) {
				System.out.print(data.toString());
				
				if(comma != 1)
					System.out.print(", ");
			
				--comma;						
			} // end -- for() loop
			
			System.out.print("}\n");
		} // end -- for() loop
		System.out.print("\n");
	}
}