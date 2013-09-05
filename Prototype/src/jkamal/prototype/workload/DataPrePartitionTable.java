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
import java.util.TreeMap;
import java.util.Map.Entry;

import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;
import jkamal.prototype.transaction.TransactionDataSet;

public class DataPrePartitionTable {
	private Map<Integer, ArrayList<Data>> dataPrePartitionTable;
	private String dataPrePartitionTableFileName = null;

	public DataPrePartitionTable() {
		this.setDataPrePartitionTable(new TreeMap<Integer, ArrayList<Data>>());
		this.setDataPrePartitionTableFileName("prepartition.txt");
	}
	
	// Copy Constructor
	public DataPrePartitionTable(DataPrePartitionTable dataPrePartitionTable) {
		// Cloning DataPrePartitionTable
		Map<Integer, ArrayList<Data>> clonePrePartitionTable = new TreeMap<Integer, ArrayList<Data>>();
		ArrayList<Data> cloneDataList;
		Data cloneData;
		for(Entry<Integer, ArrayList<Data>> entry : dataPrePartitionTable.getDataPrePartitionTable().entrySet()) {
			cloneDataList = new ArrayList<Data>();
			for(Data data : entry.getValue()) {
				cloneData = new Data(data);
				cloneDataList.add(cloneData);				
			}
			clonePrePartitionTable.put(entry.getKey(), cloneDataList);
		}		
		this.setDataPrePartitionTable(clonePrePartitionTable);		
	}
	
	public Map<Integer, ArrayList<Data>> getDataPrePartitionTable() {
		return dataPrePartitionTable;
	}

	public void setDataPrePartitionTable(Map<Integer, ArrayList<Data>> prePartitionTable) {
		this.dataPrePartitionTable = prePartitionTable;
	}

	public String getDataPrePartitionTableFileName() {
		return dataPrePartitionTableFileName;
	}

	public void setDataPrePartitionTableFileName(String prePartitionTableFileName) {
		this.dataPrePartitionTableFileName = prePartitionTableFileName;
	}

	public void generatePrePartitionTable(Database db, Workload workload, String DIR_LOCATION) {
		TransactionDataSet transactionDataSet = workload.getWrl_transactionDataSet();
		Data data;
		ArrayList<Data> dataList;
		
		Iterator<Data> iterator = transactionDataSet.getTransactionDataSet().iterator();
		while(iterator.hasNext()) {
			data = iterator.next();			
			
			if(this.getDataPrePartitionTable().containsKey(data.getData_partition_id())) {
				this.getDataPrePartitionTable().get(data.getData_partition_id()).add(data);
			} else {
				dataList = new ArrayList<Data>();
				dataList.add(data);
				this.getDataPrePartitionTable().put(data.getData_partition_id(), dataList);
			}
		}
		
		generatePrePartitionTableFile(DIR_LOCATION);
	}
	
	public void generatePrePartitionTableFile(String file_dir) {
		File prePartitionTableFile = new File(file_dir+"\\"+this.getDataPrePartitionTableFileName());
		int space = -1;
		
		try {
			prePartitionTableFile.createNewFile();
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(prePartitionTableFile), "utf-8"));
								
				for(Entry<Integer, ArrayList<Data>> entry : this.getDataPrePartitionTable().entrySet()) {
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
		
		//return prePartitionTableFile;
	}
	
	public void print() {
		int comma = -1;
		
		// Pre-Partitioning Details
		System.out.print("\n===Data Pre-partitioning Details========================\n");						
		for(Entry<Integer, ArrayList<Data>> entry : this.getDataPrePartitionTable().entrySet()) {
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