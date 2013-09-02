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
	private Map<Integer, ArrayList<Data>> prePartitionTable;
	private String prePartitionTableFileName = null;

	public DataPrePartitionTable() {
		this.setPrePartitionTable(new TreeMap<Integer, ArrayList<Data>>());
		this.setPrePartitionTableFileName("prepartition.txt");
	}
	
	public Map<Integer, ArrayList<Data>> getPrePartitionTable() {
		return prePartitionTable;
	}

	public void setPrePartitionTable(Map<Integer, ArrayList<Data>> prePartitionTable) {
		this.prePartitionTable = prePartitionTable;
	}

	public String getPrePartitionTableFileName() {
		return prePartitionTableFileName;
	}

	public void setPrePartitionTableFileName(String prePartitionTableFileName) {
		this.prePartitionTableFileName = prePartitionTableFileName;
	}

	public void generatePrePartitionTable(Database db, Workload workload, String DIR_LOCATION) {
		TransactionDataSet transactionDataSet = workload.getWrl_transactionDataSet();
		Data data;
		ArrayList<Data> dataList;
		
		Iterator<Data> iterator = transactionDataSet.getTransactionDataSet().iterator();
		while(iterator.hasNext()) {
			data = iterator.next();			
			
			if(this.getPrePartitionTable().containsKey(data.getData_partition_id())) {
				this.getPrePartitionTable().get(data.getData_partition_id()).add(data);
			} else {
				dataList = new ArrayList<Data>();
				dataList.add(data);
				this.getPrePartitionTable().put(data.getData_partition_id(), dataList);
			}
		}
		
		generatePrePartitionTableFile(DIR_LOCATION);
	}
	
	public void generatePrePartitionTableFile(String file_dir) {
		File prePartitionTableFile = new File(file_dir+"\\"+this.getPrePartitionTableFileName());
		int space = -1;
		
		try {
			prePartitionTableFile.createNewFile();
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(prePartitionTableFile), "utf-8"));
								
				for(Entry<Integer, ArrayList<Data>> entry : this.getPrePartitionTable().entrySet()) {
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
}