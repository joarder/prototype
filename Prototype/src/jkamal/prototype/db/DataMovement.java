/**
 * @author Joarder Kamal
 * 
 * Perform Data Movement after analysing Workload using HyperGraph Partitioning 
 */

package jkamal.prototype.db;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import jkamal.prototype.transaction.Transaction;
import jkamal.prototype.util.Matrix;
import jkamal.prototype.util.MatrixElement;
import jkamal.prototype.workload.MappingTable;
import jkamal.prototype.workload.Workload;

public class DataMovement {
	public DataMovement() {}
	
	public void OneToOne(Database db, Workload workload) throws IOException {
		// Create a Clone of the Database and Workload using Copy Constructor
		Database cloneDb = new Database(db);
		Workload cloneWorkload = new Workload(workload);
		
		// Create Mapping Matrix
		MappingTable mappingTable = new MappingTable();		
		Matrix mapping = mappingTable.generateMappingTable(cloneDb, cloneWorkload);
		System.out.println("\n>> Movement Matrix [First Row: Pre-Partition Id, First Col: Cluster Id, Elements: Data Occurance Counts] ...\n");
		mapping.print();
		
		// Create Key-Value (Destination PID-Cluster ID) Mappings from Mapping Matrix
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>();
		MatrixElement colMax;
		for(int col = 1; col < mapping.getN(); col++) {
				colMax = mapping.findColMax(col);
				keyMap.put(colMax.getCol_pos()-1, colMax.getRow_pos()-1); // which cluster will go to which partition
				System.out.println("-#-Col("+col+") >> C"+(colMax.getCol_pos()-1)+"|P"+(colMax.getRow_pos()-1));
		}
		
		// Perform Actual Data Movement
		int movements = 0;
		MatrixElement e;
		for(int row = 1; row < mapping.getM(); row++) {
			for(int col = 1; col < mapping.getN(); col++) {
				e = mapping.getMatrix()[row][col];
				
				Data data;
				Data roaming_data;
				int dst_partition_id = -1;
				Partition partition;
				
				Iterator<Data> iterator = e.getDataList().iterator();
				while(iterator.hasNext()) {
					data = iterator.next();
					dst_partition_id = keyMap.get(data.getData_hmetis_cluster_id());
					//System.out.print("\n-#-"+data.toString());
					
					if(data.getData_partition_id() != dst_partition_id) {
						data.setData_roaming_partition_id(dst_partition_id);
						data.setData_isRoaming(true);												
					
						roaming_data = new Data(data); // Cloning the Data
						//System.out.print("-@R-"+roaming_data.toString());
						partition = cloneDb.getDb_partition_table().getPartition(dst_partition_id);
						partition.getPartition_data_items().add(roaming_data);
						++movements;
					}
				}
			}
		}
		
		// Recalculate the Costs of Distributed Transactions (CDT)
		for(Transaction transaction : cloneWorkload.getWrl_transactionList())
			transaction.generateTransactionCost(cloneDb);
		
		System.out.println("\n Total Data movements required using One-to-One mapping strategy "+movements);
		
		//==============================================================================================
		// Printing out details after performing Data Movement using hMetis		
		System.out.println("\n>> After data movement using One-to-One strategy ...");
		cloneWorkload.print(cloneDb);		
	}
	
	public void OneToMany(Database db, Workload workload) {
		
	}
	
	/*
	public void move(Database db, Workload workload) throws IOException {
				
		
		int original_id = -1;
		int hmetis_id = -1;
		Data data;
		Data roaming_data;
		Partition partition; 
		int moves = 0;
		
		Iterator<Data> iterator = transactionDataSet.getTransactionDataSet().iterator();
		while(iterator.hasNext()) {
			data = iterator.next();
			original_id = data.getData_partition_id();
			hmetis_id = data.getData_hmetis_cluster_id();
			
			if(original_id != hmetis_id && !data.isData_isRoaming()) {				
				data.setData_roaming_partition_id(hmetis_id);
				data.setData_isRoaming(true);												
				
				roaming_data = new Data(data); // Calling Copy Constructor
				
				partition = db.getDb_partition_table().getPartition(hmetis_id);
				partition.getPartition_data_items().add(roaming_data);
				moves++;
			}
		}		
		
		//System.out.print("\n>> Total Data Moments Required using hMetis: "+moves);
		
		// Recalculate the Costs of Distributed Transactions (CDT)
		for(Transaction transaction : workload.getWrl_transactionList())
			transaction.generateTransactionCost(db);				
	}

	// This move() will utilize the Idea Matrix to perform Data movements
	public void move(Database db, Workload workload, IdeaTable ideaTable) {		
		TransactionDataSet transactionDataSet = workload.getWrl_transactionDataSet();
		int original_id = -1;
		int hmetis_id = -1;
		int idea_id = -1;
		int diagonal_pos = 1;
		int moves = 0;
		Data data;
		Data roaming_data;
		Partition partition; 
		
		Iterator<Data> iterator = transactionDataSet.getTransactionDataSet().iterator();
		while(iterator.hasNext()) {
			data = iterator.next();
			original_id = data.getData_partition_id();
			hmetis_id = data.getData_hmetis_cluster_id();
			idea_id = ideaTable.getKeyMap().get(original_id);
			diagonal_pos = idea_id;
			
			//System.out.print("\n-#-Oid:"+original_id+"|Hid:"+hmetis_id+"|Iid:"+idea_id);
			if(original_id != hmetis_id && !data.isData_isRoaming() && diagonal_pos == hmetis_id) {
				//System.out.print("\n-@-Oid:"+original_id+"|Hid:"+hmetis_id+"|Iid:"+idea_id);
				data.setData_roaming_partition_id(idea_id);
				data.setData_isRoaming(true);												
			
				roaming_data = new Data(data); // Calling Copy Constructor
			
				partition = db.getDb_partition_table().getPartition(idea_id);
				partition.getPartition_data_items().add(roaming_data);
				++moves;								
			}
		}
		
		//System.out.print("\n>> Total Data Moments Required using Idea: "+moves);
		
		// Recalculate the Costs of Distributed Transactions (CDT)
		for(Transaction transaction : workload.getWrl_transactionList())
			transaction.generateTransactionCost(db);
	}*/
}