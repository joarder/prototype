/**
 * @author Joarder Kamal
 * 
 * Perform Data Movement after analysing Workload using HyperGraph Partitioning 
 */

package jkamal.prototype.db;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import jkamal.prototype.util.Matrix;
import jkamal.prototype.util.MatrixElement;
import jkamal.prototype.workload.MappingTable;
import jkamal.prototype.workload.Transaction;
import jkamal.prototype.workload.Workload;

public class DataMovement {
	public DataMovement() {}
	
	public void strategy1(Database db, Workload workload) throws IOException {
		// Create a Clone of the Database and Workload using Copy Constructor
		Database cloneDb = new Database(db);
		Workload cloneWorkload = new Workload(workload);
		
		// Create Mapping Matrix
		MappingTable mappingTable = new MappingTable();		
		Matrix mapping = mappingTable.generateMappingTable(cloneDb, cloneWorkload);
		System.out.println(">> Generating Data Movement Mapping Matrix ...\n   [First Row: Pre-Partition Id, First Col: Cluster Id, Elements: Data Occurance Counts]");
		mapping.print();
		
		// Create Key-Value (Destination PID-Cluster ID) Mappings from Mapping Matrix
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>();
		MatrixElement colMax;
		for(int col = 1; col < mapping.getN(); col++) {
				colMax = mapping.findColMax(col);
				keyMap.put(colMax.getCol_pos()-1, colMax.getRow_pos()-1); // which cluster will go to which partition
				//System.out.println("-#-Col("+col+") >> C"+(colMax.getCol_pos()-1)+"|P"+(colMax.getRow_pos()-1));
		}
		
		// Perform Actual Data Movement
		// Stage-1: Within the Workload Data List
		int movements = this.move(cloneDb, cloneWorkload, keyMap);
		
		// Stage-2: Within the individual Transaction Data Set (Only due to cloned Workload, otherwise NOT required)
		List<Transaction> transactionList = cloneWorkload.getWrl_transactionList();
		Transaction transaction;
		Data trData;
		int dst_partition_id = -1;
		
		Iterator<Transaction> tr_iterator = transactionList.iterator();
		while(tr_iterator.hasNext()) {
			transaction = tr_iterator.next();
			Iterator<Data> tr_data_iterator = transaction.getTr_dataSet().iterator();
			while(tr_data_iterator.hasNext()) {
				trData = tr_data_iterator.next();
				dst_partition_id = keyMap.get(trData.getData_hmetis_cluster_id());
				
				if(trData.getData_partition_id() != dst_partition_id && !trData.isData_isPartitionRoaming()) {
					trData.setData_roaming_partition_id(dst_partition_id);
					trData.setData_isPartitionRoaming(true);
				}
			}
			transaction.generateTransactionCost(cloneDb);
		}
		
		cloneWorkload.generateDataPartitionTable();
		cloneWorkload.generateDataNodeTable();
		
		System.out.print("\n>> Total "+movements+" Data movements are required using Strategy-1.");
		
		//==============================================================================================
		// Printing out details after performing Data Movement using hMetis		
		System.out.println("\n>> After data movement using One-to-One strategy ...");
		cloneWorkload.print(cloneDb);		
	}
	
	public void strategy2(Database db, Workload workload) {
		// Create Mapping Matrix
		MappingTable mappingTable = new MappingTable();		
		Matrix mapping = mappingTable.generateMappingTable(db, workload);
		System.out.println(">> Generating Data Movement Mapping Matrix ...\n   [First Row: Pre-Partition Id, First Col: Cluster Id, Elements: Data Occurance Counts]");
		//mapping.print();
				
		// Step-1 :: Max Movement Matrix Formation
		MatrixElement max;
		int diagonal_pos = 1;		
		
		for(int m = 1; m < mapping.getM(); m++) {
			max = mapping.findMax(diagonal_pos);
			//System.out.println(">> Max: "+max.getCounts()+", Col: "+(max.getCol_pos()+1)+", Row: "+(max.getRow_pos()+1));
			
			// Row/Col swap with diagonal Row/Col
			if(max.getCounts() != 0) {
				mapping.swap_row(max.getRow_pos(), diagonal_pos);
				mapping.swap_col(max.getCol_pos(), diagonal_pos);
			}			
			
			++diagonal_pos;
		}		

		// @debug
		//System.out.println("\n>> @debug :: Movement Matrix before PID change >>");
		mapping.print();
		
		// Step-2 :: PID Conversion		
		// Create the PID conversion Key Map
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>(); 
		for(int row = 1; row < mapping.getM(); row++) {
			keyMap.put((int)mapping.getMatrix()[0][row].getCounts(), (int)mapping.getMatrix()[row][0].getCounts());
			//System.out.println("-#-Row("+row+" >> C"+(int)mapping.getMatrix()[0][row].getCounts()+"|P"+(int)mapping.getMatrix()[row][0].getCounts());
		}
	
		// Perform Actual Data Movement	
		// Stage-1: Within the Workload Data List
		int movements = this.move(db, workload, keyMap);		
		System.out.print("\n>> Total "+movements+" Data movements are required using Strategy-2.");
		
		// Generating Workload's Data Partition and Node Distribution Details
		workload.generateDataPartitionTable();
		workload.generateDataNodeTable();
		
		//==============================================================================================
		// Printing out details after performing Data Movement using hMetis		
		System.out.println("\n>> After data movement using Strategy-2 ...");
		workload.print(db);
	}
	
	private int move(Database db, Workload workload, Map<Integer, Integer> keyMap) {
		// Perform Actual Data Movement
		// Stage-1: Within the Workload Data List
		List<Data> wrlDataList = workload.getWrl_dataList();	
		Data wrlData;				
		Data roaming_data;
		Partition old_partition;
		Partition dst_partition;
		int old_partition_id = -1;
		int dst_partition_id = -1;
		int old_node_id = -1;
		int dst_node_id = -1;
		int movements = 0;	int d = 0;	
		
		Iterator<Data> data_iterator = wrlDataList.iterator();
		while(data_iterator.hasNext()) {d++;
			wrlData = data_iterator.next();
			old_partition_id = wrlData.getData_partition_id();
			old_partition = db.getDb_partition_table().getPartition(old_partition_id);
			old_node_id = db.getDb_partition_table().lookup(old_partition_id);			
			
			dst_partition_id = keyMap.get(wrlData.getData_hmetis_cluster_id());
			dst_partition = db.getDb_partition_table().getPartition(dst_partition_id);
			dst_node_id = db.getDb_partition_table().lookup(dst_partition_id);
			//System.out.print("\n-#"+d+"-"+wrlData.toString()+"--C"+wrlData.getData_hmetis_cluster_id()+"--P"+dst_partition_id);			
			
			if(old_partition_id != dst_partition_id) {
				wrlData.setData_hasShadowHMetisId(false);
				if(wrlData.isData_isRoaming()) { // Data is already Roaming
					System.out.print("\n >> @debug :: *R"+d+"-"+wrlData.toString());
				} else { // Data will be Roaming for the first time
					wrlData.setData_isRoaming(true);
					wrlData.setData_isPartitionRoaming(true);
					wrlData.setData_roaming_partition_id(dst_partition_id);
					if(old_node_id != dst_node_id) {
						wrlData.setData_roaming_node_id(dst_node_id);					
						wrlData.setData_isNodeRoaming(true);
					}										
				
					roaming_data = new Data(wrlData); // Cloning the workload Data item
					roaming_data.setData_partition_id(dst_partition_id);					
					roaming_data.setData_roaming_partition_id(-1);
					roaming_data.setData_node_id(dst_node_id);
					roaming_data.setData_roaming_node_id(-1);						
					//System.out.print("-@R"+d+"-"+wrlData.toString());
					
					// Add the Roaming Data into the destination Partition's Foreign Data Item List
					dst_partition.getForeign_data_items().add(roaming_data);
					// Add an entry in the Old Partition Table's Roaming Data Item Table
					old_partition.getRoaming_data_items().put(wrlData.getData_id(), dst_partition_id);
					// Remove the Data item from Old Partition's Data Item List
					old_partition.getPartition_data_items().remove(wrlData);

					++movements;						
				} // end -- if-else()			
			} // end -- if()
		} // end -- while()
		
		return movements;
	}
}