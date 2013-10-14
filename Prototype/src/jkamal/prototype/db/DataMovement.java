/**
 * @author Joarder Kamal
 * 
 * Perform Data Movement after analysing Workload using HyperGraph Partitioning 
 */

package jkamal.prototype.db;

import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import jkamal.prototype.util.Matrix;
import jkamal.prototype.util.MatrixElement;
import jkamal.prototype.workload.MappingTable;
import jkamal.prototype.workload.Transaction;
import jkamal.prototype.workload.Workload;

public class DataMovement {
	private String strategy;
	
	public DataMovement() {}
	
	public String getStrategy() {
		return strategy;
	}

	public void setStrategy(String strategy) {
		this.strategy = strategy;
	}

	public void baseStrategy(Database db, Workload workload) {
		// Create Mapping Matrix
		MappingTable mappingTable = new MappingTable();		
		Matrix mapping = mappingTable.generateMappingTable(db, workload);
		System.out.println("[ACT] Generating Data Movement Mapping Matrix ...\n" +
				"      (First Row: Pre-Partition Id, First Col: Cluster Id, Elements: Data Occurance Counts)");
		mapping.print();
		
		// Create Key-Value (Destination PID-Cluster ID) Mappings from Mapping Matrix
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>();		
		for(int col = 0; col < mapping.getN(); col++) {				
			keyMap.put(col, col); // which cluster will go to which partition
			//System.out.println("-#-Entry("+col+") [ACT] C"+col+"|P"+col);
		}
		
		// Perform Actual Data Movement
		this.move(db, workload, keyMap);
		workload.setWrl_hasDataMoved(true);					
		workload.setMessage("Base Strategy");
				
		this.metricsGeneration(db, workload);		
		workload.print(db);		
	}
	
	public void strategy1(Database db, Workload workload) {		
		// Create Mapping Matrix
		MappingTable mappingTable = new MappingTable();		
		Matrix mapping = mappingTable.generateMappingTable(db, workload);
		System.out.println("[ACT] Generating Data Movement Mapping Matrix ...\n   [First Row: Pre-Partition Id, First Col: Cluster Id, Elements: Data Occurance Counts]");
		//mapping.print();
		
		// Create Key-Value (Destination PID-Cluster ID) Mappings from Mapping Matrix
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>();
		MatrixElement colMax;
		for(int col = 1; col < mapping.getN(); col++) {
			colMax = mapping.findColMax(col);
			keyMap.put(colMax.getCol_pos()-1, colMax.getRow_pos()-1); // which cluster will go to which partition
			//System.out.println("-#-Col("+col+") [ACT] C"+(colMax.getCol_pos()-1)+"|P"+(colMax.getRow_pos()-1));
		}
		
		// Perform Actual Data Movement
		this.move(db, workload, keyMap);
		//workload.setWrl_interPartitionDataMovements(movements);
		workload.setWrl_hasDataMoved(true);
		workload.setMessage("Strategy-1");
		
		this.metricsGeneration(db, workload);
		workload.print(db);		
	}
	
	public void strategy2(Database db, Workload workload) {		
		// Create Mapping Matrix
		MappingTable mappingTable = new MappingTable();		
		Matrix mapping = mappingTable.generateMappingTable(db, workload);
		System.out.println("[ACT] Generating Data Movement Mapping Matrix ...\n   [First Row: Pre-Partition Id, First Col: Cluster Id, Elements: Data Occurance Counts]");
		mapping.print();
				
		// Step-1 :: Max Movement Matrix Formation
		MatrixElement max;
		int diagonal_pos = 1;		
		
		for(int m = 1; m < mapping.getM(); m++) {
			max = mapping.findMax(diagonal_pos);
			//System.out.println("[ACT] Max: "+max.getCounts()+", Col: "+(max.getCol_pos()+1)+", Row: "+(max.getRow_pos()+1));
			
			// Row/Col swap with diagonal Row/Col
			if(max.getCounts() != 0) {
				mapping.swap_row(max.getRow_pos(), diagonal_pos);
				mapping.swap_col(max.getCol_pos(), diagonal_pos);
			}			
			
			++diagonal_pos;
		}		

		// @debug
		System.out.println("[ACT] Creating Movement Matrix after Sub Matrix Max calculation ...");
		mapping.print();
		
		// Step-2 :: PID Conversion		
		// Create the PID conversion Key Map
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>(); 
		for(int row = 1; row < mapping.getM(); row++) {
			keyMap.put((int)mapping.getMatrix()[0][row].getCounts(), (int)mapping.getMatrix()[row][0].getCounts());
			//System.out.println("-#-Row("+row+" [ACT] C"+(int)mapping.getMatrix()[0][row].getCounts()+"|P"+(int)mapping.getMatrix()[row][0].getCounts());
		}
	
		// Perform Actual Data Movement	
		this.move(db, workload, keyMap);
		workload.setWrl_hasDataMoved(true);				
		workload.setMessage("Strategy-2");

		this.metricsGeneration(db, workload);			
		workload.print(db);
	}
	
	private void move(Database db, Workload workload, Map<Integer, Integer> keyMap) {
		// Perform Actual Data Movement
		// Stage-1: Within the Workload Data List
		//Data wrlData;				
		Data roaming_data;
		Partition home_partition = null;
		Partition old_partition = null;
		Partition dst_partition = null;
		int old_partition_id = -1;
		int dst_partition_id = -1;
		int old_node_id = -1;
		int dst_node_id = -1;
		int inter_partition_movements = 0;
		int inter_node_movements = 0;
		
		//for(Entry<Integer, Set<Data[ACT] entry : workload.getWrl_trDataMap().entrySet()) {
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
			//for(Data wrlData : entry.getValue()) {
				for(Data wrlData : transaction.getTr_dataSet()) {			
					home_partition = db.getDb_partition_table().getPartition(wrlData.getData_home_partition_id());
				
					old_partition_id = wrlData.getData_partition_id();
					old_partition = db.getDb_partition_table().getPartition(old_partition_id);
					old_node_id = db.getDb_partition_table().lookup(old_partition_id);			
				
					if(wrlData.getData_hmetis_cluster_id() != -1) {
						dst_partition_id = keyMap.get(wrlData.getData_hmetis_cluster_id());
						dst_partition = db.getDb_partition_table().getPartition(dst_partition_id);
						dst_node_id = db.getDb_partition_table().lookup(dst_partition_id);
					} else {
						dst_partition_id = old_partition_id;
					}
					//System.out.print("\n-#"+wrlData.toString()+" || dst-P"+dst_partition_id);			
				
					if(old_partition_id != dst_partition_id) { // Data needs to be moved
						wrlData.setData_hmetis_cluster_id(-1);
						wrlData.setData_shadow_hmetis_id(-1);
						wrlData.setData_hasShadowHMetisId(false);
					
						if(wrlData.isData_isRoaming()) { // Data is already Roaming
						//System.out.print("\n [ACT] @debug :: *R"+d+"-"+wrlData.toString());
						
							// Case-1: Returning to Home
							if(dst_partition_id != wrlData.getData_home_partition_id()) {
								wrlData.setData_isRoaming(false);
								wrlData.setData_isPartitionRoaming(false);												
							
								roaming_data = this.createRoamingData(wrlData, dst_partition_id, dst_node_id);
													
								home_partition.getPartition_data_items().add(roaming_data);
								home_partition.getRoaming_data_items().remove(wrlData.getData_id());						
							
								old_partition.getForeign_data_items().remove(wrlData);
							} else { // Case-2: Roaming to another Partition
								wrlData.setData_roaming_partition_id(dst_partition_id);
								if(old_node_id != dst_node_id) {
									wrlData.setData_roaming_node_id(dst_node_id);					
									wrlData.setData_isNodeRoaming(true);
									++inter_node_movements;
								}
							
								roaming_data = this.createRoamingData(wrlData, dst_partition_id, dst_node_id);
							
								home_partition.getRoaming_data_items().remove(wrlData.getData_id());
								home_partition.getRoaming_data_items().put(wrlData.getData_id(), dst_partition_id);
							
								dst_partition.getForeign_data_items().add(roaming_data);
								old_partition.getForeign_data_items().remove(wrlData);
							}						
	
							++inter_partition_movements;
						} else { // Data will be Roaming for the first time
							wrlData.setData_isRoaming(true);
							wrlData.setData_isPartitionRoaming(true);
							wrlData.setData_roaming_partition_id(dst_partition_id);
							
							if(old_node_id != dst_node_id) {
								wrlData.setData_roaming_node_id(dst_node_id);					
								wrlData.setData_isNodeRoaming(true);
								++inter_node_movements;
							}										
					
							roaming_data = this.createRoamingData(wrlData, dst_partition_id, dst_node_id);
						
							// Add the Roaming Data into the destination Partition's Foreign Data Item List
							// Add an entry in the Old Partition Table's Roaming Data Item Table
							// Remove the Data item from Old Partition's Data Item List
							dst_partition.getForeign_data_items().add(roaming_data);
							old_partition.getRoaming_data_items().put(wrlData.getData_id(), dst_partition_id);
							old_partition.getPartition_data_items().remove(wrlData);
	
							++inter_partition_movements;						
						} // end -- if-else()			
					} // end -- if()
				} // end -- for()-Data
			} // end -- for()-Transaction
		} // end -- for()-Transaction-Type
		
		workload.setWrl_interPartitionDataMovements(inter_partition_movements);
		workload.setWrl_interNodeDataMovements(inter_node_movements);
	}
	
	// Create a Roaming Data from the Workload Data
	public Data createRoamingData(Data wrlData, int dst_partition_id, int dst_node_id) {
		Data roaming_data = new Data(wrlData); // Cloning the workload Data item
		roaming_data.setData_partition_id(dst_partition_id);					
		roaming_data.setData_roaming_partition_id(-1);
		roaming_data.setData_node_id(dst_node_id);
		roaming_data.setData_roaming_node_id(-1);				
		
		return roaming_data;
	}
	
	public void metricsGeneration(Database db, Workload workload) {
		System.out.println("\n[MSG] Total "+workload.getWrl_interPartitionDataMovements()+" Data movements are required using "
				+workload.getMessage()+".");
		
		// Generating Workload's Data Partition and Node Distribution Details
		workload.generateDataPartitionTable();
		workload.generateDataNodeTable();
		
		// Calculating Various Metrics
		workload.calculateDTPercentage();
		workload.calculateDTImapct();
		workload.calculatePDMVPercentage(workload.getWrl_interPartitionDataMovements());
		workload.calculateNDMVPercentage(workload.getWrl_interNodeDataMovements());
		
		for(Entry<Integer, Set<Partition>> entry : db.getDb_partition_table().getPartition_table().entrySet()) {
			for(Partition partition : entry.getValue()) {
				partition.calculateMainOccupied();
				partition.calculateRoamingOccupied();
				partition.calculateForeignOccupied();
				partition.calculateCurrentLoad();
			}						
		}
	}
}