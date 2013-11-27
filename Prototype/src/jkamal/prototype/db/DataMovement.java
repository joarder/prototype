/**
 * @author Joarder Kamal
 * 
 * Perform Data Movement after analysing Workload using HyperGraph Partitioning 
 */

package jkamal.prototype.db;

import java.util.ArrayList;
import java.util.Map;
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
		workload.show(db);		
	}
	
	public void strategy1(Database db, Workload workload) {		
		// Create Mapping Matrix
		MappingTable mappingTable = new MappingTable();		
		Matrix mapping = mappingTable.generateMappingTable(db, workload);
		System.out.println("[ACT] Generating Data Movement Mapping Matrix ...\n" +
				"   [First Row: Pre-Partition Id, First Col: Cluster Id, Elements: Data Occurance Counts]");
		mapping.print();
		
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
		workload.setWrl_hasDataMoved(true);
		workload.setMessage("Strategy-1");
		
		this.metricsGeneration(db, workload);
		workload.show(db);		
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
		workload.show(db);
	}
	
	// Perform Actual Data Movement
	private void move(Database db, Workload workload, Map<Integer, Integer> keyMap) {
		Partition home_partition = null;
		Partition current_partition = null;
		Partition dst_partition = null;
		int home_partition_id = -1;
		int current_partition_id = -1;
		int dst_partition_id = -1;
		int home_node_id = -1;
		int current_node_id = -1;		
		int dst_node_id = -1;
		int intra_node_data_movements = 0;
		int inter_node_data_movements = 0;
		int repeated_data = 0;		
		
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {		
				for(Data workload_data : transaction.getTr_dataSet()) {						
					if(workload_data.getData_hmetisClusterId() != -1) {// && !observed_workload_data.contains(workload_data)) { // to check for repeated data						
						home_partition_id = workload_data.getData_homePartitionId();
						home_partition = db.getPartition(workload_data.getData_homePartitionId());
						home_node_id = workload_data.getData_homeNodeId();						
						
						current_partition_id = workload_data.getData_partitionId();									
						current_partition = db.getPartition(current_partition_id);
						current_node_id = workload_data.getData_nodeId();			
						
						dst_partition_id = keyMap.get(workload_data.getData_hmetisClusterId());
						dst_partition = db.getPartition(dst_partition_id);
						dst_node_id = dst_partition.getPartition_nodeId();							
						
						workload_data.setData_hmetisClusterId(-1);
						if(workload.getWrl_data_movement_strategy() != "bs") {
							updateHMetisClusterId(workload, workload_data);
						}						
						
						//System.out.println("@debug >> "+workload_data.toString()+" | Dpid/Dnid = P"+dst_partition_id+"/N"+dst_node_id);
						
						if(dst_node_id != current_node_id) { // Data needs to be moved within the Nodes						
							if(workload_data.isData_isRoaming()) { // Data is already Roaming
								// Case-1: Returning to Home Node
								if(dst_node_id == home_node_id) { // Case-1a: Returning to Home Node and Home Partition									
									if(dst_partition_id == home_partition_id) {
										//roaming_data = this.createRoamingData(workload_data, dst_partition_id, dst_node_id, false);
										workload_data.setData_isRoaming(false);
										workload_data.setData_partitionId(dst_partition_id);					
										workload_data.setData_nodeId(dst_node_id);
															
										home_partition.getPartition_dataSet().add(workload_data);
										//home_partition.getRoaming_dataObjects().remove(workload_data.getData_id());						
										home_partition.decPartition_roaming_data();
										
										//current_partition.getForeign_dataObjects().remove(new Integer(workload_data.getData_id()));
										current_partition.decPartition_foreign_data();
										current_partition.getPartition_dataSet().remove(workload_data);									
										
										//System.out.println("@debug (Returning to Home Node/Home Partition)->> ");
										
										//System.out.println("============");
										//current_partition.printContents();
										//dst_partition.printContents();
										//System.out.println("============");
										
										++inter_node_data_movements;
									} else { // Case-1b: Returning to Home Node but different Partition
										//roaming_data = this.createRoamingData(workload_data, dst_partition_id, dst_node_id, true);
										workload_data.setData_isRoaming(true);
										workload_data.setData_partitionId(dst_partition_id);					
										workload_data.setData_nodeId(dst_node_id);
										
										dst_partition.getPartition_dataSet().add(workload_data);
										//dst_partition.getForeign_dataObjects().add(workload_data.getData_id());
										dst_partition.incPartition_foreign_data();
										
										//home_partition.getRoaming_dataObjects().remove(workload_data.getData_id());
										//home_partition.getRoaming_dataObjects().put(workload_data.getData_id(), dst_partition_id);
										
										//Update Partition Data lookup table
										home_partition.getPartition_dataLookupTable().remove(workload_data.getData_id());
										home_partition.getPartition_dataLookupTable().put(workload_data.getData_id(), dst_partition_id);
										
										//current_partition.getForeign_dataObjects().remove((Integer)workload_data.getData_id());
										current_partition.decPartition_foreign_data();
										current_partition.getPartition_dataSet().remove(workload_data);
										
										//System.out.println("@debug (Returning to Home Node/Different Partition)->> ");
										
										//System.out.println("============");
										//current_partition.printContents();
										//dst_partition.printContents();
										//System.out.println("============");
										
										++inter_node_data_movements;
									}
								} else { 
									// Case-2: Roaming to another Partition in another Node		
									//roaming_data = this.createRoamingData(workload_data, dst_partition_id, dst_node_id, true);
									workload_data.setData_isRoaming(true);
									workload_data.setData_partitionId(dst_partition_id);					
									workload_data.setData_nodeId(dst_node_id);
								
									dst_partition.getPartition_dataSet().add(workload_data);
									//dst_partition.getForeign_dataObjects().add(workload_data.getData_id());
									dst_partition.incPartition_foreign_data();
									
									//home_partition.getRoaming_dataObjects().remove(workload_data.getData_id());
									//home_partition.getRoaming_dataObjects().put(workload_data.getData_id(), dst_partition_id);																	
									
									//Update Partition Data lookup table
									home_partition.getPartition_dataLookupTable().remove(workload_data.getData_id());
									home_partition.getPartition_dataLookupTable().put(workload_data.getData_id(), dst_partition_id);
									
									
									//current_partition.getForeign_dataObjects().remove((Integer)workload_data.getData_id());
									current_partition.decPartition_foreign_data();
									current_partition.getPartition_dataSet().remove(workload_data);		
																		
									//System.out.println("@debug (Roaming to Another Partition)->> ");
									
									//System.out.println("============");
									//current_partition.printContents();
									//dst_partition.printContents();
									//System.out.println("============");
									
									++inter_node_data_movements;
								}
							} else { // Case-3: Data will be Roamed for the first time																
								//roaming_data = this.createRoamingData(workload_data, dst_partition_id, dst_node_id, true);
								workload_data.setData_isRoaming(true);
								workload_data.setData_partitionId(dst_partition_id);					
								workload_data.setData_nodeId(dst_node_id);
							
								// Add the Roaming Data into the destination Partition's Foreign Data Item List
								// Add an entry in the Old Partition Table's Roaming Data Item Table
								// Remove the Data item from Old Partition's Data Item List
								dst_partition.getPartition_dataSet().add(workload_data);
								//dst_partition.getForeign_dataObjects().add(workload_data.getData_id());
								dst_partition.incPartition_foreign_data();
								
								//home_partition.getRoaming_dataObjects().put(workload_data.getData_id(), dst_partition_id);
								
								//Update Partition Data lookup table
								home_partition.getPartition_dataLookupTable().remove(workload_data.getData_id());
								home_partition.getPartition_dataLookupTable().put(workload_data.getData_id(), dst_partition_id);
								
								home_partition.getPartition_dataSet().remove(workload_data);
																
								//System.out.println("@debug (First Time Roaming)->> ");

								//System.out.println("============");
								//home_partition.printContents();
								//dst_partition.printContents();
								//System.out.println("============");
								
								++inter_node_data_movements;								
							} // end -- if-else()																	
						} else { // Case-4 : Avoiding Intra Node Data Movement
							++intra_node_data_movements; // Only counted
							//System.out.println("@debug (Intra Node Movement)->> "+workload_data.toString());
						} // end -- if-else()												
					} else { // Case-5 : Repeated Data
						++repeated_data;
						//System.out.println("@debug *(Repeated)->> "+workload_data.toString());
					} // end -- if-else()										
				} // end -- for()-Data
			} // end -- for()-Transaction
		} // end -- for()-Transaction-Type
		
		//System.out.println(">> * Intra (Avoided): "+intra_node_data_movements);
		//System.out.println(">> * Inter (Performed): "+inter_node_data_movements);
		System.out.println(">> * Repeated Data: "+repeated_data);
		
		workload.setWrl_intraNodeDataMovements(intra_node_data_movements);
		workload.setWrl_interNodeDataMovements(inter_node_data_movements);
	}
	
	// Create a Roaming Data from the Workload Data
	public Data createRoamingData(Data workload_data, int dst_partition_id, int dst_node_id, boolean is_roaming) {
		Data data = new Data(workload_data); // Cloning the workload Data item
		data.setData_isRoaming(is_roaming);
		data.setData_partitionId(dst_partition_id);					
		data.setData_nodeId(dst_node_id);
		
		return data;
	}
	
	public void updateHMetisClusterId(Workload workload, Data workload_data) {
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {		
				for(Data data : transaction.getTr_dataSet()) {						
					if(data.getData_id() == workload_data.getData_id()) {
						data.setData_hmetisClusterId(-1);
					}
				}
			}
		}
	}
	
	public void metricsGeneration(Database db, Workload workload) {
		// Calculating Various Metrics
		workload.calculateDTPercentage();
		workload.calculateDTImapct();
		workload.calculateIntraNodeDataMovementPercentage(workload.getWrl_intraNodeDataMovements());
		workload.calculateInterNodeDataMovementPercentage(workload.getWrl_interNodeDataMovements());
	}
}