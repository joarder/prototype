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
import java.util.TreeSet;
import java.util.Map.Entry;
import jkamal.prototype.util.Matrix;
import jkamal.prototype.util.MatrixElement;
import jkamal.prototype.workload.MappingTable;
import jkamal.prototype.workload.Transaction;
import jkamal.prototype.workload.Workload;

public class DataMovement {
	private int intra_node_data_movements;
	private int inter_node_data_movements;
	
	public DataMovement() {}

	private int getIntra_node_data_movements() {
		return intra_node_data_movements;
	}

	private void setIntra_node_data_movements(int intra_node_data_movements) {
		this.intra_node_data_movements = intra_node_data_movements;
	}
	
	private void incIntra_node_data_movements() {
		int intra = this.getIntra_node_data_movements();
		this.setIntra_node_data_movements(++intra);
	}

	private int getInter_node_data_movements() {
		return inter_node_data_movements;
	}

	private void setInter_node_data_movements(int inter_node_data_movements) {
		this.inter_node_data_movements = inter_node_data_movements;
	}
	
	private void incInter_node_data_movements() {
		int inter = this.getInter_node_data_movements();
		this.setInter_node_data_movements(++inter);
	}

	public void baseStrategy(Database db, Workload workload) {
		workload.setWrl_interNodeDataMovements(0);
		workload.setWrl_intraNodeDataMovements(0);
		
		this.metricsGeneration(db, workload);
		
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
		workload.setMessage("bs");
				
		this.metricsGeneration(db, workload);		
		workload.show(db);		
	}
	
	public void strategy1(Database db, Workload workload) {
		workload.setWrl_interNodeDataMovements(0);
		workload.setWrl_intraNodeDataMovements(0);
		
		this.metricsGeneration(db, workload);
		
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
			keyMap.put(colMax.getCol_pos(), colMax.getRow_pos()); // which cluster will go to which partition
			//System.out.println("-#-Col("+col+") [ACT] C"+(colMax.getCol_pos())+"|P"+(colMax.getRow_pos()));
		}
		
		// Perform Actual Data Movement
		this.move(db, workload, keyMap);
		workload.setWrl_hasDataMoved(true);
		workload.setMessage("s1");
		
		this.metricsGeneration(db, workload);
		workload.show(db);		
	}
	
	public void strategy2(Database db, Workload workload) {	
		workload.setWrl_interNodeDataMovements(0);
		workload.setWrl_intraNodeDataMovements(0);
		
		this.metricsGeneration(db, workload);
		
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
		workload.setMessage("s2");

		this.metricsGeneration(db, workload);			
		workload.show(db);
	}
	
	private void updateData(Data data, int dst_partition_id, int dst_node_id, boolean roaming) {		
		data.setData_partitionId(dst_partition_id);					
		data.setData_nodeId(dst_node_id);
		
		if(roaming)
			data.setData_isRoaming(true);
		else
			data.setData_isRoaming(false);
	}
	
	private void updatePartition(Database db, Data data, int current_partition_id, int dst_partition_id) {					
		Partition current_partition = db.getPartition(current_partition_id);
		Partition dst_partition = db.getPartition(dst_partition_id);
		Partition home_partition = db.getPartition(data.getData_homePartitionId());
		
		// Actual Movement
		dst_partition.getPartition_dataSet().add(data);		
		current_partition.getPartition_dataSet().remove(data);
		
		// Update Lookup Table
		home_partition.getPartition_dataLookupTable().remove(data.getData_id());
		home_partition.getPartition_dataLookupTable().put(data.getData_id(), dst_partition_id);						
	}
	
	private void updateMovementCounts(int dst_node_id, int current_node_id) {
		if(dst_node_id != current_node_id)
			this.incInter_node_data_movements();
		else
			this.incIntra_node_data_movements();
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
		
		Set<Integer> dataSet = new TreeSet<Integer>();
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {		
				for(Data data : transaction.getTr_dataSet()) {					
					Data dbData = db.search(data.getData_id());
					
					if(!dataSet.contains(dbData.getData_id())) {
						dataSet.add(dbData.getData_id());
						
						home_partition_id = dbData.getData_homePartitionId();
						home_partition = db.getPartition(dbData.getData_homePartitionId());
						home_node_id = dbData.getData_homeNodeId();												
						
						current_partition_id = dbData.getData_partitionId();									
						current_partition = db.getPartition(current_partition_id);
						current_node_id = dbData.getData_nodeId();			
						
						dst_partition_id = keyMap.get(dbData.getData_hmetisClusterId());
						dst_partition = db.getPartition(dst_partition_id);
						dst_node_id = dst_partition.getPartition_nodeId();
						
						dbData.setData_hmetisClusterId(-1);						
						
						if(dst_partition_id != current_partition_id) { // Data needs to be moved					
							if(dbData.isData_isRoaming()) { // Data is already Roaming
								if(dst_partition_id == home_partition_id) {
									this.updateData(dbData, dst_partition_id, dst_node_id, false);
									this.updatePartition(db, dbData, current_partition_id, dst_partition_id);
									this.updateMovementCounts(dst_node_id, current_node_id);
									
									current_partition.decPartition_foreign_data();
									home_partition.decPartition_roaming_data();
									
								} else if(dst_partition_id == current_partition_id) {									
									// Nothing to do									
								} else {
									this.updateData(dbData, dst_partition_id, dst_node_id, true);
									this.updatePartition(db, dbData, current_partition_id, dst_partition_id);
									this.updateMovementCounts(dst_node_id, current_node_id);
									
									dst_partition.incPartition_foreign_data();
									current_partition.decPartition_foreign_data();
									
								}
							} else {
								this.updateData(dbData, dst_partition_id, dst_node_id, true);
								this.updatePartition(db, dbData, current_partition_id, dst_partition_id);
								this.updateMovementCounts(dst_node_id, current_node_id);
								
								dst_partition.incPartition_foreign_data();								
								home_partition.incPartition_roaming_data();
							}
						}
					}
								
								
								
								
								
								
								
								
								/*
								//System.out.print("@debug >> Roaming ? "+dbData.isData_isRoaming());
								// Case-1: Roaming within/Returning to Home Node
								// Case-1a: Roaming within/Returning to Home Node and Home Partition
								if(dst_node_id == home_node_id) {									
									if(dst_partition_id == home_partition_id) {
										//data.setData_isRoaming(false);										
										//data.setData_partitionId(dst_partition_id);					
										//data.setData_nodeId(dst_node_id);
										this.refreshDbData(dbData, dst_partition_id, dst_node_id, false);
															
										home_partition.getPartition_dataSet().add(dbData);																
										home_partition.decPartition_roaming_data();
										
										current_partition.decPartition_foreign_data();
										current_partition.getPartition_dataSet().remove(dbData);
										
										if(dst_node_id != current_node_id)
											++inter_node_data_movements;
										else
											++intra_node_data_movements;
									} else { 
								// Case-1b: Roaming within/Returning to Home Node but different Partition
										//data.setData_isRoaming(true);
										//data.setData_partitionId(dst_partition_id);					
										//data.setData_nodeId(dst_node_id);
										this.refreshDbData(dbData, dst_partition_id, dst_node_id, true);
										
										dst_partition.getPartition_dataSet().add(dbData);
										dst_partition.incPartition_foreign_data();										
										
										//Update Partition Data lookup table
										home_partition.getPartition_dataLookupTable().remove(dbData.getData_id());
										home_partition.getPartition_dataLookupTable().put(dbData.getData_id(), dst_partition_id);
										
										current_partition.decPartition_foreign_data();
										current_partition.getPartition_dataSet().remove(dbData);
										
										if(dst_node_id != current_node_id)
											++inter_node_data_movements;
										else
											++intra_node_data_movements;
									}
								} else if(dst_node_id == current_node_id) {									 
								// Case-1c: Roaming within current Node but different Partition
										//data.setData_isRoaming(true);
										//data.setData_partitionId(dst_partition_id);					
										//data.setData_nodeId(dst_node_id);
										this.refreshDbData(dbData, dst_partition_id, dst_node_id, true);
										
										dst_partition.getPartition_dataSet().add(dbData);
										dst_partition.incPartition_foreign_data();										
										
										//Update Partition Data lookup table
										home_partition.getPartition_dataLookupTable().remove(dbData.getData_id());
										home_partition.getPartition_dataLookupTable().put(dbData.getData_id(), dst_partition_id);
										
										current_partition.decPartition_foreign_data();
										current_partition.getPartition_dataSet().remove(dbData);
										
										if(dst_node_id != current_node_id)
											++inter_node_data_movements;
										else
											++intra_node_data_movements;									
								} else { 
									// Case-2: Roaming to another Partition in another Node		
									//data.setData_isRoaming(true);
									//data.setData_partitionId(dst_partition_id);					
									//data.setData_nodeId(dst_node_id);
									this.refreshDbData(dbData, dst_partition_id, dst_node_id, true);
								
									dst_partition.getPartition_dataSet().add(dbData);
									dst_partition.incPartition_foreign_data();
																												
									//Update Partition Data lookup table
									home_partition.getPartition_dataLookupTable().remove(dbData.getData_id());
									home_partition.getPartition_dataLookupTable().put(dbData.getData_id(), dst_partition_id);
									
									current_partition.decPartition_foreign_data();
									current_partition.getPartition_dataSet().remove(dbData);
									
									++inter_node_data_movements;
								}
							} else { // Case-3: Data will be Roamed for the first time
								//data.setData_isRoaming(true);
								//data.setData_partitionId(dst_partition_id);					
								//data.setData_nodeId(dst_node_id);
								this.refreshDbData(dbData, dst_partition_id, dst_node_id, true);
							
								// Add the Roaming Data into the destination Partition's Foreign Data Item List
								// Add an entry in the Old Partition Table's Roaming Data Item Table
								// Remove the Data item from Old Partition's Data Item List
								dst_partition.getPartition_dataSet().add(dbData);
								dst_partition.incPartition_foreign_data();								
								
								//Update Partition Data lookup table
								home_partition.getPartition_dataLookupTable().remove(dbData.getData_id());
								home_partition.getPartition_dataLookupTable().put(dbData.getData_id(), dst_partition_id);
								
								home_partition.incPartition_roaming_data();
								home_partition.getPartition_dataSet().remove(dbData);								
								
								if(dst_node_id != current_node_id)
									++inter_node_data_movements;
								else
									++intra_node_data_movements;
							} // end -- if-else()																	
						}
																				
						} else { // Case-5 : Repeated Data
							//++repeated_data;						
						} // end -- if-else()*/										
					} // end -- for()-Data
				} // end -- for()-Transaction
			} // end -- for()-Transaction-Type		
		
		//System.out.println(">> * Repeated Data: "+repeated_data);
		
		workload.setWrl_intraNodeDataMovements(this.getIntra_node_data_movements());
		workload.setWrl_interNodeDataMovements(this.getInter_node_data_movements());
	}
	
	public void refreshDatabase(Database db, Workload workload) {
		Set<Integer> dataSet = new TreeSet<Integer>();
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {		
				for(Data workload_data : transaction.getTr_dataSet()) {
					if(!dataSet.contains(workload_data.getData_id())) {
						Data data = db.search(workload_data.getData_id());
	
						System.out.println("@debug >> w*"+workload_data.toString()+" | "+data.toString());
						
						data.setData_nodeId(workload_data.getData_nodeId());
						data.setData_partitionId(workload_data.getData_partitionId());
						data.setData_isRoaming(workload_data.isData_isRoaming());
						
						System.out.println("@debug >> #"+data.toString());
						
						dataSet.add(data.getData_id());
					}
				}
			}
		}
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
		workload.calculateDTImapct(db);
		workload.calculateIntraNodeDataMovementPercentage(workload.getWrl_intraNodeDataMovements());
		workload.calculateInterNodeDataMovementPercentage(workload.getWrl_interNodeDataMovements());
	}
}