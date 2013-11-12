/**
 * @author Joarder Kamal
 */

package jkamal.prototype.db;

import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

public class Database {
	private int db_id;
	private String db_name;
	private int db_tenant;
	private GlobalDataMap db_dataMap;
	private Set<Partition> db_partitions;
	private PartitioningModel db_partition_model; // Options: Range, Salting, Hash (Random), Consistent-Hash (Random)
	private int db_partition_size;
	private PartitionTable db_partition_table;
	private RoutingTable db_routing_table;
	private String db_dmv_strategy;	
	
	public Database(int id, String name, int tenant_id, String model, double partition_size) {
		this.setDb_id(id);
		this.setDb_name(name);
		this.setDb_tenant(tenant_id);
		this.setDb_dataMap(new GlobalDataMap());
		this.setDb_partitions(new TreeSet<Partition>());
		this.setDb_partition_size((int)(partition_size * 1000)); // Partition Size Range (1 ~ 1000 GB), 1 GB = 1000 Data Objects of equal size
		this.setDb_partition_table(new PartitionTable());
		this.setDb_routing_table(new RoutingTable());
		this.setDb_dmv_strategy("bs");
		
		if(model.equals("Range"))
			this.setDb_partition_model(new RangePartition());
		else if(model.equals("Hash"))
			this.setDb_partition_model(new RandomPartition());
		else
			System.out.println();
	}	
	
	// Copy Constructor
	public Database(Database db) {
		this.setDb_id(db.getDb_id());
		this.setDb_name(db.getDb_name());
		this.setDb_tenant(db.getDb_tenant());		
		this.setDb_dataMap(new GlobalDataMap(db.getDb_dataMap()));
		
		Set<Partition> cloneDbPartitions = new TreeSet<Partition>();
		Partition clonePartition;
		for(Partition partition : db.getDb_partitions()) {
			clonePartition = new Partition(partition);
			cloneDbPartitions.add(clonePartition);
		}		
		this.setDb_partitions(cloneDbPartitions);
		
		this.setDb_partition_size(db.getDb_partition_size());		
		this.setDb_partition_table(new PartitionTable(db.getDb_partitionTable()));
		//this.db_routing_table = new RoutingTable(db.getDb_routing_table());
		this.setDb_dmv_strategy(db.getDb_dmv_strategy());
	}

	public int getDb_id() {
		return db_id;
	}

	public void setDb_id(int db_id) {
		this.db_id = db_id;
	}

	public String getDb_name() {
		return db_name;
	}

	public void setDb_name(String db_name) {
		this.db_name = db_name;
	}

	public int getDb_tenant() {
		return db_tenant;
	}

	public void setDb_tenant(int db_tenant) {
		this.db_tenant = db_tenant;
	}
	
	public GlobalDataMap getDb_dataMap() {
		return db_dataMap;
	}

	public void setDb_dataMap(GlobalDataMap db_dataMap) {
		this.db_dataMap = db_dataMap;
	}

	public Set<Partition> getDb_partitions() {
		return db_partitions;
	}

	public void setDb_partitions(Set<Partition> db_partitions) {
		this.db_partitions = db_partitions;
	}
	
	
	public PartitioningModel getDb_partition_model() {
		return db_partition_model;
	}

	public void setDb_partition_model(PartitioningModel db_partition_model) {
		this.db_partition_model = db_partition_model;
	}

	public int getDb_partition_size() {
		return db_partition_size;
	}

	public void setDb_partition_size(int db_partition_size) {
		this.db_partition_size = db_partition_size;
	}

	public PartitionTable getDb_partitionTable() {
		return db_partition_table;
	}

	public void setDb_partition_table(PartitionTable db_partition_table) {
		this.db_partition_table = db_partition_table;
	}

	public RoutingTable getDb_routing_table() {
		return db_routing_table;
	}

	public void setDb_routing_table(RoutingTable db_routing_table) {
		this.db_routing_table = db_routing_table;
	}

	public String getDb_dmv_strategy() {
		return db_dmv_strategy;
	}

	public void setDb_dmv_strategy(String db_dmv_strategy) {
		this.db_dmv_strategy = db_dmv_strategy;
	}

	public void insert() {
		
	}
	
	public void update() {
		
	}
	
	// Returns the Node Id where the actual Data item resides. -1 means data NOT Found.
	public int search(Data data) {
		int node_id = -1;
		
		
		return node_id;
	}
	
	public void delete() {
		
	}
	
	public void print() {
		// DB Details
		//System.out.println();
		System.out.println("[OUT] Database Details====");
		System.out.println("      Database: "+this.getDb_name());
		System.out.println("      Number of Partitions: "+this.getDb_partitions().size());
		
		// Partition Table Details
		//System.out.println();
		System.out.println("[OUT] Partition Table Details====");		
		
		int comma = -1;
		for(Entry<Integer, Set<Partition>> entry : this.getDb_partitionTable().getPartition_table().entrySet()) {
			System.out.print("      N"+entry.getKey()+" {");
			
			comma = entry.getValue().size();
			for(Partition partition : entry.getValue()) {
				System.out.print(partition.toString());
				
				if(comma != 1)
					System.out.print(", ");
			
				--comma;
			}
			
			System.out.print("}\n");			
		}		
	}
}