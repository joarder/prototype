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
	
	private Set<Partition> db_partitions;
	private PartitioningModel db_partition_model; // Options: Range, Salting, Hash (Random), Consistent-Hash (Random)
	private int db_partition_size;
	
	private String db_dmv_strategy;	
	
	public Database(int id, String name, int tenant_id, String model, double partition_size) {
		this.setDb_id(id);
		this.setDb_name(name);
		this.setDb_tenant(tenant_id);

		this.setDb_partitions(new TreeSet<Partition>());
		this.setDb_partition_size((int)(partition_size * 1000)); // Partition Size Range (1 ~ 1000 GB), 1 GB = 1000 Data Objects of equal size

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
		
		Set<Partition> cloneDbPartitions = new TreeSet<Partition>();
		Partition clonePartition;
		for(Partition partition : db.getDb_partitions()) {
			clonePartition = new Partition(partition);
			cloneDbPartitions.add(clonePartition);
		}		
		this.setDb_partitions(cloneDbPartitions);
		
		this.setDb_partition_size(db.getDb_partition_size());				
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

	public String getDb_dmv_strategy() {
		return db_dmv_strategy;
	}

	public void setDb_dmv_strategy(String db_dmv_strategy) {
		this.db_dmv_strategy = db_dmv_strategy;
	}

	public boolean insert() {
		boolean success = false;
		
		
		return success;
	}
	
	public boolean update() {
		boolean success = false;
		
		
		return success;
	}
	
	// Searches for a specific Data by it's Id
	public Data search(int data_id) {		
		for(Partition partition : this.getDb_partitions()) {
			int partition_id = partition.lookupPartitionId_byDataId(data_id);
			
			if(partition_id != -1) 
				return(partition.getData_byDataId(data_id));
		}			
		
		return null;
	}
	
	public boolean delete(int data_id) {
		boolean success = false;
		
		
		return success;
	}
	
	public Partition getPartition(int partition_id) {
		for(Partition partition : this.getDb_partitions()) {						
			if(partition.getPartition_id() == partition_id) 
				return partition;
		}
		
		return null;
	}
	
	public void show() {
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