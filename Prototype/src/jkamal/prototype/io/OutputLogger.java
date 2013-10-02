/**
 * @author Joarder Kamal
 */

package jkamal.prototype.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.TreeMap;
import jkamal.prototype.db.Database;
import jkamal.prototype.db.DatabaseServer;
import jkamal.prototype.db.Node;
import jkamal.prototype.db.Partition;
import jkamal.prototype.workload.Workload;

public class OutputLogger {	
	private String db_logger;
	private String workload_logger;
	private String partition_logger;
	private boolean data_movement;
	private Map<Integer, String> partitionsBeforeDM;
	
	public OutputLogger() {
		this.setDb_logger("jk-trace-db.txt");
		this.setWorkload_logger("jk-trace-workload.txt");
		this.setPartition_logger("jk-trace-partition.txt");
		this.setData_movement(false);
		this.setPartitionsBeforeDM(new TreeMap<Integer, String>());
	}
	
	public String getDb_logger() {
		return db_logger;
	}

	public void setDb_logger(String db_logger) {
		this.db_logger = db_logger;
	}

	public String getWorkload_logger() {
		return workload_logger;
	}

	public void setWorkload_logger(String workload_logger) {
		this.workload_logger = workload_logger;
	}

	public String getPartition_logger() {
		return partition_logger;
	}

	public void setPartition_logger(String partition_logger) {
		this.partition_logger = partition_logger;
	}

	public boolean isData_movement() {
		return data_movement;
	}

	public void setData_movement(boolean data_movement) {
		this.data_movement = data_movement;
	}

	public Map<Integer, String> getPartitionsBeforeDM() {
		return partitionsBeforeDM;
	}

	public void setPartitionsBeforeDM(Map<Integer, String> partitionsBeforeDM) {
		this.partitionsBeforeDM = partitionsBeforeDM;
	}

	public PrintWriter getWriter(String dir, String content) {
		String trace_type = null;		
		switch(content) {
		case "db": 
			trace_type = this.getDb_logger();
			break;
		case "workload":
			trace_type = this.getWorkload_logger();
			break;
		case "partition":
			trace_type = this.getPartition_logger();
			break;
		default:
			trace_type = null;
			System.out.println("[DBG] Invalid trace type !!!");
			break;
		}
		
		File logFile = new File(dir+"\\"+trace_type);
		PrintWriter prWriter = null;
		
		try {
			if(!logFile.exists())
				logFile.createNewFile();
			
			try {
				prWriter = new PrintWriter(new BufferedWriter(new FileWriter(logFile)));				
				
			} catch(IOException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {		
			e.printStackTrace();
		}
		
		return prWriter;
	}
	
	public void log(DatabaseServer dbs, Database db, PrintWriter prWriter) {
		int space = -1;
		
		try {							
			prWriter.print(Integer.toString(dbs.getDbs_nodes().size())+" ");			
			prWriter.print(Integer.toString(db.getDb_routing_table().getData_items().size())+" ");			
			prWriter.print(Integer.toString(db.getDb_dataMap().getData_items().size()));			
			prWriter.println();
			
			space = dbs.getDbs_nodes().size();
			for(Node node : dbs.getDbs_nodes()) { 
				prWriter.print(Integer.toString(node.getNode_partitions().size()));
				
				if(space != 1)
					prWriter.print(" ");
				
				--space;
			}
			
			prWriter.println();			
			
			for(Entry<Integer, Set<Partition>> entry : db.getDb_partition_table().getPartition_table().entrySet()) {				
				space = entry.getValue().size();
				for(Partition partition : entry.getValue()) {
					prWriter.print(Integer.toString(partition.getPartition_data_items().size())+" ");
					prWriter.print(Integer.toString(partition.getRoaming_data_items().size())+" ");
					prWriter.print(Integer.toString(partition.getForeign_data_items().size()));
					
					if(space != 1)
						prWriter.print(" ");
				
					--space;
				}				
				prWriter.println();			
			}			
		} finally {
			prWriter.flush();
			prWriter.close();
		} 
	}
	
	public void logWorkload(Database db, Workload workload, PrintWriter prWriter) {		
		if(!this.isData_movement()) {									
			prWriter.print(workload.getWrl_capture()+" ");
			prWriter.print(workload.getWrl_round()+" ");
			prWriter.print(workload.getWrl_percentageVariation()+" ");
			prWriter.print("in ");
			
			prWriter.print(workload.getWrl_dt_nums()+" ");
			prWriter.print(workload.getWrl_dt_impact()+" ");																		
		} else {					
			prWriter.print(db.getDb_dmv_strategy()+" ");
			
			prWriter.print(workload.getWrl_dt_nums()+" ");
			prWriter.print(workload.getWrl_dt_impact()+" ");								
			
			prWriter.print(workload.getWrl_totalData()+" ");
			prWriter.print(workload.getWrl_interPartitionDataMovements()+" ");
			prWriter.print(workload.getWrl_percentage_pdmv()+" ");
			prWriter.print(workload.getWrl_interNodeDataMovements()+" ");
			prWriter.print(workload.getWrl_percentage_ndmv());
			
			prWriter.println();			
		}				
	}
	
	public void logPartition(Database db, Workload workload, PrintWriter prWriter) {						
		if(!this.isData_movement()) {
			for(Entry<Integer, Set<Partition>> entry : db.getDb_partition_table().getPartition_table().entrySet()) {
				for(Partition partition : entry.getValue()) {
					this.getPartitionsBeforeDM().put(partition.getPartition_id(), 
							entry.getKey()+" "+partition.getPartition_id()+" "+this.logPartitionDetails(partition)+" ");
				}						
			}						
		} else {			
			for(Entry<Integer, Set<Partition>> entry : db.getDb_partition_table().getPartition_table().entrySet()) {							
				for(Partition partition : entry.getValue()) {
					prWriter.print(this.getPartitionsBeforeDM().get(partition.getPartition_id()));					
					prWriter.println(this.logPartitionDetails(partition));
				}						
			}						
		}
	}
	
	private String logPartitionDetails(Partition partition) {
		return (partition.getPartition_current_load()+" "
				+partition.getPartition_data_items().size()+" "+partition.getPartition_percentage_main()+" "
				+partition.getRoaming_data_items().size()+" "+partition.getPartition_percentage_roaming()+" "
				+partition.getForeign_data_items().size()+" "+partition.getPartition_percentage_foreign());	
	}
	
	public void logTransactionProp(Workload workload, PrintWriter prWriter) {
		prWriter.print(workload.getWrl_transactionTypes()+" ");
		
		int space = workload.getWrl_transactionProp().length;
		for(double prop : workload.getWrl_transactionProp()) {
			prWriter.print(Integer.toString((int)Math.round(prop)));
			--space;
			
			if(space != 0)
				prWriter.print(" ");
		}
		
		prWriter.println();	
	}
}