/**
 * @author Joarder Kamal
 */

package jkamal.prototype.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;
import jkamal.prototype.db.DatabaseServer;
import jkamal.prototype.db.Node;
import jkamal.prototype.db.Partition;
import jkamal.prototype.workload.Transaction;
import jkamal.prototype.workload.Workload;

public class SimulationMetricsLogger {	
	private String db_logger;
	private String workload_logger;
	private String partition_logger;
	private boolean data_movement;
	private Map<Integer, String> partitionsBeforeDM;
	
	public SimulationMetricsLogger() {
		this.setDb_logger("db-log.txt");
		this.setWorkload_logger("workload-log.txt");
		this.setPartition_logger("partition-log.txt");
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
			prWriter.println();
			
			space = dbs.getDbs_nodes().size();
			for(Node node : dbs.getDbs_nodes()) { 
				prWriter.print(Integer.toString(node.getNode_partitions().size()));
				
				if(space != 1)
					prWriter.print(" ");
				
				--space;
			}
			
			prWriter.println();			
			
			for(Partition partition : db.getDb_partitions()) {				
				space = db.getDb_partitions().size();
				
				prWriter.print(Integer.toString(partition.getPartition_dataSet().size())+" ");
				prWriter.print(Integer.toString(partition.getPartition_roaming_data())+" "); //getRoaming_dataObjects().size()
				prWriter.print(Integer.toString(partition.getPartition_foreign_data())); //getForeign_dataObjects().size()
				
				if(space != 1)
					prWriter.print(" ");
			
				--space;									
			}	
			
			prWriter.println();
		} finally {
			prWriter.flush();
			prWriter.close();
		} 
	}
	
	public void logDb(Database db, Workload workload, PrintWriter printWriter) {
		for(Partition partition : db.getDb_partitions()) {
			for(Data data : partition.getPartition_dataSet()) {
				printWriter.print(workload.getWrl_id()+" ");
				printWriter.print(data.getData_id()+" ");
				printWriter.print(data.getData_partitionId()+" ");
				
				int space = data.getData_transaction_involved().size();
				for(Integer transaction_id : data.getData_transaction_involved()) {
					Transaction transaction = workload.findWrl_transaction(transaction_id);
					
					printWriter.print(transaction.getTr_id()+" ");
					printWriter.print(transaction.getTr_ranking());
					
					--space;
					if(space != 0)
						printWriter.print(" ");
				}
				
				printWriter.print(data.getData_id()+" ");
			}
		}
	}
	
	public void logWorkload(Database db, Workload workload, PrintWriter prWriter) {		
		if(!this.isData_movement()) {
			prWriter.print(workload.getWrl_id()+" ");			
			prWriter.print("in ");
			
			prWriter.print(workload.getWrl_DtNumbers()+" ");
			prWriter.print(workload.getWrl_DtImpact()+" ");																		
		} else {					
			prWriter.print(db.getDb_dmv_strategy()+" ");
			
			prWriter.print(workload.getWrl_DtNumbers()+" ");
			prWriter.print(workload.getWrl_DtImpact()+" ");								
			
			prWriter.print(workload.getWrl_totalDataObjects()+" ");
			prWriter.print(workload.getWrl_intraNodeDataMovements()+" ");
			prWriter.print(workload.getWrl_percentageIntraNodeDataMovement()+" ");
			prWriter.print(workload.getWrl_interNodeDataMovements()+" ");
			prWriter.print(workload.getWrl_percentageInterNodeDataMovement());
			
			prWriter.println();			
		}				
	}
	
	public void logPartition(Database db, Workload workload, PrintWriter prWriter) {						
		/*if(!this.isData_movement()) {
			for(Entry<Integer, Set<Partition>> entry : db.getDb_partitionTable().getPartition_table().entrySet()) {
				for(Partition partition : entry.getValue()) {
					this.getPartitionsBeforeDM().put(partition.getPartition_id(), 
							entry.getKey()+" "+partition.getPartition_id()+" "+this.logPartitionDetails(partition)+" ");
				}						
			}						
		} else {			
			for(Entry<Integer, Set<Partition>> entry : db.getDb_partitionTable().getPartition_table().entrySet()) {							
				for(Partition partition : entry.getValue()) {
					prWriter.print(this.getPartitionsBeforeDM().get(partition.getPartition_id()));					
					prWriter.println(this.logPartitionDetails(partition));
				}						
			}						
		}*/
	}
	
	private String logPartitionDetails(Partition partition) {
		return (partition.getPartition_current_load()+" "
				//+partition.getPartition_dataSet().size()+" "+partition.getPartition_percentageMain()+" "
				//+partition.getRoaming_dataObjects().size()+" "+partition.getPartition_percentageRoaming()+" "
				//+partition.getForeign_dataObjects().size()+" "+partition.getPartition_percentageForeign());	
				+partition.getPartition_dataSet().size()//+" "+partition.getPartition_percentageMain()+" "
				+partition.getPartition_roaming_data()//+" "+partition.getPartition_percentageRoaming()+" "
				+partition.getPartition_foreign_data());//+" "+partition.getPartition_percentageForeign());	
	}
	
	public void logTransactionProp(Workload workload, PrintWriter prWriter) {
		prWriter.print(workload.getWrl_transactionTypes()+" ");
		
		int space = workload.getWrl_transactionProportions().length;
		for(double prop : workload.getWrl_transactionProportions()) {
			prWriter.print(Integer.toString((int)Math.round(prop)));
			--space;
			
			if(space != 0)
				prWriter.print(" ");
		}
		
		prWriter.println();	
	}
}