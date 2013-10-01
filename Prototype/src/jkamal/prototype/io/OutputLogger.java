/**
 * @author Joarder Kamal
 */

package jkamal.prototype.io;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Set;
import java.util.Map.Entry;
import jkamal.prototype.db.Database;
import jkamal.prototype.db.DatabaseServer;
import jkamal.prototype.db.Node;
import jkamal.prototype.db.Partition;
import jkamal.prototype.workload.Transaction;
import jkamal.prototype.workload.Workload;

public class OutputLogger {	
	private String logger_file;
	
	public OutputLogger() {
		this.setLogger_file("log.txt");
	}
	
	public String getLogger_file() {
		return logger_file;
	}

	public void setLogger_file(String logger_file) {
		this.logger_file = logger_file;
	}

	public PrintWriter getWriter(String dir) {
		File logFile = new File(dir+"\\"+this.getLogger_file());
		PrintWriter prWriter = null;
		
		try {
			if(!logFile.exists())
				logFile.createNewFile();
			
			try {
				prWriter = new PrintWriter(new BufferedWriter(new FileWriter(logFile)));				
				
			} catch(IOException e) {
				e.printStackTrace();
			}//finally {
				//writer.close();
			//}
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
			//prWriter.flush();
			//prWriter.close();
		} 
	}
	
	public void log(Database db, Workload workload, PrintWriter prWriter) {
		int space = 0;		
		
		try {
			prWriter.print(workload.getWrl_capture()+" ");
			prWriter.print(workload.getWrl_dt_nums()+" ");
			prWriter.print(workload.getWrl_totalData()+" ");
			prWriter.print(workload.getWrl_transactionTypes()+" ");
			
			space = workload.getWrl_transactionProp().length;				
			for(double prop : workload.getWrl_transactionProp()) {
				prWriter.print(prop);
				--space;
				
				if(space != 0)
					prWriter.print(" ");
			}
			prWriter.println();	
		} finally {
			//prWriter.flush();
			//prWriter.close();
		} 
		
		/*this.calculateDTPercentage();	
		this.calculateDTImapct();
		System.out.println();
		System.out.println("      # Distributed Transactions: "+this.getWrl_dt_nums()+"("+this.getWrl_percentage_dt()+"%) " +
				"| Total Transactions: "+this.getWrl_totalTransaction()
				+" | DT Impact: "+this.getWrl_dt_impact()
				);
		if(this.isWrl_hasDataMoved())
			System.out.println("      # Data Movements: "+this.getWrl_dataMoved()+"("+this.getWrl_percentage_dmv()+"%) " +
					"| Total Data: "+this.getWrl_totalData());
		
		//this.printPartitionTable(db);
		//this.printDataPartitionTable();
		//this.printDataNodeTable();
		//System.out.println();
		*/
		
	}
}