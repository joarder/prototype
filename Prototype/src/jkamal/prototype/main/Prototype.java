/**
 * @author Joarder Kamal
 * 
 * Main Class to run the Database Simulator
 */

package jkamal.prototype.main;

import java.io.File;
import java.io.IOException;
import jkamal.prototype.alg.HGraphMinCut;
import jkamal.prototype.bootstrap.Bootstrapping;
import jkamal.prototype.db.DataMovement;
import jkamal.prototype.db.Database;
import jkamal.prototype.db.DatabaseServer;
import jkamal.prototype.io.PrintDatabaseDetails;
import jkamal.prototype.util.Matrix;
import jkamal.prototype.workload.DataPostPartitionTable;
import jkamal.prototype.workload.DataPrePartitionTable;
import jkamal.prototype.workload.IdeaTable;
import jkamal.prototype.workload.MappingTable;
import jkamal.prototype.workload.Workload;
import jkamal.prototype.workload.WorkloadGeneration;

public class Prototype {	
	private static int DB_SERVERS = 4;
	private static int DATA_OBJECTS = 10000; // 10GB Data (in Size)
	private static String DIR_LOCATION = "C:\\Users\\Joarder Kamal\\git\\Prototype\\Prototype\\exec\\native\\hMetis\\1.5.3-win32";	
	private static String HMETIS = "hmetis";
	private static int TRANSACTION_NUMS = 5;
	
	public static void main(String[] args) throws IOException {
		
		// Database Server and Tenant Database Creation
		DatabaseServer dbs = new DatabaseServer(0, "testdbs", DB_SERVERS);
		System.out.println(">> Creating Database Server #"+dbs.getDbs_name()+"# with "+dbs.getDbs_node_numbers()+" Nodes ...");
		
		// Database creation for tenant id-"0" with Range partitioning model
		Database db = new Database(0, "testdb", 0, "Range");
		System.out.println(">> Creating Database #"+db.getDb_name()+"# within "+dbs.getDbs_name()+" Database Server ...");		
		
		// Perform Bootstrapping through synthetic Data generation and placing it into appropriate Partition
		// following partitioning schemes like Range, Salting, Hashing and Consistent Hashing partitioning		
		Bootstrapping bootstrapping = new Bootstrapping();
		bootstrapping.bootstrapping(dbs, db, DATA_OBJECTS);
		
		// Printing out details after data loading
		PrintDatabaseDetails dbDetails = new PrintDatabaseDetails();
		dbDetails.printDetails(dbs, db);
		
		System.out.print("\n>> Data loading complete !!!\n");

		//============================================================================================== 		
		// Synthetic Workload Generation 				
		System.out.print("\n>> Generating a database workload with "+ TRANSACTION_NUMS +" synthetic transactions ...\n");
		WorkloadGeneration workloadGen = new WorkloadGeneration();
		Workload workload = workloadGen.generateWorkload(db, "AuctionMark", TRANSACTION_NUMS, DIR_LOCATION);		
		workload.print(db);
		
		//==============================================================================================
		// Create Data Pre-Partition Table for the Workload
		DataPrePartitionTable prePartitionTable = db.getDb_partition_table().getDataPrePartitionTable();
		prePartitionTable.generatePrePartitionTable(db, workload, DIR_LOCATION);
		prePartitionTable.print();
		
		System.out.print("\n>> Workload generation complete !!!");		
		
		//==============================================================================================
		// Perform workload analysis and use hypergraph partitioning tool (hMetis) to reduce the cost of 
		// distributed transactions as well as maintain the load balance among the data partitions				
		System.out.print("\n>> Run HyperGraph Partitioning on the Workload ...");
		// Sleep for 5sec to ensure the files are generated		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		//==============================================================================================
		// Run hMetis HyperGraph Partitioning				
		File dir = new File(DIR_LOCATION);
		HGraphMinCut minCut = new HGraphMinCut(db, workload, dir, HMETIS); 		
		minCut.runHMetis();

		// Sleep for 5sec to ensure the files are generated
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		 
		
	//Start Baseline Approach==============================================================================================
		// Generate Movement Mapping Table Matrix from Original Partition IDs and hMetis Cluster IDs
		
		//System.out.print("\n>> Total Data Movements Required (using hMetis Movement Matrix): "+Integer.toString(mappingTable.getMovements())+"\n");
		
		//==============================================================================================
		// Perform Data Movement with Idea implementation
		DataMovement dataMovement = new DataMovement();
		dataMovement.OneToOne(db, workload);
		dataMovement.OneToMany(db, workload);		
						
		//dataMovement.move(cloneDb, cloneWorkload);
		
		//==============================================================================================
		// Printing out details after performing Data Movement using hMetis		
		//System.out.println("\n>> After data movement ...");
		//cloneWorkload.print(cloneDb);
		//System.out.println();

		//==============================================================================================
		// Create Data Post-Partition Table
		//DataPostPartitionTable postPartitionTable = db.getDb_partition_table().getDataPostPartitionTable();		
		//postPartitionTable.generatePostPartitionTable(db, workload, DIR_LOCATION);
		//postPartitionTable.print();
	//End Baseline Approach==============================================================================================
		
		//==============================================================================================
		// Run our idea !!!
		/*IdeaTable ideaTable = new IdeaTable();
		Matrix ideaMatrix = ideaTable.runIdea(mappingMatrix);
		System.out.print("\n>> Idea Matrix [First Row: Pre-Partition Id, First Col: Post-Partition Id, Elements: Data Occurance Counts] ...\n");
		ideaMatrix.print();
		System.out.print("\n>> Total Data Movements Required (using Idea Matrix): "+Integer.toString(ideaTable.getMovements())+"\n");
		
		//==============================================================================================
		// Perform Data Movement with Idea implementation
		DataMovement idea_DataMovement = new DataMovement();
		idea_DataMovement.move(db, workload, ideaTable);
		
		//==============================================================================================
		// Printing out details after performing Data Movement using Idea		
		System.out.println("\n>> After data movement ...");
		workload.print(db);
		System.out.println();*/
	}
}