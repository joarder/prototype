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
import jkamal.prototype.io.PrintPartitioningDetails;
import jkamal.prototype.io.PrintWorkloadDetails;
import jkamal.prototype.util.Matrix;
import jkamal.prototype.workload.DataPostPartitionTable;
import jkamal.prototype.workload.DataPrePartitionTable;
import jkamal.prototype.workload.IdeaTable;
import jkamal.prototype.workload.MovementTable;
import jkamal.prototype.workload.Workload;
import jkamal.prototype.workload.WorkloadGeneration;

public class Prototype {	
	private static int DB_SERVERS = 4;
	private static int DATA_OBJECTS = 10000; // 10GB Data (in Size)
	private static String DIR_LOCATION = "C:\\Users\\Joarder Kamal\\git\\Prototype\\Prototype\\exec\\native\\hMetis\\1.5.3-win32";	
	private static String HMETIS = "hmetis";
	private static String KHMETIS = "khmetis";
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
		System.out.print("\n>> Workload generation complete !!!\n");		
		
		//==============================================================================================
		// Perform workload analysis and use hypergraph partitioning tool (hMetis) to reduce the cost of 
		// distributed transactions as well as maintain the load balance among the data partitions				
		System.out.print("\n>> Run HyperGraph Partitioning on the Workload ... \n");
		// Sleep for 5sec to ensure the files are generated		
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		//==============================================================================================
		// Create Data Pre-Partition Table
		DataPrePartitionTable prePartitionTable = new DataPrePartitionTable();
		prePartitionTable.generatePrePartitionTable(db, workload, DIR_LOCATION);
		db.getDb_partition_table().setPrePartitionTable(prePartitionTable);
		
		//==============================================================================================
		// Run hMetis HyperGraph Partitioning				
		File dir = new File(DIR_LOCATION);
		HGraphMinCut minCut = new HGraphMinCut(db, workload, dir, HMETIS);
		//HGraphMinCut minCut = new HGraphMinCut(db, workload, dir, KHMETIS); 		
		minCut.runHMetis();

		// Sleep for 5sec to ensure the files are generated
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		//==============================================================================================
		// Create Data Post-Partition Table
		DataPostPartitionTable postPartitionTable = new DataPostPartitionTable();		
		postPartitionTable.generatePostPartitionTable(db, workload, DIR_LOCATION);		
		db.getDb_partition_table().setPostPartitionTable(postPartitionTable);
		
		//==============================================================================================
		// Print Data Partitioning Details
		PrintPartitioningDetails printPartitioningDetails = new PrintPartitioningDetails();
		printPartitioningDetails.printDetails(db); 
		
		//==============================================================================================
		// Generate Movement Table Matrix from Old and hMetis partitioning
		MovementTable movementTable = new MovementTable();		
		Matrix movementMatrix = movementTable.generateMovementTable(db, workload);
		System.out.println("\n>> Movement Matrix [First Row: Pre-Partition Id, First Col: Post-Partition Id, Elements: Data Occurance Counts] ...\n");
		movementMatrix.print();
		System.out.print("\n>> Total Data Movements Required (using hMetis Movement Matrix): "+Integer.toString(movementTable.getMovements())+"\n");
		
		//==============================================================================================
		// Perform Data Movement with Idea implementation
		DataMovement dataMovement = new DataMovement();
		// Create a Clone of the Database and Workload using Copy Constructor
		Database cloneDb = new Database(db);
		Workload cloneWorkload = new Workload(workload);
		
//==========================================================================================================		
		//System.out.print("\n>> Status check after cloning");
		//==============================================================================================
		// Print Data Partitioning Details
		//PrintPartitioningDetails clonePrintPartitioningDetails = new PrintPartitioningDetails();
		//clonePrintPartitioningDetails.printDetails(cloneDb);
		//System.out.println();
		//cloneWorkload.print(cloneDb);
		//System.out.println();
//==========================================================================================================
		
		dataMovement.move(cloneDb, cloneWorkload);
		
		//==============================================================================================
		// Printing out details after performing Data Movement using hMetis		
		System.out.println();
		cloneWorkload.print(cloneDb);
		System.out.println();
		
		//==============================================================================================
		// Run our idea !!!
		IdeaTable ideaTable = new IdeaTable();
		Matrix ideaMatrix = ideaTable.runIdea(movementMatrix);
		System.out.print("\n>> Idea Matrix [First Row: Pre-Partition Id, First Col: Post-Partition Id, Elements: Data Occurance Counts] ...\n");
		ideaMatrix.print();
		System.out.print("\n>> Total Data Movements Required (using Idea Matrix): "+Integer.toString(ideaTable.getMovements())+"\n");
		
		//==============================================================================================
		// Perform Data Movement with Idea implementation
		DataMovement idea_DataMovement = new DataMovement();
		idea_DataMovement.move(db, workload, ideaTable);
		
		//==============================================================================================
		// Printing out details after performing Data Movement using Idea		
		System.out.println();
		workload.print(db);
		System.out.println();
	}
}