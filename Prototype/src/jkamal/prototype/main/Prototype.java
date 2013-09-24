/**
 * @author Joarder Kamal
 * 
 * Main Class to run the Database Simulator
 */

package jkamal.prototype.main;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import jkamal.prototype.alg.HGraphMinCut;
import jkamal.prototype.bootstrap.Bootstrapping;
import jkamal.prototype.db.DataMovement;
import jkamal.prototype.db.Database;
import jkamal.prototype.db.DatabaseServer;
import jkamal.prototype.io.PrintDatabaseDetails;
import jkamal.prototype.workload.HGraphClusters;
import jkamal.prototype.workload.Workload;
import jkamal.prototype.workload.WorkloadGeneration;

public class Prototype {	
	public final static int DB_SERVERS = 4;
	public final static int DATA_OBJECTS = 5000; // 10GB Data (in Size)
	public final static String DIR_LOCATION = "C:\\Users\\Joarder Kamal\\git\\Prototype\\Prototype\\exec\\native\\hMetis\\1.5.3-win32";	
	public final static String HMETIS = "hmetis";
	public static int TRANSACTION_NUMS = 1000;
	
	public static void main(String[] args) throws IOException {
		
		// Database Server and Tenant Database Creation
		DatabaseServer dbs = new DatabaseServer(0, "testdbs", DB_SERVERS);
		System.out.println(">> Creating Database Server #"+dbs.getDbs_name()+"# with "+dbs.getDbs_nodes().size()+" Nodes ...");
		
		// Database creation for tenant id-"0" with Range partitioning model
		Database db = new Database(0, "testdb", 0, "Range");
		System.out.println(">> Creating Database #"+db.getDb_name()+"# within "+dbs.getDbs_name()+" Database Server ...");		
		
		// Perform Bootstrapping through synthetic Data generation and placing it into appropriate Partition
		// following partitioning schemes like Range, Salting, Hashing and Consistent Hashing partitioning
		System.out.println(">> Started Bootstrapping Process ...");
		System.out.println(">> Generating "+ DATA_OBJECTS +" synthetic data items ...");
		System.out.println();
		Bootstrapping bootstrapping = new Bootstrapping();
		bootstrapping.bootstrapping(dbs, db, DATA_OBJECTS);
		
		// Printing out details after data loading
		PrintDatabaseDetails dbDetails = new PrintDatabaseDetails();
		dbDetails.printDetails(dbs, db);
		
		System.out.println("\n>> Data loading complete !!!");

		//==============================================================================================
		// Initial
		WorkloadGeneration workloadGen = new WorkloadGeneration();
		Workload workload = null;
		File dir = new File(DIR_LOCATION);
		HGraphMinCut minCut;
		HGraphClusters hGraphClusters = new HGraphClusters();
		DataMovement dataMovement = new DataMovement();
		
		double time_variant = 0.0;
		double workload_variant = 0.0;
		boolean workload_mode = true; // positive
		int workload_round = 0;
		Random rand;
		
		//============================================================================================== 		
		// Prototype Run Rounds
		while(workload_round != 24) {		
			// Generate Workload variations for successive rounds except the first run
			if(workload_round !=0) {
				rand = new Random();
				time_variant = rand.nextDouble();
				workload_variant = Math.round(time_variant * 100.0)/100.0;
				workload_mode = rand.nextBoolean();
														
				if(workload_mode)
					TRANSACTION_NUMS += (int)(TRANSACTION_NUMS * workload_variant);
				else 
					TRANSACTION_NUMS -= (int)(TRANSACTION_NUMS * workload_variant);
				
				System.out.println("@debug >> Workload Variant: "+workload_variant+"| Workload_mode: "+workload_mode+"| Transactions: "+TRANSACTION_NUMS);
			} else {
				workload = workloadGen.init(db, "AuctionMark", 0);
			}
						
			System.out.println(">> Round-"+workload_round+" :: Generating a transactional workload with "+ TRANSACTION_NUMS +" synthetic transactions ...\n");						
			workload = workloadGen.generateRepeatedWorkload(dbs, db, TRANSACTION_NUMS, DIR_LOCATION, workload, workload_round, workload_mode);
			workload.setMessage(" (Initial) ");
			workload.print(db);
			System.out.println();
			System.out.println(">> Round-"+workload_round+" :: Workload generation completed !!!");		
			
			//==============================================================================================
			// Perform workload analysis and use HyperGraph partitioning tool (hMetis) to reduce the cost of 
			// distributed transactions as well as maintain the load balance among the data partitions				
			System.out.println(">> Run HyperGraph Partitioning on the Workload ...");
			// Sleep for 5sec to ensure the files are generated		
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			//==============================================================================================
			// Run hMetis HyperGraph Partitioning							
			minCut = new HGraphMinCut(db, workload, dir, HMETIS); 		
			minCut.runHMetis();

			// Sleep for 5sec to ensure the files are generated
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}		
			
			//==============================================================================================
			// Read Part file and assign corresponding Data cluster Id			
			hGraphClusters.readPartFile(db, workload);		
			
			//==============================================================================================
			// Perform Data Movement following One(Cluster)-to-One(Partition) and Many(Cluster)-to-One(Partition)
			System.out.println(">> Strategy-1 :: One(Cluster)-to-One(Partition)");
			dataMovement.strategy1(db, workload);
			System.out.println();
			System.out.println(">> Strategy-2 :: One(Cluster)-to-One(Unique Partition)");
			dataMovement.strategy2(db, workload);										

			++workload_round;
		}		
	}
}