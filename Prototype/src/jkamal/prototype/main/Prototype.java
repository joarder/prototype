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
import jkamal.prototype.workload.WorkloadReplay;

public class Prototype {	
	public final static int DB_SERVERS = 4;
	public final static int DATA_OBJECTS = 10000; // 10GB Data (in Size)
	public final static String DIR_LOCATION = "C:\\Users\\Joarder Kamal\\git\\Prototype\\Prototype\\exec\\native\\hMetis\\1.5.3-win32";	
	public final static String HMETIS = "khmetis";
	public final static int TRANSACTION_NUMS = 1000;		
	
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
		
		System.out.println("\n[MSG] Data loading complete !!!");

		//==============================================================================================
		// Initial
		WorkloadGeneration workloadGen = new WorkloadGeneration();
		Workload workload = null;
		WorkloadReplay workloadReplay = new WorkloadReplay();
		File dir = new File(DIR_LOCATION);
		HGraphMinCut minCut;
		HGraphClusters hGraphClusters = new HGraphClusters();
		DataMovement dataMovement = new DataMovement();
		
		double time_variant = 0.0;
		double workload_variant = 0.0;
		int totalTransaction = 0;
		boolean workload_mode = true; // positive
		int workload_round = 0;
		Random rand;
		double rangeMin = 0.1;
		double rangeMax = 0.9;
		
		//============================================================================================== 		
		// Prototype Run Rounds
		while(workload_round != 10) {		
			// Generate Workload variations for successive rounds except the first run
			if(workload_round != 0) {
				workload.setWrl_round(workload_round);
				
				rand = new Random();
				time_variant = rangeMin + (rangeMax - rangeMin) * rand.nextDouble();
				workload_variant = Math.round(time_variant * 100.0)/100.0;
				totalTransaction = workload.getWrl_totalTransaction();												
				
				workload_mode= rand.nextBoolean();
				workload.setWrl_mode(workload_mode);
					
				workload.setWrl_transactionVariant((int)(totalTransaction * workload_variant));
				System.out.println("@debug >> Workload variation: "+(workload_variant * 100)+"% | "+workload.isWrl_mode());
				if(workload_mode) {					
					totalTransaction += workload.getWrl_transactionVariant();
					System.out.print(">> Round-"+workload.getWrl_round()+" :: " +
							"Varying the workload by increasing "+ workload.getWrl_transactionVariant()
							+" transactions of "+workload.getWrl_transactionTypes()+" types having a proportion of ");
				} else { 										
					totalTransaction -= workload.getWrl_transactionVariant();
					System.out.print(">> Round-"+workload.getWrl_round()+" :: " +
							"Varying the workload by reducing "+ workload.getWrl_transactionVariant()
							+" transactions of "+workload.getWrl_transactionTypes()+" types having a proportion of ");
				}								
			} else {	
				workload = workloadGen.init(db, "TPC-C", 0);
				workload.setWrl_totalTransaction(TRANSACTION_NUMS);
				System.out.print(">> Round-"+workload.getWrl_round()+" :: " +
						"Generating an initial workload with "+ TRANSACTION_NUMS
						+" transactions of "+workload.getWrl_transactionTypes()+" types having a proportion of ");				
			}
												
			workload = workloadGen.generateWorkload(dbs, db, workload, DIR_LOCATION);
			
			if(workload.isWorkloadEmpty()) {
				// Workload is empty
				workload_mode = true; // positive
				workload_round = 0;	
				workload.setWorkloadEmpty(false);				
			} else {			
				workload.setMessage(" (Initial) ");
				workload.print(db);
				System.out.println();
				System.out.println("[MSG] Round-"+workload_round+" :: Workload generation completed !!!");		
				
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
				// Capture Database and Workload states for replaying
				workloadReplay.captureWorkload(db, workload);
				
				//==============================================================================================
				// Perform Data Movement following One(Cluster)-to-One(Partition) and Many(Cluster)-to-One(Partition)
				System.out.println(">> Applying Strategy-1 :: One(Cluster)-to-One(Partition)");
				dataMovement.strategy1(db, workload);
				//System.out.println();
				//System.out.println(">> Applying Strategy-2 :: One(Cluster)-to-One(Unique Partition)");
				//dataMovement.strategy2(db, workload);										
	
				// Increase Workload Round by 1
				++workload_round;
				workload.setWrl_round(workload_round);
			}
		}		
	}
}