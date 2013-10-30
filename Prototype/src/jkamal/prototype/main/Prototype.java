/**
 * @author Joarder Kamal
 * 
 * Main Class to run the Database Simulator
 */

package jkamal.prototype.main;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;
import jkamal.prototype.alg.HGraphMinCut;
import jkamal.prototype.bootstrap.Bootstrapping;
import jkamal.prototype.db.DataMovement;
import jkamal.prototype.db.Database;
import jkamal.prototype.db.DatabaseServer;
import jkamal.prototype.io.OutputLogger;
import jkamal.prototype.workload.HGraphClusters;
import jkamal.prototype.workload.Workload;
import jkamal.prototype.workload.WorkloadGeneration;
import jkamal.prototype.workload.WorkloadReplay;

public class Prototype {	
	public final static int DB_SERVERS = 3;
	public final static int DATA_OBJECTS = 10000; // 10GB Data (in Size)
	public final static String DIR_LOCATION = "C:\\Users\\Joarder Kamal\\git\\Prototype\\Prototype\\exec\\native\\hMetis\\1.5.3-win32";	
	public final static String HMETIS = "khmetis";
	public final static int TRANSACTION_NUMS = 100;
	public final static int SIMULATION_RUN_NUMBERS = 2;
	
	public static void main(String[] args) throws IOException {			
		OutputLogger logger = new OutputLogger();
		PrintWriter dbWriter = logger.getWriter(DIR_LOCATION, "db");
		PrintWriter workloadWriter = logger.getWriter(DIR_LOCATION, "workload");
		PrintWriter partitionWriter = logger.getWriter(DIR_LOCATION, "partition");
		
		// Database Server and Tenant Database Creation
		DatabaseServer dbs = new DatabaseServer(0, "testdbs", DB_SERVERS);
		System.out.println("[ACT] Creating Database Server #"+dbs.getDbs_name()+"# with "+dbs.getDbs_nodes().size()+" Nodes ...");
		
		// Database creation for tenant id-"0" with Range partitioning model
		Database db = new Database(0, "testdb", 0, "Range");
		System.out.println("[ACT] Creating Database #"+db.getDb_name()+"# within "+dbs.getDbs_name()+" Database Server ...");		
		
		// Perform Bootstrapping through synthetic Data generation and placing it into appropriate Partition
		// following partitioning schemes like Range, Salting, Hashing and Consistent Hashing partitioning
		System.out.println("[ACT] Started Bootstrapping Process ...");
		System.out.println("[ACT] Generating "+ DATA_OBJECTS +" synthetic data items ...");
		//System.out.println();
		Bootstrapping bootstrapping = new Bootstrapping();
		bootstrapping.bootstrapping(dbs, db, DATA_OBJECTS);
		System.out.println("[MSG] Data creation and placement into partitions done.");
		
		// Printing out details after data loading
		dbs.print();
		db.print();						
		
		// Logging
		logger.log(dbs, db, dbWriter);
		
		//==============================================================================================
		// Initial
		WorkloadGeneration workloadGen = new WorkloadGeneration();
		Workload workload = null;
		WorkloadReplay workloadReplay = new WorkloadReplay();
		File dir = new File(DIR_LOCATION);
		HGraphMinCut minCut;
		HGraphClusters hGraphClusters = new HGraphClusters();
		DataMovement dataMovement = new DataMovement();		
		
		double transaction_birth_rate = 0.0;
		double bRangeMin = 0.45;
		double bRangeMax = 0.55;
		int transaction_borning = 0;
		
		double transaction_death_rate = 0.0;		
		double dRangeMin = 0.45;
		double dRangeMax = 0.55;
		int transaction_dying = 0;		
		int simulation_round = 0;
		Random bRand, dRand;
		
		System.out.println("***********************************************************************************************************************");
		//============================================================================================== 		
		// Prototype Run Rounds
		while(simulation_round != SIMULATION_RUN_NUMBERS) {								
			System.out.println("[RUN] Running Simulation Round-"+simulation_round+" ...");
			
			if(simulation_round != 0) {			
				workload.setWrl_round(simulation_round);				
				System.out.println("[MSG] Total "+workload.getWrl_totalTransaction()+" previous transactions have been carried forward to the current Workload");

				// Generating Random Death Rate within a range of (0.45 ~ 0.55)
				dRand = new Random();
				transaction_death_rate = Math.round((dRangeMin + (dRangeMax - dRangeMin) * dRand.nextDouble()) * 100.0) / 100.0;				
				transaction_dying = (int) ((int) workload.getWrl_totalTransaction() * transaction_death_rate);				
				workload.setWrl_transactionDeathRate(transaction_death_rate);
				workload.setWrl_transactionDying(transaction_dying);
				
				// Generating Random Birth Rate within a range of (0.45 ~ 0.55)
				bRand = new Random();
				transaction_birth_rate = Math.round((bRangeMin + (bRangeMax - bRangeMin) * bRand.nextDouble()) * 100.0) / 100.0;				
				transaction_borning = (int) ((int) workload.getWrl_totalTransaction() * transaction_birth_rate);
				workload.setWrl_transactionBirthRate(transaction_birth_rate);
				workload.setWrl_transactionBorning(transaction_borning);
			} else {	
				// Initial
				workload = workloadGen.init(db, "TPC-C", 0);
				workload.setWrl_initTotalTransactions(TRANSACTION_NUMS);
				
				// Printing Output Messages
				System.out.print("[ACT] Simulation Round-"+workload.getWrl_round()+" :: " +
						"Generating an initial workload with "+ TRANSACTION_NUMS
						+" transactions of "+workload.getWrl_transactionTypes()+" types having a proportion of ");				
			}  // end -- if-else()
			
			// Generate Synthetic Workload
			workload = workloadGen.generateWorkload(dbs, db, workload, DIR_LOCATION);
			
			workload.setMessage(" (Initial) ");
			workload.print(db);
			System.out.println();
			System.out.println("[MSG] Simulation Round-"+simulation_round+" :: Workload generation completed !!!");	
			
			//==============================================================================================
			// Perform workload analysis and use HyperGraph partitioning tool (hMetis) to reduce the cost of 
			// distributed transactions as well as maintain the load balance among the data partitions				
			System.out.println("[ACT] Run HyperGraph Partitioning on the Workload ...");
			
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
			workloadReplay.captureWorkload(simulation_round, db, workload);
							
			// Logging Before Data Movement				
			logger.logWorkload(db, workload, workloadWriter);
			logger.logPartition(db, workload, partitionWriter);
			
			//==============================================================================================
			// Perform Data Movement following One(Cluster)-to-One(Partition) and Many(Cluster)-to-One(Partition)
			System.out.println("[ACT] Base Strategy[Simulation Round-"+simulation_round+"] :: One(Cluster)-to-One(Partition) Peer");
			db.setDb_dmv_strategy("bs");
			dataMovement.baseStrategy(db, workload);

			// Logging After Data Movement
			logger.setData_movement(true);
			logger.logWorkload(db, workload, workloadWriter);
			logger.logPartition(db, workload, partitionWriter);
			logger.setData_movement(false);
			
			// Increase Simulation Round by 1
			++simulation_round;			
			System.out.println("***********************************************************************************************************************");
		}
		
		// Evaluating Strategy-1
		System.out.println("[ACT] Replaying Workload Capture using Strategy-1 ...");
		System.out.println("***********************************************************************************************************************");
		int strategy1_run_round = 0;
		while(strategy1_run_round != SIMULATION_RUN_NUMBERS) {					
			Database db1 = new Database(workloadReplay.getDb_replayMap().get(strategy1_run_round));
			Workload workload1 = new Workload(workloadReplay.getWrl_ReplayMap().get(strategy1_run_round));
			
			// Logging Before Data Movement
			logger.logWorkload(db1, workload1, workloadWriter);
			logger.logPartition(db1, workload1, partitionWriter);
			
			System.out.println("[ACT] Strategy-1[Capture-"+workload.getWrl_capture()+" | Round-"+workload.getWrl_round()
					+"] :: One(Cluster)-to-One(Partition) [Column Max]");
			db1.setDb_dmv_strategy("s1");
			dataMovement.strategy1(db1, workload1);			
			
			// Logging After Data Movement
			logger.setData_movement(true);
			logger.logWorkload(db1, workload1, workloadWriter);
			logger.logPartition(db1, workload1, partitionWriter);
			logger.setData_movement(false);
			
			++strategy1_run_round;	
			System.out.println("***********************************************************************************************************************");
		}
		
		// Evaluating Strategy-2
		System.out.println("[ACT] Replaying Workload Capture using Strategy-2 ...");
		System.out.println("***********************************************************************************************************************");
		int strategy2_run_round = 0;
		while(strategy2_run_round != SIMULATION_RUN_NUMBERS) {			
			Database db2 = new Database(workloadReplay.getDb_replayMap().get(strategy2_run_round));
			Workload workload2 = new Workload(workloadReplay.getWrl_ReplayMap().get(strategy2_run_round));
			
			// Logging Before Data Movement
			logger.logWorkload(db2, workload2, workloadWriter);
			logger.logPartition(db2, workload2, partitionWriter);
			
			System.out.println("[ACT] Strategy-2[Capture-"+workload.getWrl_capture()+" | Round-"+workload.getWrl_round()
					+"] :: One(Cluster)-to-One(Unique Partition) [Sub Matrix Max]");
			db2.setDb_dmv_strategy("s2");
			dataMovement.strategy2(db2, workload2);			
			
			// Logging After Data Movement
			logger.setData_movement(true);
			logger.logWorkload(db2, workload2, workloadWriter);
			logger.logPartition(db2, workload2, partitionWriter);
			logger.setData_movement(false);
			
			++strategy2_run_round;
			System.out.println("***********************************************************************************************************************");
		}
		
		// End Logging
		workloadWriter.flush();
		partitionWriter.flush();
		
		workloadWriter.close();
		partitionWriter.close();
	}
}