/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.Iterator;
import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;
import jkamal.prototype.transaction.TransactionDataSet;
import jkamal.prototype.util.Matrix;

public class MovementTable {
	private int movements;
	
	public MovementTable() {
		this.setMovements(movements);
	}
	
	public int getMovements() {
		return movements;
	}

	public void setMovements(int movements) {
		this.movements = movements;
	}
	
	public Matrix generateMovementTable(Database db, Workload workload) {
		TransactionDataSet transactionDataSet = workload.getWrl_transactionDataSet();
		int M = db.getDb_partition_table().getPrePartitionTable().getPrePartitionTable().size();
		int N = db.getDb_partition_table().getPostPartitionTable().getPostPartitionTable().size();		
		
		// Create a 2D Matrix to represent the Data movements due to partitioning decision
		double[][] movement = new double[M+1][N+1];	
		// Define row1 and col1 as the Pre- and Post-Partition ids
		for(int i = 1; i < M+1; i++) {
			movement[i][0] = i-1;
			for(int j = 1; j < N+1; j++) {
				movement[0][j] = j-1;
			}
		}
		
		Data data;
		int moves = 0;
		
		Iterator<Data> iterator = transactionDataSet.getTransactionDataSet().iterator();
		while(iterator.hasNext()) {
			data = iterator.next();
			movement[data.getData_partition_id()+1][data.getData_hmetis_partition_id()+1] += 1;
			
			if(data.getData_partition_id() != data.getData_hmetis_partition_id())
				this.setMovements(++moves);
		}
		
		// Create the Movement Matrix
		Matrix MovementMatrix = new Matrix(movement);		
		return MovementMatrix;
	}
}