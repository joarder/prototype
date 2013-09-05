/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.Iterator;
import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;
import jkamal.prototype.transaction.TransactionDataSet;
import jkamal.prototype.util.Matrix;
import jkamal.prototype.util.MatrixElement;

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
		//int M = db.getDb_partition_table().getPrePartitionTable().getPrePartitionTable().size();
		int N = db.getDb_partition_table().getDataPostPartitionTable().getDataPostPartitionTable().size()+1;
		int M = N; // Having a NxN matrix
		
		// Create a 2D Matrix to represent the Data movements due to partitioning decision
		MatrixElement[][] movement = new MatrixElement[M][N];
		
		// Initialization
		for(int i = 0; i < M; i++) {		
			for(int j = 0; j < N; j++) {
				if(i == 0 && j == 0)
					movement[i][j] = new MatrixElement(i, j, -1);
				else
					movement[i][j] = new MatrixElement(i, j, 0);
			}
		}
		
		// Define row1 and col1 as the Pre- and Post-Partition ids
		for(int i = 1; i < M; i++) {			
			movement[i][0].setValue(i-1);
			for(int j = 1; j < N; j++) {
				movement[0][j].setValue(j-1);
			}
		}
		
		int original_id = -1;
		int hmetis_id = -1;
		Data data;
		int moves = 0;
		MatrixElement e;
		
		Iterator<Data> iterator = transactionDataSet.getTransactionDataSet().iterator();
		while(iterator.hasNext()) {
			data = iterator.next();
			original_id = data.getData_partition_id();
			hmetis_id = data.getData_hmetis_cluster_id();
			
			e= movement[original_id+1][hmetis_id+1]; // Need to @debug
			e.setValue(e.getValue()+1);
			
			if(original_id != hmetis_id)
				this.setMovements(++moves);
		}
		
		System.out.print("\n>> Total Data Moments Required (MovementTable): "+moves+"\n");
		
		// Create the Movement Matrix
		Matrix MovementMatrix = new Matrix(movement);		
		return MovementMatrix;
	}
}