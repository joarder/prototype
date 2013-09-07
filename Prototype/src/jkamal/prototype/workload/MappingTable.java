/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.Iterator;
import java.util.List;

import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;
import jkamal.prototype.util.Matrix;
import jkamal.prototype.util.MatrixElement;

public class MappingTable {
	public MappingTable() {}
	
	public Matrix generateMappingTable(Database db, Workload workload) {
		int M = db.getDb_partitions().size()+1;
		int N = M; // Having a NxN matrix
		
		// Create a 2D Matrix to represent the Data movements due to partitioning decision
		MatrixElement[][] mapping = new MatrixElement[M][N];
		
		// Initialization
		for(int i = 0; i < M; i++) {		
			for(int j = 0; j < N; j++) {
				if(i == 0 && j == 0)
					mapping[i][j] = new MatrixElement(i, j, -1);
				else
					mapping[i][j] = new MatrixElement(i, j, 0);
			}
		}
		
		// Define row1 and col1 as the Partition IDs and HGraph Cluster IDs
		for(int i = 1; i < M; i++) {			
			mapping[i][0].setCounts(i-1);
			for(int j = 1; j < N; j++) {
				mapping[0][j].setCounts(j-1);
			}
		}
		
		int partition_id = -1;
		int cluster_id = -1;
		List<Data> trDataSet = workload.getWrl_dataList();
		Data data;
		MatrixElement e;
		
		Iterator<Data> iterator = trDataSet.iterator();
		while(iterator.hasNext()) {
			data = iterator.next();
			partition_id = data.getData_partition_id();
			cluster_id = data.getData_hmetis_cluster_id();			
			
			e = mapping[partition_id+1][cluster_id+1]; // Need to @debug
			e.setCounts(e.getCounts()+1);
		}		
		
		// Create the Movement Matrix
		return (new Matrix(mapping));		 
	}
}