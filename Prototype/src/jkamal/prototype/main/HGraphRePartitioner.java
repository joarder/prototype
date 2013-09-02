package jkamal.prototype.main;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import jkamal.prototype.base.HEdge;
import jkamal.prototype.base.HGraph;
import jkamal.prototype.base.HVertex;

public class HGraphRePartitioner {
	
	int num_partitions = -1;
	String part_dir = null;
	String part_file = null;
	HGraph hGraph;
	Map<Integer, Integer> partLabel;
	
	public HGraphRePartitioner(HGraph hGraph, int nparts, String part_datadir, String part_filename) {		
		this.num_partitions = nparts;
		this.part_dir = part_datadir;
		this.part_file = part_filename;
		this.hGraph = hGraph;
	}
	
	public void readPartFile() throws IOException {		
		String hgraph_part_file = this.part_file+".part."+this.num_partitions;						
		File hgraph_data_input = new File(this.part_dir+"\\"+hgraph_part_file);
		int vertex = 0;
		
		this.partLabel = new TreeMap<Integer, Integer>();
		Scanner scanner = new Scanner(hgraph_data_input);
		try {
			while(scanner.hasNextLine()) {
				vertex++;
				partLabel.put(vertex, Integer.valueOf(scanner.nextLine()));
			}
		} finally {
			scanner.close();
		}
	}
	
	public void repartition() throws IOException {
		//read part.num_partitions file and set the vertices into (num_partitions) of HGrap
		readPartFile();
		
		// Applying repartitioning decisions into the hypergraph, global vertex set and partition table
		Set<HEdge> edges = hGraph.getAllHEdges();
		HEdge edge;
		HVertex vertex;
		ArrayList<HVertex> edge_vertices;
		int repartitionId = -1;
		int oldPartitionId = -1;
		TreeSet<HVertex> pVertices;
		
		// Iterating over the HGraph
		Iterator<HEdge> edge_iterator = edges.iterator();
		while(edge_iterator.hasNext()) {
			edge = edge_iterator.next();			
			edge_vertices = hGraph.getHVertices(edge);
			
			Iterator<HVertex> vertex_iterator = edge_vertices.iterator();
			while(vertex_iterator.hasNext()) {
				vertex = vertex_iterator.next();				
				oldPartitionId = vertex.getPartitionId();
				repartitionId = partLabel.get(vertex.getVertexId());				
				
				if(oldPartitionId != repartitionId) {
					if(!hGraph.hasHGraphContainPartition(repartitionId)) {
						pVertices = new TreeSet<HVertex>();
						vertex.setPartitionId(repartitionId);
						pVertices.add(vertex);																		
						hGraph.addToHPartitionTable(repartitionId, pVertices);
						hGraph.delHVertexFromHPartitionTable(oldPartitionId, vertex);							
					} else { // repartitionId already in the partition table just add v to its v-Set
						vertex.setPartitionId(repartitionId);
						
						hGraph.getPartitionVertices(repartitionId).add(vertex);						
						hGraph.delHVertexFromHPartitionTable(oldPartitionId, vertex);
					}										
				}		
			} // end -- while() - vertex
			
			repartitionId = -1;
			oldPartitionId = -1;			
		} // end -- while()	- edge	
	}
}