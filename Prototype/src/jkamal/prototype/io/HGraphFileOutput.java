package jkamal.prototype.io;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import jkamal.prototype.base.HEdge;
import jkamal.prototype.base.HEdgeSet;
import jkamal.prototype.base.HGraph;
import jkamal.prototype.base.HVertex;
import jkamal.prototype.base.HVertexSet;

public class HGraphFileOutput {
	
	public void printHEdgeSet(HEdgeSet hEdgeSet) {
		System.out.println("# Global Edge Set #");
		System.out.println("===================");
		System.out.print("Eg");
		System.out.print(" >> ");
		System.out.print("[");
		
		Iterator<HEdge> iterator =  hEdgeSet.getIterator();
		while(iterator.hasNext()) {
			System.out.print(iterator.next());
			if(iterator.hasNext())
				System.out.print(", ");
		}
		
		System.out.println("]");
		System.out.println();
	}
	
	public void printHVertexSet(HVertexSet hVertexSet) {
		System.out.println("# Global Vertex Set #");
		System.out.println("=====================");
		System.out.print("Vg");
		System.out.print(" >> ");
		System.out.print("[");
		
		Iterator<HVertex> iterator =  hVertexSet.getIterator();
		while(iterator.hasNext()) {
			System.out.print(iterator.next());
			if(iterator.hasNext())
				System.out.print(", ");
		}
		
		System.out.println("]");
		System.out.println();
	}
	
	public void printHPartitionTable(HGraph hGraph) {
		Set<Integer> partitions = hGraph.getAllPartitions();
		int partition;
		int vertices = 0;
		HVertex vertex;
		TreeSet<HVertex> partition_vertices;
		
		System.out.println();
		System.out.println("# Partition Table #");
		System.out.println("===================");
		
		Iterator<Integer> partition_iterator = partitions.iterator();
		while(partition_iterator.hasNext()) {
			partition = partition_iterator.next();
			
			System.out.print("P"+partition);			
			System.out.print(" >> ");
			System.out.print("[");
			
			partition_vertices = hGraph.getPartitionVertices(partition);
			Iterator<HVertex> vertex_iterator = partition_vertices.iterator();
			while(vertex_iterator.hasNext()) {
				++vertices;
				vertex = vertex_iterator.next();
				System.out.print(vertex.toString());
				
				if(vertex_iterator.hasNext())
					System.out.print(", ");
			}
			
			System.out.println("]");
			System.out.println("Number of vertices in P"+partition+": "+vertices);
			vertices = 0;
		}
		
		System.out.println();		
	}
	
	/**
	 * Given a HGraph<HEdge, List<HVertex>> this function will print all the HEdge[HVertices] labels included in the HyperGraph
	 */
	public void printHGraph(HGraph hGraph) {
		Set<HEdge> edges = hGraph.getAllHEdges();
		HEdge edge;
		HVertex vertex;
		ArrayList<HVertex> edge_vertices;
		
		System.out.println();
		System.out.println("# Hypergraph #");
		System.out.println("==============");
		
		Iterator<HEdge> edge_iterator = edges.iterator();
		while(edge_iterator.hasNext()) {
			edge = edge_iterator.next();
			System.out.print(edge.toString()); //@debug
			
			System.out.print(" >> ");
			System.out.print("[");
			
			edge_vertices = hGraph.getHVertices(edge);
			Iterator<HVertex> vertex_iterator = edge_vertices.iterator();
			while(vertex_iterator.hasNext()) {				
				vertex = vertex_iterator.next();
				System.out.print(vertex.toString());
				
				if(vertex_iterator.hasNext())
					System.out.print(", ");
			}			
			
			System.out.println("]");
		}
		
		System.out.println();
	}	
}