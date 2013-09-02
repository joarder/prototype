/**
 * @author Joarder Kamal
 */

package jkamal.prototype.io;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.TreeSet;

import jkamal.prototype.base.HEdge;
import jkamal.prototype.base.HGraph;
import jkamal.prototype.base.HVertex;

public class HGraphFileInput {	
	public HGraph input(File filename) throws IOException {		
		HGraph hGraph = new HGraph();
		HEdge hEdge;
		HVertex hVertex;
		ArrayList<HVertex> hVertices;
		TreeSet<HVertex> pVertices = new TreeSet<HVertex>();
		
		int edge = 0;
		int vertex = 0;
		int vertex_id = 0;
		String edgeWeight = null;
		String vertexWeight = null;
		String edgeVertices = null;
		
		Scanner scanner = new Scanner(filename);		
		try {
			String[] first_line = scanner.nextLine().split("\\s+", 3); // reading the first line of the input file

			int EdgeCount = Integer.parseInt(first_line[0]);	
			int VertexCount = Integer.parseInt(first_line[1]);  
			String hasWeight = first_line[2];
			
			System.out.println("# Edge Count = "+EdgeCount); //@debug
			System.out.println("# Vertex Count = "+VertexCount); //@debug			
						
			String hasEdgeWeight = hasWeight.substring(0, 1);
			if(Integer.parseInt(hasEdgeWeight) == 1)
				hGraph.setHasHEdgeWeight(true); // default is false
		
			String hasVertexWeight = hasWeight.substring(1, 2);
			if(Integer.parseInt(hasVertexWeight) == 1)
				hGraph.setHasHVertexWeight(true); // default is false			

			while (scanner.hasNextLine()) {
				if(EdgeCount != 0) { // check whether any more edges are left to read from input file
					Scanner edge_scanner = new Scanner(scanner.nextLine());
					try {
						while(edge_scanner.hasNext()) {
							++edge;
							
							if(hGraph.isHasHEdgeWeight()) {
							String[] followings = edge_scanner.nextLine().split("\\s+", 2);
							
							edgeWeight = followings[0];							
							edgeVertices = followings[1];
							} else {
								edgeWeight = "0";
								edgeVertices = edge_scanner.nextLine();
							}
							
							// Parse the hyper edge							
							hEdge = new HEdge();
							hEdge.setEdgeId(edge); // Edge id
							hEdge.setEdgeLabel(Integer.toString(edge)); // Edge label
							hEdge.setEdgeWeight(Float.parseFloat(edgeWeight)); // Edge weight
							hGraph.getGlobalEdgeSet().addHEdge(hEdge); // Add this Edge to the Global Edge List
							//System.out.print(hEdge.getEdgeLabel()+">> "); //@debug
							
							// Parse the Vertices inside a hyper edge
							// Form the Vertices
							hVertices = new ArrayList<HVertex>();
							int marker = 0;
							Scanner vertex_scanner = new Scanner(edgeVertices);	
							try {
								while(vertex_scanner.hasNext()) {
									vertex = vertex_scanner.nextInt();
									
									if(marker == 0)
										hEdge.setStartVertex(vertex); // Edge start vertex
									else {
										hEdge.setEndVertex(vertex); // Edge end vertex
										++marker;
									}
									
									hVertex = new HVertex();
									hVertex.setVertexId(vertex); // Vertex id
									hVertex.setVertexLabel(Integer.toString(vertex)); // Vertex label									
									hVertices.add(hVertex);
									pVertices.add(hVertex);
									hGraph.addToHPartitionTable(0, pVertices);  // Add this Vertex (unique) to the Partition Table
									hGraph.getGlobalVertexSet().addHVertex(hVertex); // Add this Vertex to the Global Vertex List									
									
									//System.out.print(hVertex.getVertexLabel()+" "); //@debug						
								} // end while() -- vertex_scanner
							} finally {					
								vertex_scanner.close();
								marker = 0;
							}
							
							//System.out.println(); //@debug							
							hGraph.addToHGraphElement(hEdge, hVertices);	// Set a HyperGraph Node with Edge and Vertex List
							--EdgeCount; // decrement line counts for edges
						} // end while() -- edge_scanner
					} finally {
						edge_scanner.close();						
					}
				} // end if() -- finish reading edges
				else { // else loop for reading vertex weights
					if(VertexCount != 0 && (hGraph.isHasHVertexWeight())) {						
						Scanner vertex_weight_scanner = new Scanner(scanner.nextLine());
						try {
							while(vertex_weight_scanner.hasNext()) {
								++vertex_id;
								vertexWeight = vertex_weight_scanner.next();								
								hGraph.getGlobalVertexSet().findHVertex(vertex_id).setVertexWeight(Float.parseFloat(vertexWeight));								
							} // end while() -- vertex_weight_scanner						
						} finally {
							vertex_weight_scanner.close();
						}
						
						--VertexCount;
					} // end if() -- finish reading vertex weights
				}
			} // end while() -- scanner			
		} finally {			
			scanner.close();
		}

		return hGraph;
	}
}