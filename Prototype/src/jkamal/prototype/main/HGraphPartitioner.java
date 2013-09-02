/**
 * @author Joarder Kamal
 */

package jkamal.prototype.main;

import java.io.File;
import java.io.IOException;

import jkamal.prototype.alg.HGraphFold;
import jkamal.prototype.alg.HGraphMinCut;
import jkamal.prototype.base.HGraph;
import jkamal.prototype.io.HGraphFileInput;
import jkamal.prototype.io.HGraphFileOutput;

public class HGraphPartitioner {

	public HGraphPartitioner() {}
	
	public void runHGraphPartitioner(File hgraph_data_input) throws IOException {			
		String hgraph_data_dir = "C:\\Users\\Joarder Kamal\\Projects\\Eclipse\\Prototype\\exec\\native\\hMetis\\1.5.3-win32";
		//String hgraph_data_file = "sword.hgr";
		String hgraph_data_file = "workload.hgr";
		//String hgraph_data_file = "s13207P.hgr";
		//File hgraph_data_input = new File(hgraph_data_dir+"\\"+hgraph_data_file);
		//===============================================================================Initial
		
		// Take Input
		HGraph hGraph;
		HGraphFileInput hgraph_input = new HGraphFileInput();
		hGraph = hgraph_input.input(hgraph_data_input);
		//===============================================================================Process Input
		
		// Print HGraph Contents
		HGraphFileOutput hgraph_output = new HGraphFileOutput();
		hgraph_output.printHGraph(hGraph);
		hgraph_output.printHPartitionTable(hGraph);
		hgraph_output.printHEdgeSet(hGraph.getGlobalEdgeSet());
		hgraph_output.printHVertexSet(hGraph.getGlobalVertexSet());
		//===============================================================================Show Output
		
		//HGraph compressedHGraph;
		//int N = (hGraph.getGlobalVertexSet().gethVertexSet().size())/2;	// Compress the graph into half (CR = 0.5)
		//HGraphFold hGraphFold = new HGraphFold(hGraph, N);
		//compressedHGraph = hGraphFold.compress();
		//===============================================================================Graph Compression
		
		// Define # of desired number of partitions
		int num_partitions = 2;
		if(num_partitions > hGraph.getGlobalVertexSet().gethVertexSet().size()) {
			System.out.println("Number of desired partitions can not be accesseding total number of vertices in the Hypergraph !!!");			
		} else {
			System.out.println(">> Repartitioning the Hypergraph into "+num_partitions+" parts .......");
		
			// Run HMetis Hypergraph Partitioning Decision Algorithm
			// Parameters: exec directory, exec name, hypergraph input file, number of desired partitions
			File hgraph_exec_input = new File(hgraph_data_dir);
			//HGraphMinCut minCut = new HGraphMinCut(hgraph_exec_input, "khmetis", hgraph_data_file, num_partitions);
			//HGraphMinCut minCut = new HGraphMinCut(compressedHGraph, hgraph_exec_input, "khmetis", hgraph_data_file, num_partitions);
			
			//minCut.runHMetis();
		
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
			// Run Repartitioner based on the partitioning decision obtained in previous step
			HGraphRePartitioner hGraphPart = new HGraphRePartitioner(hGraph, num_partitions, hgraph_data_dir, hgraph_data_file);			
			hGraphPart.repartition();

		
			// Print HGraph contents after repartitioning
			hgraph_output.printHGraph(hGraph);
			hgraph_output.printHVertexSet(hGraph.getGlobalVertexSet());
			hgraph_output.printHPartitionTable(hGraph);			
		}
		//===============================================================================
	}
}