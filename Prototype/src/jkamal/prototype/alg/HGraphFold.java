/**
 * @author Joarder Kamal
 * Implementation of Hypergraph compression technique proposed at "SWORD"
 */

package jkamal.prototype.alg;

import java.util.Iterator;

import jkamal.prototype.base.HGraph;
import jkamal.prototype.base.HVertex;
import jkamal.prototype.base.HVertexSet;

public class HGraphFold {
	private HGraph compressedHGraph;
	private int compression_rate = 0;
	
	public HGraphFold(HGraph hGraph, int N) {
		this.compressedHGraph = hGraph;
		this.compression_rate = N;
	}
	
	public HGraph compress() {
	
		HVertexSet hVertexSet = this.compressedHGraph.getGlobalVertexSet();
		
		Iterator<HVertex> iterator =  hVertexSet.getIterator();
		while(iterator.hasNext()) {
			//System.out.print(iterator.next());
			//if(iterator.hasNext())
				//System.out.print(", ");
			
			
		}
		
		return this.compressedHGraph;
	}
	
	public int hashFunction(int vertexId) {
		int hashCode = -1;
		
		return hashCode;		
	}
}