/**
 * @author Joarder Kamal
 * Implementation of Implementation of Hypergraph compression technique mimicking Karger's Randomise Min-Cut Alg.
 */

package jkamal.prototype.alg;

import java.util.Random;

import jkamal.prototype.base.HGraph;

public class HGraphRandomizeFold {
	public HGraphRandomizeFold() {
		
	}
	
	public void runHRandomizeMinCut(HGraph hGraph) {
		Random rand = new Random();
		
		//while(hGraph.getGlobalVertexSet())
			
			//hGraph.getAllHEdges().
	}
	
	/*
	 *     public static int minCut( Graph gr ) {
        
        Random rnd = new Random();
        
        while( gr.vertices.size() > 2 ) {
            Edge edge = gr.edges.remove( rnd.nextInt( gr.edges.size() ) );
            Vertex v1 = cleanVertex( gr, edge.ends.get( 0 ), edge );
            Vertex v2 = cleanVertex( gr, edge.ends.get( 1 ), edge );
            //contract
            Vertex mergedVertex = new Vertex( v1.lbl );
            redirectEdges( gr, v1, mergedVertex );
            redirectEdges( gr, v2, mergedVertex );
            
            gr.addVertex( mergedVertex );
        }
        return gr.edges.size();
    }
    
    private static Vertex cleanVertex( Graph gr, Vertex v, Edge e ) {
        gr.vertices.remove( v.lbl );
        v.edges.remove( e );
        return v;
    }
    
    private static void redirectEdges( Graph gr, Vertex fromV, Vertex toV ) {
        for ( Iterator<Edge> it = fromV.edges.iterator(); it.hasNext(); ) {
            Edge edge = it.next();
            it.remove();
            if( edge.getOppositeVertex( fromV ) == toV ) {
                //remove self-loop
                toV.edges.remove( edge );
                gr.edges.remove( edge );
            } else {
                edge.replaceVertex( fromV, toV );
                toV.addEdge( edge );
            }
        }
    }
	 */
}
