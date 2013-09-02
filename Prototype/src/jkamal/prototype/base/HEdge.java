/**
 * @author Joarder Kamal
 */

package jkamal.prototype.base;

public class HEdge implements Comparable<HEdge> {
	private int edgeId;
	private String edgeLabel;
	private float edgeWeight;
	private int startVertex;
	private int endVertex;
    
    public HEdge() {
    	this.edgeId = 0;
    	this.edgeLabel = "*undefined_edge";
    	this.edgeWeight = (float) 0.0;
    	this.setStartVertex(0);
    	this.setEndVertex(0);
    }

    public int getEdgeId() {
		return edgeId;
	}

	public void setEdgeId(int edgeId) {
		this.edgeId = edgeId;
	}

	public String getEdgeLabel() {
		return this.edgeLabel;
	}

	public void setEdgeLabel(String edgeLabel) {
		this.edgeLabel = "e"+edgeLabel;
	}
	
	public float getEdgeWeight() {
		return this.edgeWeight;
	}
	
	public void setEdgeWeight(Float edgeWeight) {
		this.edgeWeight = edgeWeight;
	}
	
	public int getStartVertex() {
		return startVertex;
	}

	public void setStartVertex(int startVertex) {
		this.startVertex = startVertex;
	}

	public int getEndVertex() {
		return endVertex;
	}

	public void setEndVertex(int endVertex) {
		this.endVertex = endVertex;
	}

	@Override
	public String toString() {
		return (this.edgeLabel+"("+this.edgeWeight+")");
	}

	@Override
	public int compareTo(HEdge hEdge) {		
		int compare = ((int)this.edgeId < (int)hEdge.edgeId) ? -1: ((int)this.edgeId > (int)hEdge.edgeId) ? 1:0;
		return compare;
	}
}