/**
 * @author Joarder Kamal
 */

package jkamal.prototype.base;

public class HVertex implements Comparable<HVertex> {
	private int vertexId;
	private String vertexLabel;
	private float vertexWeight;
	private int partitionId;
	
	public HVertex() {
		this.vertexId = 0;
		this.vertexLabel = "undefined_vertex";
		this.vertexWeight = (float) 0.0;
		this.setPartitionId(0);
	}	

	public int getVertexId() {
		return vertexId;
	}

	public void setVertexId(int vertexId) {
		this.vertexId = vertexId;
	}

	public String getVertexLabel() {
		return this.vertexLabel;
	}

	public void setVertexLabel(String vertexLabel) {
		this.vertexLabel = "v"+vertexLabel;
	}
	
	public float getVertexWeight() {
		return this.vertexWeight;
	}
	
	public void setVertexWeight(float vertexWeight) {
		this.vertexWeight = vertexWeight;
	}
	
	public int getPartitionId() {
		return partitionId;
	}

	public void setPartitionId(int partitionId) {
		this.partitionId = partitionId;
	}

	@Override
	public String toString() {
		return (this.vertexLabel+"(W="+this.vertexWeight+", P"+this.partitionId+")");
	}
	
	@Override
	public int compareTo(HVertex hVertex) {		
		int compare = ((int)this.vertexId < (int)hVertex.vertexId) ? -1: ((int)this.vertexId > (int)hVertex.vertexId) ? 1:0;
		return compare;
	}
}