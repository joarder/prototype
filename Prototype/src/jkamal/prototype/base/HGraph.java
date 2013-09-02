/**
 * @author Joarder Kamal
 * 
 * @description
 * HGraph defines a weighted Hypergraph representation where both hyperedges and vertices can have individual weight
 */

package jkamal.prototype.base;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class HGraph {
	private int totalHEdges;
	private int totalHVertices;
	private boolean hasHEdgeWeight;
	private boolean hasHVertexWeight;
	private HEdgeSet GlobalEdgeSet;
	private HVertexSet GlobalVertexSet;
	private Map<Integer, TreeSet<HVertex>> hPartitionTable;
	private Map<HEdge, ArrayList<HVertex>> hGraphElement;

	public HGraph() {
		this.setTotalHEdges(0);
		this.setTotalHVertices(0);
		this.setHasHEdgeWeight(false);
		this.setHasHVertexWeight(false);
		this.GlobalEdgeSet = new HEdgeSet();
		this.GlobalVertexSet = new HVertexSet();
		this.hPartitionTable = new TreeMap<Integer, TreeSet<HVertex>>();
		this.hGraphElement = new TreeMap<HEdge, ArrayList<HVertex>>();
	}

	public int getTotalHEdges() {
		return totalHEdges;
	}

	public void setTotalHEdges(int totalHEdges) {
		this.totalHEdges = totalHEdges;
	}
	
	public int getTotalHVertices() {
		return totalHVertices;
	}

	public void setTotalHVertices(int totalHVertices) {
		this.totalHVertices = totalHVertices;
	}

	public boolean isHasHEdgeWeight() {
		return hasHEdgeWeight;
	}

	public void setHasHEdgeWeight(boolean hasHEdgeWeight) {
		this.hasHEdgeWeight = hasHEdgeWeight;
	}

	public boolean isHasHVertexWeight() {
		return hasHVertexWeight;
	}

	public void setHasHVertexWeight(boolean hasHVertexWeight) {
		this.hasHVertexWeight = hasHVertexWeight;
	}
	
	public HVertexSet getGlobalVertexSet() {
		return this.GlobalVertexSet;
	}

	public void setGlobalVertexSet(HVertexSet globalVertexSet) {
		this.GlobalVertexSet = globalVertexSet;
	}

	public HEdgeSet getGlobalEdgeSet() {
		return this.GlobalEdgeSet;
	}

	public void setGlobalEdgeSet(HEdgeSet globalEdgeSet) {
		this.GlobalEdgeSet = globalEdgeSet;
	}
	
	public Map<Integer, TreeSet<HVertex>> getPartitionTable() {
		return this.hPartitionTable;
	}

	public void addToHPartitionTable(int partitionId, TreeSet<HVertex> hVertices) {
		this.hPartitionTable.put(partitionId, hVertices);
	}	
	
	public void delFromHPartitionTable(int partitionId) {
		this.hPartitionTable.remove(partitionId);
	}
	
	public void delHVertexFromHPartitionTable(int partitionId, HVertex hVertex) {
		this.getPartitionVertices(partitionId).remove(hVertex);				
	}
	
	public HVertex findInHPartitionTable(int partitionId, int vertexId) {		
		for(HVertex hVertex : this.getPartitionVertices(partitionId)) {
			if(hVertex.getVertexId() == vertexId)
				return hVertex;
		}
		
		return null;				
	}	
	
	public Set<Entry<Integer, TreeSet<HVertex>>> getPartitionEntrySet() {
		return this.hPartitionTable.entrySet();
	}

	public Map<HEdge, ArrayList<HVertex>> getHGraphElement() {
		return this.hGraphElement;
	}
	
	public void addToHGraphElement(HEdge hEdge, ArrayList<HVertex> hVertices) {
		this.hGraphElement.put(hEdge, hVertices);
	}
	
	public Set<Entry<HEdge, ArrayList<HVertex>>> getEntrySet() {
		return this.hGraphElement.entrySet();
	}

	public Set<Integer> getAllPartitions() {
		return this.hPartitionTable.keySet();
	}
	
	public Set<HEdge> getAllHEdges() {
		return this.hGraphElement.keySet();
	}
	
	public TreeSet<HVertex> getPartitionVertices(int partitionId) {
		return this.hPartitionTable.get(partitionId);
	}
	
	public ArrayList<HVertex> getHVertices(HEdge hEdge) {
		return this.hGraphElement.get(hEdge);
	}
	
	public boolean hasHGraphContainEdge(HEdge edge) {
		return this.hGraphElement.containsKey(edge);
	}	

	public boolean hasHGraphContainVertex(HVertex vertex) {
		return this.hGraphElement.containsValue(vertex);
	}
	
	public boolean hasHGraphContainPartition(int pid) {
		return this.hPartitionTable.containsKey(pid);
	}
	
	public boolean isHGraphEmpty() {
		return this.hGraphElement.isEmpty();
	}		
}