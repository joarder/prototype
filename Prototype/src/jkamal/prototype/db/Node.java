/**
 * @author Joarder Kamal
 * 
 * Node represents physical machine
 */

package jkamal.prototype.db;

import java.util.Set;
import java.util.TreeSet;

public class Node {		
	private int node_id;
	private String node_label;
	private int node_capacity;			
	private static int NODE_MAX_CAPACITY;
	private Set<Partition> node_partitions;
	
	public Node(int id, int label) {
		this.setNode_id(id);
		this.setNode_label("N"+label);
		this.setNode_capacity(0); // Initially no partitions present in a node. Current node capacity will be determined by the number of partitions it is holding.		
		Node.setNODE_MAX_CAPACITY(10); // 10GB -- // 1TB Data (in Size) Or, equivalently 1000 Partitions can be stored in a single node.
		this.setNode_partitions(new TreeSet<Partition>());
	}

	public int getNode_id() {
		return node_id;
	}

	public void setNode_id(int node_id) {
		this.node_id = node_id;
	}

	public String getNode_label() {
		return node_label;
	}

	public void setNode_label(String node_label) {
		this.node_label = node_label;
	}

	public int getNode_capacity() {
		return node_capacity;
	}

	public void setNode_capacity(int node_capacity) {
		this.node_capacity = node_capacity;
	}

	public static long getNODE_MAX_CAPACITY() {
		return NODE_MAX_CAPACITY;
	}

	public static void setNODE_MAX_CAPACITY(int max_capacity) {
		NODE_MAX_CAPACITY = max_capacity;
	}

	public Set<Partition> getNode_partitions() {
		return node_partitions;
	}

	public void setNode_partitions(Set<Partition> node_partitions) {
		this.node_partitions = node_partitions;
	}	
}