/**
 * @author Joarder Kamal
 * 
 * A Database Server can contain one or more tenant Databases
 */

package jkamal.prototype.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class DatabaseServer {
	private int dbs_id;
	private String dbs_name;
	private int dbs_node_numbers;
	private List<Node> dbs_nodes;
	private Set<Database> dbs_tenants;
	Node db_node;
	
	public DatabaseServer(int id, String name, int nodes) {
		this.setDbs_id(id);
		this.setDbs_name(name);
		this.setDbs_node_numbers(nodes);				
		this.setDbs_nodes(new ArrayList<Node>());		
		this.setDbs_tenants(new TreeSet<Database>());
		
		for(int i=0; i<nodes; i++) {
			db_node = new Node(i, i);
			this.getDbs_nodes().add(db_node);			
		}
	}

	public int getDbs_id() {
		return dbs_id;
	}

	public void setDbs_id(int dbs_id) {
		this.dbs_id = dbs_id;
	}

	public String getDbs_name() {
		return dbs_name;
	}

	public void setDbs_name(String dbs_name) {
		this.dbs_name = dbs_name;
	}

	public int getDbs_node_numbers() {
		return dbs_node_numbers;
	}

	public void setDbs_node_numbers(int dbs_node_numbers) {
		this.dbs_node_numbers = dbs_node_numbers;
	}

	public List<Node> getDbs_nodes() {
		return dbs_nodes;
	}

	public void setDbs_nodes(List<Node> dbs_nodes) {
		this.dbs_nodes = dbs_nodes;
	}

	public Set<Database> getDbs_tenants() {
		return dbs_tenants;
	}

	public void setDbs_tenants(Set<Database> dbs_tenants) {
		this.dbs_tenants = dbs_tenants;
	}
}