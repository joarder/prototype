/**
 * @author Joarder Kamal
 * 
 * A Database Server can contain one or more tenant Databases
 */

package jkamal.prototype.db;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

public class DatabaseServer {
	private int dbs_id;
	private String dbs_name;
	private Set<Node> dbs_nodes;
	private Set<Database> dbs_tenants;
	
	public DatabaseServer(int id, String name, int nodes) {
		this.setDbs_id(id);
		this.setDbs_name(name);				
		this.setDbs_nodes(new TreeSet<Node>());		
		this.setDbs_tenants(new TreeSet<Database>());
		
		for(int i = 0; i < nodes; i++)			
			this.getDbs_nodes().add(new Node(i, i));
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

	public Set<Node> getDbs_nodes() {
		return dbs_nodes;
	}

	public void setDbs_nodes(Set<Node> dbs_nodes) {
		this.dbs_nodes = dbs_nodes;
	}
	
	public Node getDbs_node(int id) {
		Node node;
		Iterator<Node> iterator = this.getDbs_nodes().iterator();
		while(iterator.hasNext()) {
			node = iterator.next();
			if(node.getNode_id() == id)
				return node;
		}
		
		return null;
	}

	public Set<Database> getDbs_tenants() {
		return dbs_tenants;
	}

	public void setDbs_tenants(Set<Database> dbs_tenants) {
		this.dbs_tenants = dbs_tenants;
	}
}