package jkamal.prototype.base;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

public class HVertexSet {
	private Set<HVertex> hVertexSet;
	
	public HVertexSet() {
		this.hVertexSet = new TreeSet<HVertex>();
	}

	public Set<HVertex> gethVertexSet() {
		return this.hVertexSet;
	}

	public void sethVertexSet(Set<HVertex> hVertexSet) {
		this.hVertexSet = hVertexSet;
	}
	
	public void addHVertex(HVertex hVertex) {
		this.hVertexSet.add(hVertex);
	}
	
	public void delHVertex(HVertex hVertex) {
		this.hVertexSet.remove(hVertex);
	}
	
	public Iterator<HVertex> getIterator() {
		return this.hVertexSet.iterator();
	}
	
	public boolean hasContainHVertex(int id) {
		Iterator<HVertex> iterator = this.getIterator();
		while(iterator.hasNext()) {
			if(iterator.next().getVertexId() == id)
				return true;
		}
		
		return false;
	}
	
	public HVertex findHVertex(int id) {		
		for(HVertex hVertex : this.hVertexSet) {
			if(hVertex.getVertexId() == id)
				return hVertex;
		}
		
		return null;				
	}
}
