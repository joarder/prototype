package jkamal.prototype.base;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

public class HEdgeSet {
	private Set<HEdge> hEdgeSet;
	
	public HEdgeSet() {
		this.hEdgeSet = new TreeSet<HEdge>();
	}
	
	public Set<HEdge> gethEdgeSet() {
		return hEdgeSet;
	}

	public void sethEdgeSet(Set<HEdge> hEdgeSet) {
		this.hEdgeSet = hEdgeSet;
	}
	
	public void addHEdge(HEdge hEdge) {
		this.hEdgeSet.add(hEdge);
	}
	
	public void delHEdge(HEdge hEdge) {
		this.hEdgeSet.remove(hEdge);
	}
	
	public Iterator<HEdge> getIterator() {
		return this.hEdgeSet.iterator();
	}
	
	public boolean hasContainHEdge(int id) {
		Iterator<HEdge> iterator = this.getIterator();
		while(iterator.hasNext()) {
			if(iterator.next().getEdgeId() == id)
				return true;
		}
		
		return false;
	}
	
	public HEdge findHEdge(int id) {		
		for(HEdge hEdge : this.hEdgeSet) {
			if(hEdge.getEdgeId() == id)
				return hEdge;
		}
		
		return null;				
	}
}