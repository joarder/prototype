/**
 * @author Joarder Kamal
 * 
 * Idea implementation
 */

package jkamal.prototype.workload;

import java.util.Map;
import java.util.TreeMap;
import jkamal.prototype.util.Matrix;
import jkamal.prototype.util.MatrixElement;

public class IdeaTable {
	private int movements;
	private Map<Integer, Integer> oldMap;
	private Map<Integer, Integer> keyMap;
	
	public IdeaTable() {
		this.setMovements(movements);
		this.setOldMap(new TreeMap<Integer, Integer>());
		this.setKeyMap(new TreeMap<Integer, Integer>());
	}
	
	public int getMovements() {
		return movements;
	}

	public void setMovements(int movements) {
		this.movements = movements;
	}
	
	public Map<Integer, Integer> getOldMap() {
		return oldMap;
	}

	public void setOldMap(Map<Integer, Integer> oldMap) {
		this.oldMap = oldMap;
	}

	public Map<Integer, Integer> getKeyMap() {
		return keyMap;
	}

	public void setKeyMap(Map<Integer, Integer> keyMap) {
		this.keyMap = keyMap;
	}

	public Matrix runIdea(Matrix M) {
		// Step-1 :: Max Movement Matrix Formation
		MatrixElement max;
		int diagonal_pos = 1;
		int moves = 0;		
		
		for(int m = 1; m < M.getM(); m++) {
			max = M.findMax(diagonal_pos);			
			
			// Row/Col swap with diagonal Row/Col
			if(max.getCounts() != 0) {
				M.swap_row(max.getRow_pos(), diagonal_pos);
				M.swap_col(max.getCol_pos(), diagonal_pos);				
			}			
			
			if(max.getCounts() != 0)
				moves += max.getCounts();
			
			++diagonal_pos;
		}
		
		this.setMovements(moves);

		// @debug
		System.out.println("\n>> @debug :: Movement Matrix before PID change >>");
		M.print();
		
		// Step-2 :: PID Conversion		
		// Create the PID conversion Key Map
		for(int i = 1; i < M.getM(); i++)
			this.getKeyMap().put((int)M.getMatrix()[i][0].getCounts(), (int)M.getMatrix()[0][i].getCounts());				
		
		// Assignment of proposed PID into the Matrix
		for(int col = 1, key = 0; col < M.getM(); col++, key++)			
			M.getMatrix()[0][col].setCounts(this.getKeyMap().get(key));				
		
		System.out.print("\n>> Total Data Moments Required (IdeaTable): "+moves+"\n");
		
		return M;
	}
}