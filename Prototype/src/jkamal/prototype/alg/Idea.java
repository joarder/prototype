/**
 * @author Joarder Kamal
 * 
 * Idea implementation
 */

package jkamal.prototype.alg;

import jkamal.prototype.util.Matrix;
import jkamal.prototype.util.MatrixElement;

public class Idea {
	private int movements;
	
	public Idea() {
		this.setMovements(movements);
	}
	
	public int getMovements() {
		return movements;
	}

	public void setMovements(int movements) {
		this.movements = movements;
	}

	public Matrix runIdea(Matrix M) {
		MatrixElement e;
		int diagonal_pos = 1;
		int moves = 0;
		
		for(int m = 1; m < M.getM(); m++) {
			e = M.findMax(diagonal_pos);
			//System.out.println("-#-Max = "+e.getValue()+"("+e.getRow_pos()+", "+e.getCol_pos()+")");
			
			// Row/Col swap with diagonal Row/Col
			if(e.getValue() != 0) {
				M.swap_row(e.getRow_pos(), diagonal_pos);
				M.swap_col(e.getCol_pos(), diagonal_pos);				
			}
			
			moves += e.getValue();
			++diagonal_pos;
		}
		
		this.setMovements(moves);
		return M;
	}
}