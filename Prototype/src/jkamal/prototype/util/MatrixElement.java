/**
 * @author Joarder Kamal
 */

package jkamal.prototype.util;

public class MatrixElement {
	private int row_pos;
	private int col_pos;
	private double value;
	
	public MatrixElement(int r, int c, double val) {
		this.setRow_pos(r);
		this.setCol_pos(c);
		this.setValue(val);
	}

	public int getRow_pos() {
		return row_pos;
	}

	public void setRow_pos(int row_pos) {
		this.row_pos = row_pos;
	}

	public int getCol_pos() {
		return col_pos;
	}

	public void setCol_pos(int col_pos) {
		this.col_pos = col_pos;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
	}    	    	
}