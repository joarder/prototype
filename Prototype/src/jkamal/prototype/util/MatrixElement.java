/**
 * @author Joarder Kamal
 */

package jkamal.prototype.util;

import java.util.ArrayList;

import jkamal.prototype.db.Data;

public class MatrixElement {
	private int row_pos;
	private int col_pos;
	private double counts;
	private ArrayList<Data> dataList;
	
	public MatrixElement(int r, int c, double val) {
		this.setRow_pos(r);
		this.setCol_pos(c);
		this.setCounts(val);
		this.setDataList(new ArrayList<Data>());
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

	public double getCounts() {
		return counts;
	}

	public void setCounts(double value) {
		this.counts = value;
	}

	public ArrayList<Data> getDataList() {
		return dataList;
	}

	public void setDataList(ArrayList<Data> dataList) {
		this.dataList = dataList;
	}
	
	public void addData(Data data) {
		this.getDataList().add(data);
	}
}