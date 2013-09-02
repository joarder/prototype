/**
 * @author Joarder Kamal
 * 
 * DataList will be used to create transactions and corresponding workloads
 */

package jkamal.prototype.db;

import java.util.HashMap;
import java.util.Map;


public class GlobalDataMap {
	private Map<Integer, Data> data_items;
	
	public GlobalDataMap() {
		setData_items(new HashMap<Integer, Data>());
	}

	public Map<Integer, Data> getData_items() {
		return data_items;
	}

	public void setData_items(Map<Integer, Data> data_items) {
		this.data_items = data_items;
	}
}