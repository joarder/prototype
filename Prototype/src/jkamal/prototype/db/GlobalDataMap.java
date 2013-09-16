/**
 * @author Joarder Kamal
 * 
 * DataList will be used to create transactions and corresponding workloads
 */

package jkamal.prototype.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class GlobalDataMap {
	private Map<Integer, Data> data_items;
	
	public GlobalDataMap() {
		this.setData_items(new HashMap<Integer, Data>());
	}
	
	// Copy Constructor
	public GlobalDataMap(GlobalDataMap dataMap) {
		
		Map<Integer, Data> cloneDataItems = new HashMap<Integer, Data>();
		Data cloneData;		
		for(Entry<Integer, Data> entry : dataMap.getData_items().entrySet()) {
			cloneData = new Data(entry.getValue());
			cloneDataItems.put(entry.getKey(), cloneData);
		}
		this.setData_items(cloneDataItems);
	}

	public Map<Integer, Data> getData_items() {
		return data_items;
	}

	public void setData_items(Map<Integer, Data> data_items) {
		this.data_items = data_items;
	}
}