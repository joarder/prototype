/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.Map.Entry;

import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;

public class HGraphClusters {
	private String part_dir = "C:\\Users\\Joarder Kamal\\git\\Prototype\\Prototype\\exec\\native\\hMetis\\1.5.3-win32";
	
	public HGraphClusters() { }
	
	public void readPartFile(Database db, Workload workload) throws IOException {		
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>();		
		String wrl_fileName = workload.getWrl_workload_file();		
		String part_file = wrl_fileName+".part."+db.getDb_partitions().size();						
		File part = new File(this.part_dir+"\\"+part_file);
		int cluster_id = -1;
		int key = 0;
		
		Scanner scanner = new Scanner(part);
		try {
			while(scanner.hasNextLine()) {
				cluster_id = Integer.valueOf(scanner.nextLine());				
				++key;				
				keyMap.put(key, cluster_id);
				//System.out.println("@debug >> key:"+key+" | Cluster: "+cluster_id);
			}						
		} finally {
			scanner.close();
		}	
		
		//for(Entry<Integer, Set<Data>> entry : workload.getWrl_trDataMap().entrySet()) {
		for(Entry<Integer, ArrayList<Transaction>> entry : workload.getWrl_transactionMap().entrySet()) {
			for(Transaction transaction : entry.getValue()) {
			//for(Data data : entry.getValue()) {
				for(Data data : transaction.getTr_dataSet()) {
					if(data.isData_hasShadowHMetisId()) {					
						data.setData_hmetis_cluster_id(keyMap.get(data.getData_shadow_hmetis_id()));
						//System.out.println("@debug >> Data ("+data.toString()+") | hkey: "+data.getData_shadow_hmetis_id()+" | Cluster: "+data.getData_hmetis_cluster_id());
						data.setData_shadow_hmetis_id(-1);
						data.setData_hasShadowHMetisId(false);
					} else {
						//System.out.println("@debug >> *Repeated Data ("+data.toString()+") | hkey: "+data.getData_shadow_hmetis_id());
					}
				} // end -- for()-Data
			} // end -- for()-Transaction
		} // end -- for()-Transaction Types
	}
}