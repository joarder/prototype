/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;

import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;

public class HGraphClusters {
	private String part_dir = "C:\\Users\\Joarder Kamal\\git\\Prototype\\Prototype\\exec\\native\\hMetis\\1.5.3-win32";
	
	public HGraphClusters() { }
	
	public void readPartFile(Database db, Workload workload) throws IOException {
		List<Data> trDataSet = workload.getWrl_dataList();
		Data data;
		
		String wrl_fileName = workload.getWrl_workload_file();		
		String part_file = wrl_fileName+".part."+db.getDb_partitions().size();						
		File part = new File(this.part_dir+"\\"+part_file);
		int data_id = -1;
		int cluster_id = -1;
		
		Scanner scanner = new Scanner(part);
		try {
			while(scanner.hasNextLine()) {
				cluster_id = Integer.valueOf(scanner.nextLine());
				++data_id;
				data = trDataSet.get(data_id);
				data.setData_hmetis_cluster_id(cluster_id);
				data.setData_shadow_hmetis_id(-1);
				data.setData_hasShadowHMetisId(false);								
			}
		} finally {
			scanner.close();
		}		
	}
}