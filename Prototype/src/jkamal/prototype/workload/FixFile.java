/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;

import jkamal.prototype.db.Data;
import jkamal.prototype.transaction.TransactionDataSet;

public class FixFile {
	private String fix_file = null;
	
	public FixFile() {
		this.setFix_file("fixfile.txt");
	}
	
	// Copy Constructor
	public FixFile(FixFile fixFile) {
		this.fix_file = fixFile.getFix_file();
	}
		
	public String getFix_file() {
		return fix_file;
	}

	public void setFix_file(String fix_file) {
		this.fix_file = fix_file;
	}
	
	public void generateFixFile(Workload workload, String workload_file_dir) {
		TransactionDataSet transactionDataSet = workload.getWrl_transactionDataSet();
		File fixFile = new File(workload_file_dir+"\\"+this.getFix_file());
		Data data;
		
		try {
			fixFile.createNewFile();
			Writer writer = null;
			try {
				writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fixFile), "utf-8"));
				
				Iterator<Data> iterator = transactionDataSet.getTransactionDataSet().iterator();
				while(iterator.hasNext()) {
					data = iterator.next();
					
					if(data.isData_isMoveable())
						writer.write(Integer.toString(data.getData_partition_id()));
					else
						writer.write(Integer.toString(-1));
					
					if(iterator.hasNext())
						writer.write("\n");
				}
			} catch(IOException e) {
				e.printStackTrace();
			}finally {
				writer.close();
			}
		} catch (IOException e) {		
			e.printStackTrace();
		}
		
		//return fixFile;
	}
}