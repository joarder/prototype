/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.commons.math3.distribution.ZipfDistribution;
import jkamal.prototype.db.Data;
import jkamal.prototype.db.Database;
import jkamal.prototype.db.Partition;
import jkamal.prototype.main.DBMSSimulator;

public class WorkloadDataPreparation {	
	private Map<Integer, Integer> zipf_rank_map;
	private Map<Integer, Double> zipf_cumulative_probability_map;
	Map<Integer, Double> zipf_norm_cumulative_probability_map;
	
	public WorkloadDataPreparation() {		
		zipf_rank_map = new HashMap<Integer, Integer>();
		zipf_cumulative_probability_map = new HashMap<Integer, Double>();
		zipf_norm_cumulative_probability_map = new HashMap<Integer, Double>();
	}
	
	// Calculate ranking for all the Data objects in a particular Partition following Zipf Distribution
	public void getZipfRanking(int seed, int number_of_elements, int d_start) {	
		double exponent = 1.0;
		double zipf_random_value = 0.0;
		ArrayList<Integer> zipf_rank_list = new ArrayList<Integer>();		
				
		DBMSSimulator.random_data.reSeed(seed);
		
		for(int i = 0; i < number_of_elements; i++) {
			zipf_random_value = DBMSSimulator.random_data.nextZipf(number_of_elements, exponent);
			zipf_random_value += (d_start-1);
						
			zipf_rank_list.add(((int)zipf_random_value));
		}								
		
		int occurrences = 0;
		for(int i = d_start; i <= (number_of_elements + d_start - 1); i++) {
			occurrences = Collections.frequency(zipf_rank_list, (i));	
			
			this.zipf_rank_map.put((i), occurrences);						
		}		
	}
	
	// Calculate cumulative probability P(X <= x) for all the Data objects in a particular Partition following Zipf Distribution
	public void getCumulativeProbability(int seed, int number_of_elements, int d_start) {
		double exponent = 1.0;		
		ZipfDistribution zipf_distribution = new ZipfDistribution(number_of_elements, exponent);
		zipf_distribution.reseedRandomGenerator(seed);		
		
		for(int i = d_start; i <= (number_of_elements + d_start - 1); i++)
			this.zipf_cumulative_probability_map.put((i), zipf_distribution.cumulativeProbability(i - d_start + 1));				
	}
	
	// Calculate cumulative normalised probability P(X <= x) for all the Data objects in a particular Partition following Zipf Distribution
	public void getNormalisedCumulativeProbability(int normalisation_divisor, int d_start) {		
		for(int i = d_start; i <= this.zipf_cumulative_probability_map.size(); i++)
			this.zipf_norm_cumulative_probability_map.put(i, (this.zipf_cumulative_probability_map.get(i)/normalisation_divisor));
	}
	
	public void prepareWorkloadData(Database db) {
		Iterator<Partition> iterator = db.getDb_partitions().iterator();
	    Partition partition = null;
	    int partition_size = 0;	  
	    int d_start = 1; //0
	    
	    // Iterating each partitions
	    while(iterator.hasNext()) {
	    	partition = iterator.next();
	    	partition_size = partition.getPartition_dataObjects().size();
	    		    	
	    	this.getZipfRanking(partition.getPartition_id(), partition_size, d_start);
	    	this.getCumulativeProbability(partition.getPartition_id(), partition_size, d_start);
	    	this.getNormalisedCumulativeProbability((db.getDb_partitions().size() - partition.getPartition_id() + 1), d_start);
	    		    	
	    	// Iterating each data objects
	    	for(Data data : partition.getPartition_dataObjects()) {	    		
	    		data.setData_ranking(this.zipf_rank_map.get(data.getData_id()));
	    		data.setData_cumulativeProbability(Math.round( this.zipf_cumulative_probability_map.get(data.getData_id()) * 100.0)/100.0);	    		
	    		data.setData_normalisedCumulativeProbability(Math.round( this.zipf_norm_cumulative_probability_map.get(data.getData_id()) * 100.0)/100.0);	    		
	    		
	    		/*System.out.println(data.getData_id()+" "
	    				+data.getData_ranking()+" "
	    				+data.getData_cumulativeProbability()+" "
	    				+data.getData_normalisedCumulativeProbability());*/
	    		
	    		d_start = (data.getData_id() + 1);
	    	}	    
	    }	    
	}
}