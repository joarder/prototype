/**
 * @author Joarder Kamal
 */

package jkamal.prototype.util;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.RandomDataGenerator;

public class PseudoRandGenerationTest {
	public static void main(String[] args) {
		int d_total = 10000;
		int p_numbers = 2; // Partition Numbers
	    int p_size = 800;
	    int z_exponent = 1;	   	    	    	    
	    int d = 0;
	    int d_start = 0;
	    int d_end = p_size;
	    double z, c;
		
	    RandomDataGenerator randData = new RandomDataGenerator(); 
	    randData.reSeed(0);
	    
	    ZipfDistribution zipf = new ZipfDistribution(d_total, z_exponent); // Parameters = Number of Elements, Exponent
    	zipf.reseedRandomGenerator(0);	    	    	    	    	    	    
	    
	    for(int p = 0; p < p_numbers; p++) {	    		    	
	    	for(d = d_start; d < d_end; d++) {
	    		z = randData.nextZipf(p_size, z_exponent);
	    		z += d_start;
	    		
	    		c = zipf.cumulativeProbability(d);
		        
		    	System.out.println(p+" "+d+" "+(int)z+" "+c);
	    	}
	    	
	    	d_start = d_end;
	    	d_end = (d + p_size);	    	
	    }	    
	}
}