/**
 * @author Joarder Kamal
 */

package jkamal.prototype.util;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.RandomDataGenerator;

public class PseudoRandGenerationTest {
	public static void main(String[] args) {
		//int d_total = 10000;
		int p_numbers = 3; // Partition Numbers
	    int p_size = 5;
	    int z_exponent = 1;	   	    	    	    
	    int d = 0;
	    int d_start = 0;
	    int d_end = p_size;
	    double mu = 0.0;
	    double sigma = 1.0;
	    double z, c, c_rand, n;
			    	    
	    ZipfDistribution zipf;	// Parameters = Number of Elements, Exponent    		    	    	    	    	    	    
	    RandomDataGenerator randData = new RandomDataGenerator();
	    RandomDataGenerator rand = new RandomDataGenerator();
	    rand.reSeed(0);
	    
	    for(int p = 0; p < p_numbers; p++) {
	    	zipf = new ZipfDistribution(p_size, z_exponent); 
	    	zipf.reseedRandomGenerator(p);
	    	randData.reSeed(p);		    
	    	
	    	for(d = d_start; d < d_end; d++) {
	    		z = randData.nextZipf(p_size, z_exponent);
	    		z += d_start;
	    		
	    		if(z > d_end) z -= 1;
	    		
	    		c = zipf.cumulativeProbability((d - d_start));
	    		c_rand = zipf.cumulativeProbability(((int)z - d_start));
	    		
	    		//n = rand.nextGaussian(mu, sigma);
	    		n = rand.nextUniform(0.0, 1.0);
		        
		    	System.out.println(p+" "+d+" "+c+" "+(int)z+" "+c_rand+" "+n);
	    	}
	    	
	    	d_start = d_end;
	    	d_end = (d + p_size);	    	
	    }	    
	}
}