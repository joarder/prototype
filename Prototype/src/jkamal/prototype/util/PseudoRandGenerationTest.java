package jkamal.prototype.util;

import jkamal.prototype.main.DBMSSimulator;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.RandomData;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.commons.math3.random.RandomDataImpl;

public class PseudoRandGenerationTest {
	public static void main(String[] args) {   
		int x, value;
	    double p, z, w, w1, b;	    	    
	    
	    ZipfDistribution zipf = new ZipfDistribution(10, 1);

	    /*for(int i = 0; i < 10; i++) {
	    	x = randGen.rangeIntRandom();
	    	z = zipf.probability(x);
	    	
	    	System.out.println("x = "+x+"| z = "+z+", ");
	    }*/
	    	    
	    RandomDataGenerator randData = new RandomDataGenerator(); 
	    randData.reSeed(0);
	    
	    for (int i = 0; i < 168; i++) {
	        //z = randData.nextZipf(24, 0.5);
	        //w = randData.nextWeibull(1, 0.5);
	        w1 = randData.nextWeibull(1, 0.5);
	        //p = randData.nextPoisson(0.5);
	        //b = randData.nextBeta(2, 5);
	    	//value = (int) randData.nextUniform(0.0, 10000.0, false);
	        
	    	//System.out.print(value);
	        //System.out.print(" | "+Math.round((0.0 + (1.0 - 0.0) * w) * 100.0) / 100.0);
	        System.out.println(Math.round((0.0 + (1.0 - 0.0) * w1) * 100.0) / 100.0);
	        //System.out.println();
	    }	    	    
	    
	    
	}
}