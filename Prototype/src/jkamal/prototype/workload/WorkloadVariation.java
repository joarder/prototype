/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.random.RandomDataGenerator;

public class WorkloadVariation {
	private List<Double> transaction_birth_rate_list = new ArrayList<Double>();
	private List<Double> transaction_death_rate_list = new ArrayList<Double>();
    private double transaction_birth_rate = 0.0;
	private double bRangeMin = 0.0;
	private double bRangeMax = 1.0;	
	private double transaction_death_rate = 0.0;		
	private double dRangeMin = 0.0;
	private double dRangeMax = 1.0;	
	
	public WorkloadVariation() {
		
	}
	
	public List<Double> getTransaction_birth_rate_list() {
		return transaction_birth_rate_list;
	}

	public void setTransaction_birth_rate_list(
			List<Double> transaction_birth_rate_list) {
		this.transaction_birth_rate_list = transaction_birth_rate_list;
	}

	public List<Double> getTransaction_death_rate_list() {
		return transaction_death_rate_list;
	}

	public void setTransaction_death_rate_list(
			List<Double> transaction_death_rate_list) {
		this.transaction_death_rate_list = transaction_death_rate_list;
	}

	public double getTransaction_birth_rate() {
		return transaction_birth_rate;
	}

	public void setTransaction_birth_rate(double transaction_birth_rate) {
		this.transaction_birth_rate = transaction_birth_rate;
	}

	public double getbRangeMin() {
		return bRangeMin;
	}

	public void setbRangeMin(double bRangeMin) {
		this.bRangeMin = bRangeMin;
	}

	public double getbRangeMax() {
		return bRangeMax;
	}

	public void setbRangeMax(double bRangeMax) {
		this.bRangeMax = bRangeMax;
	}

	public double getTransaction_death_rate() {
		return transaction_death_rate;
	}

	public void setTransaction_death_rate(double transaction_death_rate) {
		this.transaction_death_rate = transaction_death_rate;
	}

	public double getdRangeMin() {
		return dRangeMin;
	}

	public void setdRangeMin(double dRangeMin) {
		this.dRangeMin = dRangeMin;
	}

	public double getdRangeMax() {
		return dRangeMax;
	}

	public void setdRangeMax(double dRangeMax) {
		this.dRangeMax = dRangeMax;
	}

	public double getTransactionBirthRate(int simulation_round) {
		return this.transaction_birth_rate_list.get(simulation_round-1);
	}
	
	public double getTransactionDeathRate(int simulation_round) {
		return this.transaction_death_rate_list.get(simulation_round-1);
	}
	
	public void generateVariation(RandomDataGenerator rand, int simulation_run_numbers) {		
		for(int i = 0; i < simulation_run_numbers; i++) {
			this.setTransaction_birth_rate(Math.round((this.getbRangeMin() + (this.getbRangeMax() - this.getbRangeMin()) * rand.nextWeibull(1, 0.5)) * 100.0) / 100.0);			
					
			if(this.getTransaction_birth_rate() >= 1) {
				if(this.getTransaction_birth_rate() >= 2) {
					if(this.getTransaction_birth_rate() >= 3) {
						if(this.getTransaction_birth_rate() >= 4) {
							this.setTransaction_birth_rate(this.getTransaction_birth_rate()/5);
						} else {
							this.setTransaction_birth_rate(this.getTransaction_birth_rate()/4);
						}
					} else {
						this.setTransaction_birth_rate(this.getTransaction_birth_rate()/3);
					}
				} else {
					this.setTransaction_birth_rate(this.getTransaction_birth_rate()/2);
				}
			} else {
				// Nothing to do
			}
			
			//this.setTransaction_death_rate(Math.round((this.getdRangeMin() + (this.getdRangeMax() - this.getdRangeMin()) * rand.nextWeibull(1, 1.0)) * 100.0) / 100.0);
			this.setTransaction_death_rate(1-this.getTransaction_birth_rate());
			
			this.getTransaction_birth_rate_list().add(this.getTransaction_birth_rate());
			this.getTransaction_death_rate_list().add(this.getTransaction_death_rate());
			
			/*if(this.getTransaction_death_rate() >= 1) {
				if(this.getTransaction_death_rate() >= 2) {
					if(this.getTransaction_death_rate() >= 3) {
						if(this.getTransaction_death_rate() >= 4) {
							this.setTransaction_death_rate(this.getTransaction_death_rate()/16);
						} else {
							this.setTransaction_death_rate(this.getTransaction_death_rate()/8);
						}
					} else {
						this.setTransaction_death_rate(this.getTransaction_death_rate()/4);
					}
				} else {
					this.setTransaction_death_rate(this.getTransaction_death_rate()/2);
				}
			} else {
				// Nothing to do
			}*/			
			
	    }			
	}
}