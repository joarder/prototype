/**
 * @author Joarder Kamal
 */

package jkamal.prototype.workload;

import java.util.Map;
import java.util.TreeMap;

import jkamal.prototype.db.Database;

public class WorkloadReplay {
	private Map<Integer, Database> db_replayMap;
	private Map<Integer, Workload> wrl_replayMap;
	
	public WorkloadReplay() {
		this.setDb_replayMap(new TreeMap<Integer, Database>());
		this.setWrl_ReplayMap(new TreeMap<Integer, Workload>());
	}

	public Map<Integer, Database> getDb_replayMap() {
		return db_replayMap;
	}

	public void setDb_replayMap(Map<Integer, Database> db_replayMap) {
		this.db_replayMap = db_replayMap;
	}

	public Map<Integer, Workload> getWrl_ReplayMap() {
		return wrl_replayMap;
	}

	public void setWrl_ReplayMap(Map<Integer, Workload> workloadReplay) {
		this.wrl_replayMap = workloadReplay;
	}
	
	public void captureWorkload(int capture_round, Database db, Workload workload) {
		// Create a Clone of the Database and Workload using Copy Constructor
		Database cloneDb = new Database(db);
		Workload cloneWorkload = new Workload(workload);

		this.getDb_replayMap().put(capture_round, cloneDb);
		this.getWrl_ReplayMap().put(capture_round, cloneWorkload);
	}
}