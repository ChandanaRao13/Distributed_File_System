package gash.router.cluster;

import gash.router.container.GlobalRoutingConf;
import gash.router.cluster.GlobalEdgeMonitor;
import gash.router.server.election.RaftElectionContext;
import gash.router.server.tasks.TaskList;

/**
 * 
 * GlobalServerState for Global Thread details for GlobalEdgeMonitor,
 * GlobalRouterConf, ElectionContext for leader info
 *
 */
public class GlobalServerState {
	private GlobalRoutingConf conf;
	private GlobalEdgeMonitor emon;
	private RaftElectionContext electionCtx;
	private TaskList tasks;

	public GlobalRoutingConf getConf() {
		return conf;
	}

	public void setConf(GlobalRoutingConf conf) {
		this.conf = conf;
	}

	public GlobalEdgeMonitor getEmon() {
		return emon;
	}

	public void setEmon(GlobalEdgeMonitor emon) {
		this.emon = emon;
	}

	public TaskList getTasks() {
		return tasks;
	}

	public void setTasks(TaskList tasks) {
		this.tasks = tasks;
	}

	public RaftElectionContext getElectionCtx() {
		return electionCtx;
	}

	public void setElectionCtx(RaftElectionContext electionCtx) {
		this.electionCtx = electionCtx;
	}

}
