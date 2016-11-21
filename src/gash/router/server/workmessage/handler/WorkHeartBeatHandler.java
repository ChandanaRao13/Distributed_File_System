package gash.router.server.workmessage.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;

/**
 * 
 * @author vaishampayan
 *
 */
public class WorkHeartBeatHandler implements IWorkChainHandler {
	private IWorkChainHandler nextChainHandler;
	protected ServerState state;
	protected static Logger logger = LoggerFactory.getLogger(WorkHeartBeatHandler.class);

	@Override
	public void setNextChain(IWorkChainHandler nextChain, ServerState state) {
		// TODO Auto-generated method stub
		this.nextChainHandler = nextChain;
		this.state = state;
	}

	@Override
	public void handle(WorkMessage workMessage, Channel channel) {
		// TODO Auto-generated method stub
		if (workMessage.hasBeat()) {
			// Heartbeat heartBeat = workMessage.getBeat();
			logger.debug("heartbeat from: " + workMessage.getHeader().getNodeId());
		}
		nextChainHandler.handle(workMessage, channel);
	}

}