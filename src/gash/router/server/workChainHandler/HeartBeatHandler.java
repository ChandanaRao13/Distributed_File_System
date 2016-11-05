package gash.router.server.workChainHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.WorkMessage;

/**
 * 
 * @author vaishampayan
 *
 */
public class HeartBeatHandler implements IWorkChainHandler{
	private IWorkChainHandler nextChainHandler;
	protected static Logger logger = LoggerFactory.getLogger("work");
	@Override
	public void setNextChain(IWorkChainHandler nextChain) {
		// TODO Auto-generated method stub
		this.nextChainHandler = nextChain;
	}

	@Override
	public void handle(WorkMessage workMessage, Channel channel) {
		// TODO Auto-generated method stub
		if(workMessage.hasBeat()) {
			Heartbeat heartBeat = workMessage.getBeat();
			logger.debug("heartbeat from " + workMessage.getHeader().getNodeId());
		}
		nextChainHandler.handle(workMessage, channel);
	}

}