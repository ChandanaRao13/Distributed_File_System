package gash.router.server.workChainHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import pipe.common.Common.Failure;
import pipe.work.Work.WorkMessage;

/**
 * 
 * @author vaishampayan
 *
 */
public class FailureHandler implements IWorkChainHandler{
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
		if(workMessage.hasErr()) {
			Failure err = workMessage.getErr();
			logger.error("failure from " + workMessage.getHeader().getNodeId());
		}
		else {
			nextChainHandler.handle(workMessage, channel);
		}
	}
}