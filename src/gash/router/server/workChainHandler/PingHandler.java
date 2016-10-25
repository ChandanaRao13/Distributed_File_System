package gash.router.server.workChainHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;

/**
 * 
 * @author vaishampayan
 *
 */
public class PingHandler implements IWorkChainHandler{
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
		if(workMessage.hasPing()) {
			//handling the ping message
			logger.info("ping from " + workMessage.getHeader().getNodeId());
			boolean p = workMessage.getPing();
			WorkMessage.Builder rb = WorkMessage.newBuilder();
			rb.setPing(true);
			channel.write(rb.build());
		}
		else {
			this.nextChainHandler.handle(workMessage, channel);
		}
	}

}