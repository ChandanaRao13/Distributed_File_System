package gash.router.server.workChainHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;
import routing.Pipe.FileTask;

/**
 * 
 * @author vaishampayan
 *
 */
public class TaskHandler implements IWorkChainHandler{
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
		if(workMessage.hasFiletask()) {
			logger.info("Recieved replicate work message");
		//	QueueManager.getInstance().enqueueInboundWork(msg, channel);
		}
		else {
			this.nextChainHandler.handle(workMessage, channel);
		}
	}

}