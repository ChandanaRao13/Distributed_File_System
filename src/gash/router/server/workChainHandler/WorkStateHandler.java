package gash.router.server.workChainHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;

/**
 * 
 * @author vaishampayan
 *
 */
public class WorkStateHandler implements IWorkChainHandler{
	private IWorkChainHandler nextChainHandler;
	protected ServerState state;
	protected static Logger logger = LoggerFactory.getLogger("work");

	@Override
	public void setNextChain(IWorkChainHandler nextChain,ServerState state) {
		//this.nextChainHandler = nextChain;
		System.out.println("No handlers");
		//this.state = state;
	}

	@Override
	public void handle(WorkMessage workMessage, Channel channel) {
		// TODO Auto-generated method stub
		if(workMessage.hasState()) {
			WorkState s = workMessage.getState();
		}
		else {
			// End of Chain - Handle it properly
		}
	}
	
}