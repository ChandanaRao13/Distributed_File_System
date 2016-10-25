package gash.router.server.commandRouterHandlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.queue.management.InternalChannelNode;
import routing.Pipe.FileTask.FileTaskType;



public class ReadRouterHandler implements ICommandRouterHandlers{
	private  ICommandRouterHandlers nextInChain;
	protected static Logger logger = LoggerFactory.getLogger(ReadRouterHandler.class);
	
	public void setNextChainHandler(ICommandRouterHandlers nextChain) {
		this.nextInChain = nextChain;
	}

	@Override
	public void handleFileTask(InternalChannelNode request) throws Exception {
		FileTaskType taskType = request.getCommandMessage().getFiletask().getFileTaskType();
		if(taskType == FileTaskType.READ){
			System.out.println("In Read");
		} else {
			nextInChain.handleFileTask(request);
		}
		
	}
}