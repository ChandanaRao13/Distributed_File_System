package gash.router.server.commandRouterHandlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.database.DatabaseHandler;
import gash.router.server.queue.management.InternalChannelNode;
import routing.Pipe.FileTask;
import routing.Pipe.FileTask.FileTaskType;

public class WriteRouterHandler implements ICommandRouterHandlers{
	protected static Logger logger = LoggerFactory.getLogger(WriteRouterHandler.class);
	private  ICommandRouterHandlers nextInChain;

	@Override
	public void setNextChainHandler(ICommandRouterHandlers nextChain) {	
		this.nextInChain = nextChain;
	}

	@Override
	public void handleFileTask(InternalChannelNode request) throws Exception {
		// TODO Auto-generated method stub
		FileTask fileTask = request.getCommandMessage().getFiletask();
		FileTaskType taskType = fileTask.getFileTaskType();
		if(taskType == FileTaskType.WRITE){
		//	System.out.println("In WRITE");
			DatabaseHandler.addFile(fileTask.getFilename(), fileTask.getChunk(), fileTask.getChunkNo());
		} else {
			logger.error("Handles only client read and write requests ");
		}
		
	}	
}