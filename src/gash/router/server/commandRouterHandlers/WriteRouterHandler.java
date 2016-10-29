package gash.router.server.commandRouterHandlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pipe.common.Common.Header;
import gash.router.database.DatabaseHandler;
import gash.router.server.message.generator.MessageGenerator;
import gash.router.server.queue.management.InternalChannelNode;
import gash.router.server.queue.management.QueueManager;
import gash.router.server.replication.DataReplicationManager;
import routing.Pipe.CommandMessage;
import routing.Pipe.FileTask;
import routing.Pipe.FileTask.FileTaskType;

public class WriteRouterHandler implements ICommandRouterHandlers{
	protected static Logger logger = LoggerFactory.getLogger(WriteRouterHandler.class);
	private  ICommandRouterHandlers nextInChain;

	@Override
	public void setNextChainHandler(ICommandRouterHandlers nextChain) {	
		this.nextInChain = nextChain;
	}

	@SuppressWarnings({ "deprecation" })
	@Override
	public void handleFileTask(InternalChannelNode request) throws Exception {
		FileTask fileTask = request.getCommandMessage().getFiletask();
		FileTaskType taskType = fileTask.getFileTaskType();
		
		if(taskType == FileTaskType.WRITE){
			if(DatabaseHandler.addFile(fileTask.getFilename(), fileTask.getChunk(), fileTask.getChunkNo(), fileTask.getChunkCounts())){
			//if(DatabaseHandler.addFile(fileTask.getFilename(), fileTask.getChunkCounts(), fileTask.getChunk().toByteArray(), fileTask.getChunkNo())){
				CommandMessage commandMessage = MessageGenerator.getInstance().generateClientResponseMsg(true);
				QueueManager.getInstance().enqueueOutboundCommmand(commandMessage, request.getChannel());
				DataReplicationManager.getInstance().broadcastReplication(request.getCommandMessage());
			} else {
				MessageGenerator.getInstance().generateClientResponseMsg(false);
				logger.error("Database write error, couldnot save the file into the database");
			}
		} else {
			logger.error("Handles only client read and write requests ");
		}
	}
}