package gash.router.server.commandmessage.routerhandlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.database.RethinkDatabaseHandler;
import gash.router.server.message.generator.MessageGenerator;
import gash.router.server.queue.management.InternalChannelNode;
import gash.router.server.queue.management.QueueManager;
import gash.router.server.replication.DataReplicationManager;
import routing.Pipe.CommandMessage;
import routing.Pipe.FileTask;
import routing.Pipe.FileTask.FileTaskType;

public class WriteRouterHandler implements ICommandRouterHandlers {
	protected static Logger logger = LoggerFactory.getLogger(WriteRouterHandler.class);
	private ICommandRouterHandlers nextInChain;

	@Override
	public void setNextChainHandler(ICommandRouterHandlers nextChain) {
		this.nextInChain = nextChain;
	}

	@Override
	public void handleFileTask(InternalChannelNode request) throws Exception {
		FileTask fileTask = request.getCommandMessage().getFiletask();
		FileTaskType taskType = fileTask.getFileTaskType();

		if (taskType == FileTaskType.WRITE) {
			if (RethinkDatabaseHandler.addFile(fileTask.getFilename(), fileTask.getChunkCounts(),
					fileTask.getChunk().toByteArray(), fileTask.getChunkNo())) {
				CommandMessage commandMessage = MessageGenerator.getInstance()
						.generateClientResponseMsg("File is stored in the database");
				QueueManager.getInstance().enqueueOutboundCommmand(commandMessage, request.getChannel());
				DataReplicationManager.getInstance().broadcastReplication(request.getCommandMessage());
			} else {
				CommandMessage commandMessage = MessageGenerator.getInstance()
						.generateClientResponseMsg("File is not stored in the database, please retry");
				QueueManager.getInstance().enqueueOutboundCommmand(commandMessage, request.getChannel());
				logger.error("Error: Database write error, couldnot save the file into the database");
			}
		} else {
			nextInChain.handleFileTask(request);
		}
	}
}
