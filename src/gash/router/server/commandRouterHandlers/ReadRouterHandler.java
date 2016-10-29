package gash.router.server.commandRouterHandlers;

import io.netty.channel.Channel;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pipe.work.Work.WorkMessage;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.message.generator.MessageGenerator;
import gash.router.server.queue.management.InternalChannelNode;
import gash.router.server.queue.management.LoadQueueManager;
import gash.router.server.queue.management.NodeLoad;
import gash.router.server.queue.management.QueueManager;
import routing.Pipe.CommandMessage;
import routing.Pipe.FileTask.FileTaskType;
import gash.router.database.DatabaseHandler;



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
			int chunkCount = DatabaseHandler.getFilesChunkCount(request.getCommandMessage().getFiletask().getFilename());
			String clientId = EdgeMonitor.clientInfoMap(request);
		
			//LoadQueueManager.getInstance().printInfo();
			NodeLoad node = LoadQueueManager.getInstance().getMininumNodeLoadInfo(chunkCount);
			WorkMessage message = MessageGenerator.getInstance().generateReadRequestMessage(request.getCommandMessage(),
		clientId, node.getNodeId());
			Channel nodeChannel = EdgeMonitor.node2ChannelMap.get(node.getNodeId());
			QueueManager.getInstance().enqueueOutboundRead(message, nodeChannel); 
		} else {
			nextInChain.handleFileTask(request);
		}
		
	}
}