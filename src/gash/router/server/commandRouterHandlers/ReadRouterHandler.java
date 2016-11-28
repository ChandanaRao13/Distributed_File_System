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
import gash.router.util.GlobalMessageBuilder;
import global.Global.GlobalMessage;
import routing.Pipe.CommandMessage;
import routing.Pipe.FileTask;
import routing.Pipe.FileTask.FileTaskType;
import gash.router.cluster.GlobalEdgeMonitor;
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
		FileTask fileTask = request.getCommandMessage().getFiletask();
		if (taskType == FileTaskType.READ) {
			boolean inRiak = DatabaseHandler.isFileAvailableInRiak(fileTask.getFilename());
			boolean inRethink = DatabaseHandler.isFileAvailableInRethink(fileTask.getFilename());
			String filename = fileTask.getFilename();
			String clientId = EdgeMonitor.clientInfoMap(request);
			if (inRiak || inRethink) {
				int chunkCount = DatabaseHandler
						.getFilesChunkCount(request.getCommandMessage().getFiletask().getFilename());
				request.setChunkCount(chunkCount);
				NodeLoad node = LoadQueueManager.getInstance().getMininumNodeLoadInfo(chunkCount);
				// convert the read request to work messages of total number of
				// chunks
				for (int index = 0; index < chunkCount; index++) {
					WorkMessage worKMessage = MessageGenerator.getInstance().generateReadRequestMessage(
							request.getCommandMessage(), clientId, node.getNodeId(), index);
					Channel nodeChannel = EdgeMonitor.node2ChannelMap.get(node.getNodeId());
					QueueManager.getInstance().enqueueOutboundRead(worKMessage, nodeChannel);
				}
			} else {
				if (EdgeMonitor.getLeaderId() == EdgeMonitor.getNodeId()) {
					// Convert to Global Command Message and send it
					GlobalMessage globalMessage = GlobalMessageBuilder
							.generateGlobalRequestMessage(fileTask.getFilename(), clientId);
					GlobalEdgeMonitor.broadcastToClusterFriends(globalMessage);
				} else {
					WorkMessage workMessage = GlobalMessageBuilder.generateGlobalInternalWorkMessage(request.getCommandMessage(), clientId);
					Channel ch = EdgeMonitor.node2ChannelMap.get(EdgeMonitor.getLeaderId());
					QueueManager.getInstance().enqueueOutboundRead(workMessage, ch);
				}
			}
		} else {
			nextInChain.handleFileTask(request);
		}
		
	}
}