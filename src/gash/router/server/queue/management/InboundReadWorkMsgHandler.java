package gash.router.server.queue.management;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import gash.router.cluster.GlobalEdgeMonitor;
import gash.router.database.RethinkDatabaseHandler;
import gash.router.database.datatypes.FluffyFile;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.message.generator.MessageGenerator;
import gash.router.util.GlobalMessageBuilder;
import global.Global.GlobalMessage;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.Worktype;
import routing.Pipe.CommandMessage;
import routing.Pipe.FileTask;

public class InboundReadWorkMsgHandler extends Thread {

	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger(InboundReadWorkMsgHandler.class);

	public InboundReadWorkMsgHandler(QueueManager manager) {
		super();
		this.manager = manager;
		if (manager.inboundReadWorkQueue == null)
			throw new RuntimeException("Manager has no inbound read queue");
	}

	@Override
	public void run() {

		while (true) {
			try {
				InternalChannelNode internalNode = manager.dequeueInboundRead();
				if (internalNode == null) {
					Thread.sleep(2 * 1000);
					// do the "read job" work stealing
					Channel channel = EdgeMonitor.fetchChannelToStealReadWork();
					if (channel != null) {
						// logger.info("I am free and found a channel to look
						// for stealing message, sending request");
						WorkMessage workMessage = MessageGenerator.getInstance().generateWorkReadStealMessage();
						QueueManager.getInstance().enqueueOutboundRead(workMessage, channel);
					} else {
						// Did not find any channel
						// logger.info("Did not find any channel for steal
						// sending");
					}
				} else {
					WorkMessage workMessage = internalNode.getWorkMessage();
					if (workMessage.getWorktype() != Worktype.GLOBAL_READ_REQUEST) {
						Channel channel = internalNode.getChannel();
						logger.info("Received message to read a file");
						FileTask ft = workMessage.getFiletask();
						int chunkId = ft.getChunkNo();
						String filename = ft.getFilename();

						System.out.println("Reading file: " + filename + " and chunkID : " + chunkId);
						List<FluffyFile> content = RethinkDatabaseHandler.getFileContentWithChunkId(filename, chunkId);
						ByteString byteStringContent = ByteString.copyFrom(content.get(0).getFile());
						int chunkCount = content.get(0).getTotalChunks();
						WorkMessage msg = MessageGenerator.getInstance().generateReadRequestResponseMessage(ft,
								byteStringContent, chunkId, chunkCount, workMessage.getRequestId(),
								workMessage.getHeader().getNodeId(), ft.getFilename());

						if (workMessage.hasSteal()) {
							channel = EdgeMonitor.node2ChannelMap.get(workMessage.getHeader().getNodeId());
							if (channel == null) {
								// It is same node which got request from client
								InternalChannelNode clientInfo = EdgeMonitor
										.getClientChannelFromMap(workMessage.getRequestId());
								Channel clientChannel = clientInfo.getChannel();

								clientInfo.decrementChunkCount();
								if (clientInfo.getChunkCount() == 0) {
									logger.info("Removing client info from the client channel Map");
									try {
										EdgeMonitor.removeClientChannelInfoFromMap(workMessage.getRequestId());
									} catch (Exception e) {
										logger.info(
												"Client channel is not removed successfully from the client channel Map");
										e.printStackTrace();
									}
								}

								CommandMessage outputMsg = MessageGenerator.getInstance().forwardChunkToClient(msg);
								QueueManager.getInstance().enqueueOutboundCommand(outputMsg, clientChannel);
							}
						}
						QueueManager.getInstance().enqueueOutboundRead(msg, channel);

					} else {
						GlobalMessage globalMessage = GlobalMessageBuilder.generateGlobalRequestMessage(
								workMessage.getFiletask().getFilename(), workMessage.getRequestId());
						GlobalEdgeMonitor.broadcastToClusterFriends(globalMessage);
					}
				}
			} catch (InterruptedException e) {
				logger.info("Error: InterruptedException in inbound read work msg handler: " + e.getMessage());
				e.printStackTrace();
			} catch (Exception e) {
				logger.info("Error: Exception in inbound read work msg handler: " + e.getMessage());
				e.printStackTrace();
			}
		}
	}
}