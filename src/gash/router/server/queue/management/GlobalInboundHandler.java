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
import global.Global.RequestType;
import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;

/**
 * 
 * Inbound Global Message Handler
 *
 */
public class GlobalInboundHandler extends Thread {
	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger(GlobalInboundHandler.class);

	public GlobalInboundHandler(QueueManager manager) {
		super();
		this.manager = manager;
		if (manager.globalInboundQueue == null)
			throw new RuntimeException("Manager has no global inbound queue");
	}

	@SuppressWarnings("unused")
	@Override
	public void run() {
		while (true) {
			try {
				InternalChannelNode globalNode = manager.dequeueGlobalInboundQueue();
				GlobalMessage message = globalNode.getGlobalMessage();
				if (message.hasPing()) {
					if (EdgeMonitor.getLeaderId() == EdgeMonitor.getNodeId()) {
						if (message.getGlobalHeader().getDestinationId() != GlobalEdgeMonitor.getClusterId()) {
							GlobalMessage globalMessage = GlobalMessageBuilder.buildPingMessage();
							GlobalEdgeMonitor.broadcastToClusterFriends(globalMessage);
						} else {
							CommandMessage msg = MessageGenerator.getInstance()
									.generateClientResponseMsg("Successfully able to ping all the clusters ");
							/*
							 * String clientId =
							 * message.getRequest().getRequestId();
							 * 
							 * InternalChannelNode ch =
							 * EdgeMonitor.getClientChannelFromMap(clientId);
							 * EdgeMonitor.removeClientChannelInfoFromMap(
							 * clientId);
							 * QueueManager.getInstance().enqueueOutboundCommand
							 * (msg, ch.getChannel());
							 */
						}
					}
				}
				if (message.hasRequest()) {
					if (message.getRequest().getRequestType() == RequestType.READ) {
						if (message.getGlobalHeader().getDestinationId() != GlobalEdgeMonitor.getClusterId()) {
							String filename = message.getRequest().getFileName();
							boolean inRiak = RethinkDatabaseHandler.isFileAvailableInRiak(filename);
							boolean inRethink = RethinkDatabaseHandler.isFileAvailableInRethink(filename);
							if (inRiak || inRethink) {
								// Read from leader database and return to
								// another cluster
								int chunkCount = RethinkDatabaseHandler
										.getFilesChunkCount(message.getRequest().getFileName());
								globalNode.setChunkCount(chunkCount);

								for (int index = 0; index < chunkCount; index++) {
									List<FluffyFile> content = RethinkDatabaseHandler
											.getFileContentWithChunkId(filename, index + 1);
									ByteString byteStringContent = ByteString.copyFrom(content.get(0).getFile());
									GlobalMessage globalMessage = GlobalMessageBuilder
											.generateGlobalReadResponseMessage(globalNode.getGlobalMessage(), index + 1,
													byteStringContent, chunkCount);
									GlobalEdgeMonitor.broadcastToClusterFriends(globalMessage);
								}
							} else {
								GlobalMessage messageForward = GlobalMessageBuilder
										.generateGlobalForwardRequestMessage(message);
								GlobalEdgeMonitor.broadcastToClusterFriends(messageForward);
							}
						} else {
							// Send it back to the Client
							CommandMessage msg = MessageGenerator.getInstance().generateClientResponseMsg(
									"File is not available: " + message.getRequest().getFileName());
							String clientId = message.getRequest().getRequestId();
							InternalChannelNode ch = EdgeMonitor.getClientChannelFromMap(clientId);
							EdgeMonitor.removeClientChannelInfoFromMap(clientId);
							QueueManager.getInstance().enqueueOutboundCommand(msg, ch.getChannel());
						}
					}
				} else if (message.hasResponse()) {
					if (message.getRequest().getRequestType() == RequestType.READ) {
						if (message.getGlobalHeader().getDestinationId() != GlobalEdgeMonitor.getClusterId()) {
							GlobalMessage globalForwardMessage = GlobalMessageBuilder
									.generateGlobalForwardReadResponseMessage(message);
							GlobalEdgeMonitor.broadcastToClusterFriends(globalForwardMessage);
						} else {
							CommandMessage outputMsg = GlobalMessageBuilder.forwardChunkToClient(message);
							InternalChannelNode channelNode = EdgeMonitor
									.getClientChannelFromMap(message.getResponse().getRequestId());
							channelNode.decrementChunkCount();
							if (channelNode.getChunkCount() == 0) {
								logger.info("Removing client info from the client channel Map");
								try {
									EdgeMonitor.removeClientChannelInfoFromMap(message.getResponse().getRequestId());
								} catch (Exception e) {
									logger.info(
											"Client channel is not removed successfully from the client channel Map");
									e.printStackTrace();
								}
							}
							Channel clientChannel = channelNode.getChannel();
							QueueManager.getInstance().enqueueOutboundCommand(outputMsg, clientChannel);
						}
					}
				}

			} catch (Exception e) {
				logger.error("Unexpected management communcation failure: ", e.getMessage());
				break;
			}
		}

	}
}
