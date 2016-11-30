package gash.router.server.queue.management;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import gash.router.cluster.GlobalEdgeMonitor;
import gash.router.database.DatabaseHandler;
import gash.router.database.datatypes.FluffyFile;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.message.generator.MessageGenerator;
import gash.router.server.replication.DataReplicationManager;
import gash.router.server.replication.UpdateFileInfo;
import gash.router.util.GlobalMessageBuilder;
import global.Global.File;
import global.Global.GlobalMessage;
import global.Global.RequestType;
import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;

public class GlobalInboundThread extends Thread {
	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger(GlobalInboundThread.class);

	public GlobalInboundThread(QueueManager manager) {
		super();
		this.manager = manager;
		if (manager.globalInboundQueue == null)
			throw new RuntimeException("Manager has no global inbound queue");
	}

	@Override
	public void run() {

		while (true) {
			try {
				InternalChannelNode globalNode = manager.dequeueglobalInboundQueue();
				GlobalMessage message = globalNode.getGlobalMessage();
				if (message.hasPing()) {
					System.out.println("Got Ping message: " + message);
					if (EdgeMonitor.getLeaderId() == EdgeMonitor.getNodeId()) {
						if (message.getGlobalHeader().getDestinationId() != GlobalEdgeMonitor.getClusterId()) {
							GlobalEdgeMonitor.broadcastToClusterFriends(message);
						} else {
							logger.info("Successfully able to ping all the clusters");
							CommandMessage msg = MessageGenerator.getInstance()
									.generateClientResponseMsg("Successfully able to ping all the clusters ");
							String clientId = message.getRequest().getRequestId();

							InternalChannelNode ch = EdgeMonitor.getClientChannelFromMap(clientId);
							EdgeMonitor.removeClientChannelInfoFromMap(clientId);
							QueueManager.getInstance().enqueueOutboundCommand(msg, ch.getChannel());
						}
					}
				}
				if (message.hasRequest()) {

					if (message.getRequest().getRequestType() == RequestType.READ) {
						if (message.getGlobalHeader().getDestinationId() != GlobalEdgeMonitor.getClusterId()) {
							String filename = message.getRequest().getFileName();
							boolean inRiak = DatabaseHandler.isFileAvailableInRiak(filename);
							boolean inRethink = DatabaseHandler.isFileAvailableInRethink(filename);
							if (inRiak || inRethink) {
								// Read from leader database and return to
								// another cluster

								int chunkCount = DatabaseHandler.getFilesChunkCount(message.getRequest().getFileName());
								globalNode.setChunkCount(chunkCount);

								for (int index = 0; index < chunkCount; index++) {
									List<FluffyFile> content = DatabaseHandler.getFileContentWithChunkId(filename,
											index);
									ByteString byteStringContent = ByteString.copyFrom(content.get(0).getFile());
									GlobalMessage globalMessage = GlobalMessageBuilder
											.generateGlobalReadResponseMessage(globalNode.getGlobalMessage(), index,
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
					} else if (message.getRequest().getRequestType() == RequestType.UPDATE) {
						if (message.getGlobalHeader().getDestinationId() != GlobalEdgeMonitor.getClusterId()) {
							File file = message.getRequest().getFile();
							boolean inRiak = DatabaseHandler.isFileAvailableInRiak(file.getFilename());
							boolean inRethink = DatabaseHandler.isFileAvailableInRethink(file.getFilename());
							String filename = file.getFilename();
							if (inRiak || inRethink) {
								logger.info("Deleting the file from database to update : " + filename);
								if (!DataReplicationManager.fileUpdateTracker.containsKey(filename)) {
									UpdateFileInfo fileInfo = new UpdateFileInfo(file.getTotalNoOfChunks());
									DataReplicationManager.fileUpdateTracker.put(filename, fileInfo);

									if (DatabaseHandler.deleteFile(filename)) {
										DataReplicationManager.getInstance().broadcastUpdateDeletion(message);
									} else {
										logger.error(
												"Global Check: File requested to update operation failed, in step to delete from the database");
									}
								}
								UpdateFileInfo fileInfo = DataReplicationManager.fileUpdateTracker.get(filename);

								if (DatabaseHandler.addFile(file.getFilename(), file.getTotalNoOfChunks(),
										file.getData().toByteArray(), file.getChunkId())) {
									fileInfo.decrementChunkProcessed();

									logger.info("File is updated successfully with chunk id : " + file.getChunkId());
									DataReplicationManager.getInstance().broadcastUpdateReplication(message);
								} else {
									logger.error(
											"Global check : Database write error, couldnot update the file into the database");
								}
								if (fileInfo.getChunksProcessed() > 0) {
									DataReplicationManager.fileUpdateTracker.put(filename, fileInfo);
								} else {
									DataReplicationManager.fileUpdateTracker.remove(filename);
								}

								logger.info("File " + file.getFilename() + " is deleted successfully with chunck id: "
										+ file.getChunkId());
							}

							if (EdgeMonitor.getLeaderId() == EdgeMonitor.getNodeId()) {
								GlobalEdgeMonitor.broadcastToClusterFriends(message);
							} else {
								logger.info("Send update requests to leader");
							}
						} else {
							CommandMessage msg = MessageGenerator.getInstance().generateClientResponseMsg(
									"File is updated successfully: " + message.getRequest().getFileName());
							String clientId = message.getRequest().getRequestId();

							InternalChannelNode ch = EdgeMonitor.getClientChannelFromMap(clientId);
							EdgeMonitor.removeClientChannelInfoFromMap(clientId);
							QueueManager.getInstance().enqueueOutboundCommand(msg, ch.getChannel());
						}
					} else if (message.getRequest().getRequestType() == RequestType.WRITE) {
						logger.info("Recieved global message to write");
						if (message.getGlobalHeader().getDestinationId() != GlobalEdgeMonitor.getClusterId()) {
							File file = message.getRequest().getFile();
							if (DatabaseHandler.addFile(file.getFilename(), file.getTotalNoOfChunks(),
									file.getData().toByteArray(), file.getChunkId())) {
								logger.info("Global check:  File" + file.getFilename()
										+ "is stored successfully in the cluster with chunkId:" + file.getChunkId());
								DataReplicationManager.getInstance().broadcastReplication(message);
							} else {
								logger.error(
										"Global check: Database write error, couldnot save the file into the database");
							}
							GlobalEdgeMonitor.broadcastToClusterFriends(message);

						} else {
							CommandMessage msg = MessageGenerator.getInstance().generateClientResponseMsg(
									"File is written successfully: " + message.getRequest().getFileName());
							String clientId = message.getRequest().getRequestId();

							InternalChannelNode ch = EdgeMonitor.getClientChannelFromMap(clientId);
							EdgeMonitor.removeClientChannelInfoFromMap(clientId);
							QueueManager.getInstance().enqueueOutboundCommand(msg, ch.getChannel());
						}
					} else if (message.getRequest().getRequestType() == RequestType.DELETE) {
						if (message.getGlobalHeader().getDestinationId() != GlobalEdgeMonitor.getClusterId()) {
							String filename = message.getRequest().getFileName();
							if (DatabaseHandler.isFileAvailable(filename)) {
								logger.info("Deleting the file from database : " + filename);
								if (DatabaseHandler.deleteFile(filename)) {
									DataReplicationManager.getInstance().broadcastDeletion(message);
								} else {
									logger.error(
											"Global check: File requested to delete is not deleted from the database");
								}
							}

							GlobalEdgeMonitor.broadcastToClusterFriends(message);
						} else {
							CommandMessage msg = MessageGenerator.getInstance().generateClientResponseMsg(
									"File is successfully deleted: " + message.getRequest().getFileName());
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
							System.out.println("Received message from cluster for client");
							CommandMessage outputMsg = GlobalMessageBuilder.forwardChunkToClient(message);
							logger.info("Client id is :" + message.getResponse().getRequestId() + "length: "
									+ message.getResponse().getRequestId().length() + "channel: "
									+ EdgeMonitor.getClientChannelFromMap(message.getResponse().getRequestId()));
							InternalChannelNode channelNode = EdgeMonitor
									.getClientChannelFromMap(message.getResponse().getRequestId());

							if (channelNode.getChunkCount() == 0) {
								channelNode.setChunkCount(message.getResponse().getFile().getTotalNoOfChunks());
							}
							channelNode.decrementChunkCount();
							if (channelNode.getChunkCount() == 0) {

								logger.info("Removing client info from the client channel Map in Response");
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
				logger.error("Unexpected management communcation failure", e.getMessage());
				break;
			}
		}

	}
}
