package gash.router.server.queue.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.database.RethinkDatabaseHandler;
import gash.router.server.message.generator.MessageGenerator;
import gash.router.server.replication.DataReplicationManager;
import gash.router.server.replication.UpdateFileInfo;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.Worktype;
import routing.Pipe.FileTask;

public class InboundWriteWorkMsgHandler extends Thread {
	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger(InboundWriteWorkMsgHandler.class);

	public InboundWriteWorkMsgHandler(QueueManager manager) {
		super();
		this.manager = manager;
		if (manager.inboundWriteWorkQueue == null)
			throw new RuntimeException("Manager has no inbound work write queue");
	}

	@Override
	public void run() {
		while (true) {
			try {
				InternalChannelNode internalNode = manager.dequeueInboundWriteWork();
				// To-do comment it after testing
				if (internalNode != null) {
					WorkMessage workMessage = internalNode.getWorkMessage();
					Channel channel = internalNode.getChannel();
					FileTask ft = workMessage.getFiletask();
					if (workMessage.getWorktype() == Worktype.REPLICATE_REQUEST) {
						if (RethinkDatabaseHandler.addFile(ft.getFilename(), ft.getChunkCounts(),
								ft.getChunk().toByteArray(), ft.getChunkNo())) {
							WorkMessage workResponseMessage = MessageGenerator.getInstance()
									.generateReplicationAcknowledgementMessage(workMessage);
							QueueManager.getInstance().enqueueOutboundWriteWork(workResponseMessage, channel);
						} else {
							logger.info(
									"Data is not replicated in the slave node, enqueuing the message back into the queue");
							QueueManager.getInstance().enqueueInboundWriteWork(workMessage, channel);
						}
					} else if (workMessage.getWorktype() == Worktype.DELETE_REQUEST) {
						if (RethinkDatabaseHandler.deleteFile(ft.getFilename())) {
							WorkMessage workResponseMessage = MessageGenerator.getInstance()
									.generateDeletionAcknowledgementMessage(workMessage);
							QueueManager.getInstance().enqueueOutboundWriteWork(workResponseMessage,
									internalNode.getChannel());
						} else {
							logger.info(
									"Data is not replicated in the slave node, enqueuing the message back into the queue");
							QueueManager.getInstance().enqueueInboundWriteWork(workMessage, channel);
						}
					} else if (workMessage.getWorktype() == Worktype.UPDATE_DELETE_REQUEST) {
						String filename = workMessage.getFiletask().getFilename();
						FileTask fileTask = workMessage.getFiletask();
						if (!DataReplicationManager.fileUpdateTracker.containsKey(filename)) {
							UpdateFileInfo fileInfo = new UpdateFileInfo(fileTask.getChunkCounts());
							DataReplicationManager.fileUpdateTracker.put(filename, fileInfo);
							logger.info("Trying to delete the file, in slave");

							if (RethinkDatabaseHandler.deleteFile(filename)) {
								WorkMessage workResponseMessage = MessageGenerator.getInstance()
										.generateUpdateDeletionAcknowledgementMessage(workMessage);
								QueueManager.getInstance().enqueueOutboundWriteWork(workResponseMessage, channel);
							} else {
								logger.info(
										"Data of the updated file is not deleted in the slave node, enqueuing the message back into the queue");
								QueueManager.getInstance().enqueueInboundWriteWork(workMessage, channel);
							}
						}
					} else if (workMessage.getWorktype() == Worktype.UPDATE_REPLICATE_REQUEST) {
						String filename = workMessage.getFiletask().getFilename();
						FileTask fileTask = workMessage.getFiletask();
						if (!DataReplicationManager.fileUpdateTracker.containsKey(filename)) {
							logger.info(
									"Update replication cannot be performed before recieving delete request.... enqueuing the message back into the queue");
							QueueManager.getInstance().enqueueInboundWriteWork(workMessage, channel);
						} else {
							UpdateFileInfo fileInfo = DataReplicationManager.fileUpdateTracker.get(filename);

							if (RethinkDatabaseHandler.addFile(fileTask.getFilename(), fileTask.getChunkCounts(),
									fileTask.getChunk().toByteArray(), fileTask.getChunkNo())) {
								fileInfo.decrementChunkProcessed();

								WorkMessage workResponseMessage = MessageGenerator.getInstance()
										.generateUpdateReplicationAcknowledgementMessage(workMessage);
								QueueManager.getInstance().enqueueOutboundWriteWork(workResponseMessage, channel);

							} else {
								logger.info(
										"Update replication is not successful.... enqueuing the message back into the queue");
								QueueManager.getInstance().enqueueInboundWriteWork(workMessage, channel);
							}

							if (fileInfo.getChunksProcessed() > 0) {
								DataReplicationManager.fileUpdateTracker.put(filename, fileInfo);
							} else {
								DataReplicationManager.fileUpdateTracker.remove(filename);
							}
						}
					}
				}
				// int destinationNode =
				// message.getWorkMessage().getHeader().getNodeId();
				// logger.info("Inbound write work message routing to node " +
				// destinationNode);

			} catch (Exception e) {
				logger.error("Unexpected management communcation failure", e);
				break;
			}

		}
	}
}
