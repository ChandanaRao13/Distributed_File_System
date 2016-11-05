package gash.router.server.queue.management;

import gash.router.server.message.generator.MessageGenerator;
import io.netty.channel.ChannelFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pipe.work.Work.WorkMessage;


public class InboundWorkWriteQueueThread extends Thread{
	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger(InboundWorkWriteQueueThread.class);
	public InboundWorkWriteQueueThread(QueueManager manager) {
		super();
		this.manager = manager;
		if (manager.inboundWorkWriteQueue == null)
			throw new RuntimeException("Manager has no inbound work write queue");
	}
	
	@Override
	public void run() {

		while (true) {
			try {
				InternalChannelNode internalNode = manager.dequeueInboundWorkWrite();
				
				if(internalNode != null){
					//FileTask ft= workMessage.getFiletask();
					WorkMessage workMessage = internalNode.getWorkMessage();
					//	if(DatabaseHandler.addFile(ft.getFilename(), ft.getChunkCounts(), ft.getChunk().toByteArray(), ft.getChunkNo())){

							WorkMessage workResponseMessage = MessageGenerator.getInstance().generateReplicationAcknowledgementMessage(workMessage);
							QueueManager.getInstance().enqueueOutboundWorkWrite(workResponseMessage, internalNode.getChannel());
				/*		} else {
							logger.info("Data is not replicated in the slave node, enqueuing the message back into the queue");
							QueueManager.getInstance().enqueueOutboundWorkWrite(workMessage, channel);
						} */
				}
				//int destinationNode = message.getWorkMessage().getHeader().getNodeId();
		       	//logger.info("Inbound write work message routing to node " + destinationNode);

			} catch (Exception e) {
				logger.error("Unexpected management communcation failure", e);
				break;
			}

		}
	}
}
