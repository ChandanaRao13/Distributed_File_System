package gash.router.server.queue.management;

import gash.router.database.DatabaseHandler;
import gash.router.server.message.generator.MessageGenerator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.Worktype;
import routing.Pipe.FileTask;


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
				//To-do comment it after testing
				if(internalNode != null){
					WorkMessage workMessage = internalNode.getWorkMessage();
					Channel channel = internalNode.getChannel();
					FileTask ft= workMessage.getFiletask();
					if(workMessage.getWorktype() == Worktype.REPLICATE_REQUEST){
						if(DatabaseHandler.addFile(ft.getFilename(), ft.getChunkCounts(), ft.getChunk().toByteArray(), ft.getChunkNo())){

							WorkMessage workResponseMessage = MessageGenerator.getInstance().generateReplicationAcknowledgementMessage(workMessage);
							QueueManager.getInstance().enqueueOutboundWorkWrite(workResponseMessage, channel);
						} else {
							logger.info("Data is not replicated in the slave node, enqueuing the message back into the queue");
							QueueManager.getInstance().enqueueOutboundWorkWrite(workMessage, channel);
						} 
					} else if (workMessage.getWorktype() == Worktype.DELETE_REQUEST){
						if(DatabaseHandler.deleteFile(ft.getFilename())){
							WorkMessage workResponseMessage = MessageGenerator.getInstance().generateDeletionAcknowledgementMessage(workMessage);
							QueueManager.getInstance().enqueueOutboundWorkWrite(workResponseMessage, internalNode.getChannel());
						} else {
							logger.info("Data is not replicated in the slave node, enqueuing the message back into the queue");
							QueueManager.getInstance().enqueueOutboundWorkWrite(workMessage, channel);
						} 					
					}
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
