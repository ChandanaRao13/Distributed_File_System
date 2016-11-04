package gash.router.server.queue.management;

import io.netty.channel.ChannelFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OutboundReadThread extends Thread {
	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger(OutboundReadThread.class);

	public OutboundReadThread(QueueManager manager) {
		super();
		this.manager = manager;
		if (manager.outboundWorkReadQueue == null)
			throw new RuntimeException("Manager has no outbound read queue");
	}

	@Override
	public void run() {

		while (true) {
			try {
				InternalChannelNode message = manager.dequeueOutboundRead();
				logger.info("trying to route: ");
				if (message.getChannel()!= null && message.getChannel().isOpen()) {
					if(message.getChannel().isWritable()){
						logger.info("Routing outbound read message to  node " + message.getWorkMessage().getHeader().getDestination());
						ChannelFuture cf = message.getChannel().write(message.getWorkMessage());
						message.getChannel().flush();
						cf.awaitUninterruptibly();
						if(!cf.isSuccess()){
							logger.info("Unsuccessful: failed to send chunk: " + message.getWorkMessage().getFiletask().getChunkNo());
							manager.enqueueOutboundRead(message.getWorkMessage(), message.getChannel());
							logger.info("Adding back to read outbound queue");
						}
						else {
							logger.info("cf.isSuccess() == true: " + cf.isSuccess());
						}
					}
					else {
						logger.info("isWritable: " + message.getChannel().isWritable());
						//manager.returnOutboundWorkWriteQueue(message);
						manager.enqueueOutboundRead(message.getWorkMessage(), message.getChannel());
					}
				} else {
					logger.info("Adding back to read outbound queue: not of message.getChannel()!= null && message.getChannel().isOpen()");
					//manager.returnOutboundWorkWriteQueue(message);
					manager.enqueueOutboundRead(message.getWorkMessage(), message.getChannel());
				}
					
			} catch (Exception e) {
				logger.error("Exception thrown in client communcation", e);
				break;
			}
		}

		}
	}

