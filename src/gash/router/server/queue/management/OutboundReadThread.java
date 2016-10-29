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
		if (manager.outboundReadQueue == null)
			throw new RuntimeException("Manager has no outbound read queue");
	}

	@Override
	public void run() {

		while (true) {
			try {
				InternalChannelNode message = manager.dequeueOutboundRead();
				logger.info("Routing outbound read message to  node " + message.getWorkMessage().getHeader().getDestination());
				if (message.getChannel()!= null && message.getChannel().isOpen()) {
					if(message.getChannel().isWritable()){
						ChannelFuture cf = message.getChannel().write(message.getWorkMessage());
						message.getChannel().flush();
						cf.awaitUninterruptibly();
						if(!cf.isSuccess()){
							manager.returnOutboundCommand(message);
						}
					}
				} else {
					manager.returnOutboundCommand(message);
				}
					
			} catch (Exception e) {
				logger.error("Exception thrown in client communcation", e);
				break;
			}
		}

		}
	}

