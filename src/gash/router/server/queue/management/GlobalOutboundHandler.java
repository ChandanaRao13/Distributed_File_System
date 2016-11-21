package gash.router.server.queue.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelFuture;

/**
 * 
 * OutBound Global Message Handler
 *
 */
public class GlobalOutboundHandler extends Thread {
	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger(GlobalOutboundHandler.class);

	public GlobalOutboundHandler(QueueManager manager) {
		super();
		this.manager = manager;
		if (manager.globalOutboundQueue == null)
			throw new RuntimeException("Manager has no global outbound queue");
	}

	@Override
	public void run() {
		while (true) {
			try {
				InternalChannelNode message = manager.dequeueGlobalOutboundQueue();
				logger.info("Routing global message to next cluster ");
				if (message.getChannel() != null && message.getChannel().isOpen()) {
					ChannelFuture cf = message.getChannel().writeAndFlush(message.getGlobalMessage());
					// message.getChannel().flush();
					cf.awaitUninterruptibly();
					if (cf.isSuccess()) {
						logger.info("Wrote message to the channel of another cluster");
					} else {
						manager.returnOutboundGlobalMessage(message);
					}
				} else {
					logger.info("Checking if channel is null : " + (message.getChannel() == null));
					manager.returnOutboundGlobalMessage(message);
				}
			} catch (InterruptedException ie) {
				logger.error("Error: Interrupted exception while communicating: " + ie.getMessage());
				break;
			} catch (Exception e) {
				logger.error("Exception thrown in client communcation: ", e.getMessage());
				break;
			}
		}
	}
}
