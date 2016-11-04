package gash.router.server.queue.management;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class InboundReadQueueThread extends Thread {

	private QueueManager manager;
	protected static Logger logger = LoggerFactory.getLogger(InboundCommandQueueThread.class);

	public InboundReadQueueThread(QueueManager manager) {
		super();
		this.manager = manager;
		if (manager.inboundWorkReadQueue == null)
			throw new RuntimeException("Manager has no inbound read queue");
	}

	@Override
	public void run() {

		while (true) {
			try {
				InternalChannelNode message = manager.dequeueInboundRead();	
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}
}