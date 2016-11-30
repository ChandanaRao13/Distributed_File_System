package gash.router.server.commandChainHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.queue.management.QueueManager;
import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;

public class MessageChainHandler implements ICommandChainHandler {
	private ICommandChainHandler nextInChain;
	protected static Logger logger = LoggerFactory.getLogger(MessageChainHandler.class);

	@Override
	public void setNextChainHandler(ICommandChainHandler nextChain) {

		nextInChain = nextChain;

	}

	@Override
	public void handleMessage(CommandMessage msg, Channel channel) throws Exception {
		if (msg.hasMessage()) {
			logger.info("Recieved message:" + msg.getMessage());
			QueueManager.getInstance().enqueueInboundCommmand(msg, channel);
		} else {
			nextInChain.handleMessage(msg, channel);
		}
	}
}