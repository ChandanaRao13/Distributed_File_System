package gash.router.server.commandmessage.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.queue.management.QueueManager;
import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;

public class CmdMessageHandler implements ICommandChainHandler {
	private ICommandChainHandler nextInChain;
	protected static Logger logger = LoggerFactory.getLogger(CmdMessageHandler.class);

	@Override
	public void setNextChainHandler(ICommandChainHandler nextChain) {

		nextInChain = nextChain;

	}

	/**
	 * handles the message
	 */
	@Override
	public void handleMessage(CommandMessage msg, Channel channel) throws Exception {
		if (msg.hasMessage()) {
			logger.info("Recieved command message: " + msg.getMessage());
			// FileTask fileTask = msg.getFiletask();
			// System.out.println("File name :" + fileTask.getFilename());
			QueueManager.getInstance().enqueueInboundCommmand(msg, channel);
			// databaseHandler.addFile(fileTask.getFilename(),
			// fileTask.getChunk(), fileTask.getChunkNo());
		} else {
			nextInChain.handleMessage(msg, channel);
		}
	}
}