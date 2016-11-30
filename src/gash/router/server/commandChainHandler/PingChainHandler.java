package gash.router.server.commandChainHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.cluster.GlobalEdgeMonitor;
import gash.router.util.GlobalMessageBuilder;
import global.Global.GlobalMessage;
import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;

public class PingChainHandler implements ICommandChainHandler {
	@SuppressWarnings("unused")
	private ICommandChainHandler nextInChain;
	protected static Logger logger = LoggerFactory.getLogger(PingChainHandler.class);

	public void setNextChainHandler(ICommandChainHandler nextChain) {

		this.nextInChain = nextChain;

	}

	@Override
	public void handleMessage(CommandMessage msg, Channel channel) throws Exception {

		if (msg.hasPing()) {
			logger.info("ping recieved from client " + GlobalEdgeMonitor.getClusterId());
			//String clientId = EdgeMonitor.clientInfoMap(new InternalChannelNode(msg, channel));
			GlobalMessage globalMessage = GlobalMessageBuilder.generateForwardGlobalPingMessage(msg);
			GlobalEdgeMonitor.broadcastToClusterFriends(globalMessage);

		} else {
			logger.error("Handles only ping and message for now");
		}
	}
}