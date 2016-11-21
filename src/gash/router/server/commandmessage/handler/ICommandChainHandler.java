package gash.router.server.commandmessage.handler;

import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;

/**
 * Chain Handler Interface for Command Message
 * @author Madhuri
 *
 */
public interface ICommandChainHandler {
	public void setNextChainHandler(ICommandChainHandler nextChain);
	public void handleMessage(CommandMessage msg, Channel channel) throws Exception;
}