package gash.router.server.commandmessage.routerhandlers;

import gash.router.server.queue.management.InternalChannelNode;

/**
 * 
 * Interface for CommandMessageRouterHandler
 *
 */
public interface ICommandRouterHandlers {
	public void setNextChainHandler(ICommandRouterHandlers nextChain);
	public void handleFileTask(InternalChannelNode request) throws Exception;
}