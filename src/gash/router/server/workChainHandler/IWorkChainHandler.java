package gash.router.server.workChainHandler;

import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;

public interface IWorkChainHandler {
	public void setNextChain(IWorkChainHandler nextChain);
	public void handle(WorkMessage workMessage, Channel channel);
}