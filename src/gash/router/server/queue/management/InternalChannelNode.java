package gash.router.server.queue.management;

import pipe.work.Work.WorkMessage;
import global.Global.GlobalMessage;
import io.netty.channel.Channel;
import routing.Pipe.CommandMessage;

/**
 * Internal Channel Node - An Object to handle messages in Queues
 *
 */
public class InternalChannelNode {

	private CommandMessage commandMessage;
	private WorkMessage workMessage;
	private GlobalMessage globalMessage;
	private Channel channel;
	private boolean isWork;
	private int chunkCount = 0;

	public InternalChannelNode(CommandMessage commandMessage, Channel channel) {
		this.commandMessage = commandMessage;
		this.channel = channel;
		isWork = false;

	}

	public InternalChannelNode(WorkMessage workMessage, Channel channel) {
		this.workMessage = workMessage;
		this.channel = channel;
		isWork = true;
	}

	public InternalChannelNode(GlobalMessage globalMessage, Channel channel) {
		this.globalMessage = globalMessage;
		this.channel = channel;
		isWork = false;

	}

	/**
	 * @return the commandMessage
	 */
	public CommandMessage getCommandMessage() {
		return commandMessage;
	}

	/**
	 * @param commandMessage
	 *            the commandMessage to set
	 */
	public void setCommandMessage(CommandMessage commandMessage) {
		this.commandMessage = commandMessage;
	}

	/**
	 * @return the workMessage
	 */
	public WorkMessage getWorkMessage() {
		return workMessage;
	}

	/**
	 * @param workMessage
	 *            the workMessage to set
	 */
	public void setWorkMessage(WorkMessage workMessage) {
		this.workMessage = workMessage;
	}

	/**
	 * @return the channel
	 */
	public Channel getChannel() {
		return channel;
	}

	/**
	 * @param channel
	 *            the channel to set
	 */
	public void setChannel(Channel channel) {
		this.channel = channel;
	}

	/**
	 * @return the isWork
	 */
	public boolean isWork() {
		return isWork;
	}

	/**
	 * @param isWork
	 *            the isWork to set
	 */
	public void setWork(boolean isWork) {
		this.isWork = isWork;
	}

	/**
	 * @return the chunkCount
	 */
	public int getChunkCount() {
		return chunkCount;
	}

	/**
	 * @param chunkCount
	 *            the chunkCount to set
	 */
	public void setChunkCount(int chunkCount) {
		this.chunkCount = chunkCount;
	}

	public void decrementChunkCount() {
		if (chunkCount <= 0) {
			return;
		} else {
			chunkCount--;
		}
	}

	/**
	 * 
	 * @return
	 */
	public GlobalMessage getGlobalMessage() {
		return globalMessage;
	}

	/**
	 * 
	 * @param globalMessage
	 */
	public void setGlobalMessage(GlobalMessage globalMessage) {
		this.globalMessage = globalMessage;
	}
}
