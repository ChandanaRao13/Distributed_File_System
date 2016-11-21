package gash.router.server.queue.management;

import io.netty.channel.Channel;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import global.Global.GlobalMessage;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

public class QueueManager {
	protected static Logger logger = LoggerFactory.getLogger(QueueManager.class);
	protected static AtomicReference<QueueManager> instance = new AtomicReference<QueueManager>();

	/**
	 * Command queues
	 */
	protected LinkedBlockingDeque<InternalChannelNode> inboundCommandQueue;
	protected LinkedBlockingDeque<InternalChannelNode> outboundCommandQueue;

	/**
	 * Read Work Queues
	 */
	protected LinkedBlockingDeque<InternalChannelNode> outboundReadWorkQueue;
	protected LinkedBlockingDeque<InternalChannelNode> inboundReadWorkQueue;

	/**
	 * Write Work Queues
	 */
	protected LinkedBlockingDeque<InternalChannelNode> outboundWriteWorkQueue;
	protected LinkedBlockingDeque<InternalChannelNode> inboundWriteWorkQueue;

	/**
	 * Global Queues
	 */
	protected LinkedBlockingDeque<InternalChannelNode> globalInboundQueue;
	protected LinkedBlockingDeque<InternalChannelNode> globalOutboundQueue;

	protected InboundCommandMsgHandler inboundCommandMsgHandler;
	protected OutboundCommandMsgHandler outboundCommandMsgHandler;

	protected InboundWriteWorkMsgHandler inboundWriteWorkMsgHandler;
	protected OutboundWriteWorkMsgHandler outboundWriteWorkMsgHandler;

	protected InboundReadWorkMsgHandler inboundReadWorkMsgHandler;
	protected OutboundReadWorkMsgHandler outboundReadWorkMsgHandler;

	/**
	 * Global Threads
	 */
	protected GlobalInboundHandler inboundGlobalHandler;
	protected GlobalOutboundHandler outboundGlobalHandler;

	public static QueueManager initManager() {
		instance.compareAndSet(null, new QueueManager());
		return instance.get();
	}

	public static QueueManager getInstance() {
		if (instance == null)
			instance.compareAndSet(null, new QueueManager());
		return instance.get();
	}

	public QueueManager() {
		logger.info(" Started the Manager ");

		inboundCommandQueue = new LinkedBlockingDeque<InternalChannelNode>();
		inboundCommandMsgHandler = new InboundCommandMsgHandler(this);
		inboundCommandMsgHandler.start();

		outboundWriteWorkQueue = new LinkedBlockingDeque<InternalChannelNode>();
		outboundWriteWorkMsgHandler = new OutboundWriteWorkMsgHandler(this);
		outboundWriteWorkMsgHandler.start();

		outboundCommandQueue = new LinkedBlockingDeque<InternalChannelNode>();
		outboundCommandMsgHandler = new OutboundCommandMsgHandler(this);
		outboundCommandMsgHandler.start();

		outboundReadWorkQueue = new LinkedBlockingDeque<InternalChannelNode>();
		outboundReadWorkMsgHandler = new OutboundReadWorkMsgHandler(this);
		outboundReadWorkMsgHandler.start();

		inboundReadWorkQueue = new LinkedBlockingDeque<InternalChannelNode>();
		inboundReadWorkMsgHandler = new InboundReadWorkMsgHandler(this);
		inboundReadWorkMsgHandler.start();

		inboundWriteWorkQueue = new LinkedBlockingDeque<InternalChannelNode>();
		inboundWriteWorkMsgHandler = new InboundWriteWorkMsgHandler(this);
		inboundWriteWorkMsgHandler.start();

		globalInboundQueue = new LinkedBlockingDeque<InternalChannelNode>();
		inboundGlobalHandler = new GlobalInboundHandler(this);
		inboundGlobalHandler.start();

		globalOutboundQueue = new LinkedBlockingDeque<InternalChannelNode>();
		outboundGlobalHandler = new GlobalOutboundHandler(this);
		outboundGlobalHandler.start();
	}

	/** Global Inbound Enqueue and Dequeue -start **/
	/**
	 * 
	 * @param message
	 * @param ch
	 */
	public void enqueueGlobalInboundQueue(GlobalMessage message, Channel ch) {
		try {
			InternalChannelNode entry = new InternalChannelNode(message, ch);
			globalInboundQueue.put(entry);
		} catch (InterruptedException e) {
			logger.error("global message not enqueued for processing", e);
		}
	}

	/**
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	public InternalChannelNode dequeueGlobalInboundQueue() throws InterruptedException {
		return globalInboundQueue.take();
	}

	/** Global Inbound Enqueue and Dequeue - end **/

	/** Global Outbound Enqueue and Dequeue - start **/
	/**
	 * 
	 * @param message
	 * @param ch
	 */
	public void enqueueGlobalOutboundQueue(GlobalMessage message, Channel ch) {
		try {
			InternalChannelNode entry = new InternalChannelNode(message, ch);
			globalOutboundQueue.put(entry);
		} catch (InterruptedException e) {
			logger.error("global message not enqueued for processing", e);
		}
	}

	/**
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	public InternalChannelNode dequeueGlobalOutboundQueue() throws InterruptedException {
		return globalOutboundQueue.take();
	}

	/** Global Outbound Enqueue and Dequeue - end **/

	public void returnOutboundGlobalMessage(InternalChannelNode channelNode) throws InterruptedException {
		globalOutboundQueue.putFirst(channelNode);
	}

	/** Inbound Command Enqueue and Dequeue - start **/
	/**
	 * 
	 * @param message
	 * @param ch
	 */
	public void enqueueInboundCommmand(CommandMessage message, Channel ch) {
		try {
			InternalChannelNode entry = new InternalChannelNode(message, ch);
			inboundCommandQueue.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}

	public InternalChannelNode dequeueInboundCommmand() throws InterruptedException {
		return inboundCommandQueue.take();
	}
	/** Inbound Command Enqueue and Dequeue - end **/

	/** Oubound Write Work enqueue and Dequeue - start**/
	/**
	 * 
	 * @param message
	 * @param ch
	 */
	public void enqueueOutboundWriteWork(WorkMessage message, Channel ch) {
		try {
			InternalChannelNode entry = new InternalChannelNode(message, ch);
			outboundWriteWorkQueue.put(entry);
		} catch (InterruptedException e) {
			logger.error("write work message is not enqueued for processing", e);
		}
	}

	/**
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	public InternalChannelNode dequeueOutboundWriteWork() throws InterruptedException {
		return outboundWriteWorkQueue.take();
	}
	/** Oubound Write Work enqueue and Dequeue - end**/

	/**
	 * 
	 * @param channelNode
	 * @throws InterruptedException
	 */
	public void returnOutboundWorkWrite(InternalChannelNode channelNode) throws InterruptedException {
		outboundWriteWorkQueue.putFirst(channelNode);
	}

	/**
	 * 
	 * @param channelNode
	 * @throws InterruptedException
	 */
	public void returnOutboundWorkRead(InternalChannelNode channelNode) throws InterruptedException {
		outboundReadWorkQueue.putFirst(channelNode);
	}

	public void enqueueOutboundCommmand(CommandMessage message, Channel ch) {
		try {
			InternalChannelNode entry = new InternalChannelNode(message, ch);
			outboundCommandQueue.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued in outbound command queue for processing", e);
		}
	}

	public InternalChannelNode dequeueOutboundCommmand() throws InterruptedException {
		return outboundCommandQueue.take();
	}

	public void enqueueAtFrontOutboundCommand(InternalChannelNode channelNode) throws InterruptedException {
		outboundCommandQueue.putFirst(channelNode);
	}

	/** Enqueue and Dequeue Outbound Read Work Queue -start**/
	/**
	 * 
	 * @param message
	 * @param ch
	 */
	public void enqueueOutboundRead(WorkMessage message, Channel ch) {
		try {
			InternalChannelNode entry = new InternalChannelNode(message, ch);
			outboundReadWorkQueue.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued in outbound command queue for processing", e);
		}
	}

	/**
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	public InternalChannelNode dequeueOutboundRead() throws InterruptedException {
		if (outboundReadWorkQueue.size() == 0)
			return null;
		return outboundReadWorkQueue.take();
	}
	/** Enqueue and Dequeue Outbound Read Work Queue - end**/

	/**Enqueue and Dequeue Inbound Read Queue - start**/
	/**
	 * 
	 * @param message
	 * @param ch
	 */
	public void enqueueInboundRead(WorkMessage message, Channel ch) {
		try {
			InternalChannelNode entry = new InternalChannelNode(message, ch);
			inboundReadWorkQueue.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued in outbound command queue for processing", e);
		}
	}

	/**
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	public InternalChannelNode dequeueInboundRead() throws InterruptedException {
		if (inboundReadWorkQueue.size() == 0)
			return null;
		return inboundReadWorkQueue.take();
	}
	/**Enqueue and Dequeue Inbound Read Queue - end**/

	/**Enqueue and Dequeue Outbound Command Queue - start**/
	/**
	 * 
	 * @param message
	 * @param ch
	 */
	public void enqueueOutboundCommand(CommandMessage message, Channel ch) {
		try {
			InternalChannelNode entry = new InternalChannelNode(message, ch);
			outboundCommandQueue.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued in outbound command queue for processing", e);
		}
	}

	/**
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	public InternalChannelNode dequeueOutboundCommand() throws InterruptedException {
		return outboundCommandQueue.take();
	}
	/**Enqueue and Dequeue Outbound Command Queue - end**/

	/**Enqueue and Dequeue Inbound Write Work - start**/
	/**
	 * 
	 * @param message
	 * @param ch
	 */
	public void enqueueInboundWriteWork(WorkMessage message, Channel ch) {
		try {
			InternalChannelNode entry = new InternalChannelNode(message, ch);
			inboundWriteWorkQueue.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued in inbound work write queue for processing", e);
		}
	}

	/**
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	public InternalChannelNode dequeueInboundWriteWork() throws InterruptedException {
		return inboundWriteWorkQueue.take();
	}
	/**Enqueue and Dequeue Inbound Write Work - end**/

	/**
	 * method for new node to see if the inbound write queue is empty
	 */
	public boolean isInboundWorkWriteQEmpty() {
		return this.inboundWriteWorkQueue.isEmpty();
	}
}
