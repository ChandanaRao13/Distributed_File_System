/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.PrintUtil;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.queue.management.QueueManager;
import global.Global.GlobalMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class GlobalHandler extends SimpleChannelInboundHandler<GlobalMessage> {
	protected static Logger logger = LoggerFactory.getLogger(GlobalHandler.class);
	protected GlobalServerState state;
	protected boolean debug = false;

	public GlobalHandler(GlobalServerState state) {
		System.out.println("Initializing Global Handler");
		if (state != null) {
			this.state = state;
		} else {
			return;
		}

	}

	/**
	 * override this method to provide processing behavior. T
	 * 
	 * @param msg
	 */
	public void handleMessage(GlobalMessage msg, Channel channel) {
		System.out.println("Recieved global message");
		try {
			if (msg == null) {
				System.out.println("Error: Received a null message as globalMessage: " + msg);
				logger.info("Error: Received a null message as globalMessage: " + msg);
				return;
			} else if (msg.hasPing()) {
				System.out.println("Info: Received a ping from Cluster Id: " + msg.getGlobalHeader().getClusterId());
				logger.info("Info: Received a ping from Cluster Id: " + msg.getGlobalHeader().getClusterId());
				// state.getEmon().broadcastToClusterFriends(GlobalMessageBuilder.buildPingMessage());
				QueueManager.getInstance().enqueueGlobalInboundQueue(msg, channel);
			} else if (msg.hasMessage()) {
				System.out.println("Info: Received a message: " + msg.getMessage() + " from ClusterId: " + msg.getGlobalHeader().getClusterId());
				logger.info("Info: Received a message: " + msg.getMessage() + " from ClusterId: " + msg.getGlobalHeader().getClusterId());
			} else if (msg.hasRequest()) {
				logger.info("Info: Received a global request");
				if (EdgeMonitor.getLeaderId() == EdgeMonitor.getNodeId()) {
					logger.info("Info: Received a global request and I(leader) am processing it");
					QueueManager.getInstance().enqueueGlobalInboundQueue(msg, channel);
				}
			} else if (msg.hasResponse()) {
				logger.info("Info: Received a global response");
				if (EdgeMonitor.getLeaderId() == EdgeMonitor.getNodeId()) {
					logger.info("Info: Received a global response and I(leader) am processing");
					QueueManager.getInstance().enqueueGlobalInboundQueue(msg, channel);
				}
			}
			if (debug)
				PrintUtil.printCommand(msg);
		} 
		catch (Exception e) {
			logger.info("Error: Couldn't handle the GlobalMessage: " + e.getMessage());
			System.out.println("Error: Couldn't handle the GlobalMessage: " + e.getMessage());
		}
		System.out.flush();
	}

	/**
	 * a message was received from the server. Here we dispatch the message to
	 * the client's thread pool to minimize the time it takes to process other
	 * messages.
	 * 
	 * @param ctx
	 *            The channel the message was received from
	 * @param msg
	 *            The message
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, GlobalMessage msg) throws Exception {
		handleMessage(msg, ctx.channel());
	}

	/**
	 * 
	 * @param ctx
	 * @param cause
	 * @throws Exception
	 */
	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}