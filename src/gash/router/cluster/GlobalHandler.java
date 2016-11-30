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
		//System.out.println("Global message received clusterID: " + msg.getGlobalHeader().getClusterId() + " and destinationId: " + msg.getGlobalHeader().getDestinationId());
		if (msg == null) {
			// TODO add logging
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}

		System.out.println("Global message received clusterID: " + msg.getGlobalHeader().getClusterId() + " and destinationId: " + msg.getGlobalHeader().getDestinationId());
		if(msg.hasPing()){
			System.out.println("Got Ping from Cluster Id:"+msg.getGlobalHeader().getClusterId());
			//state.getEmon().broadcastToClusterFriends(GlobalMessageBuilder.buildPingMessage());
			QueueManager.getInstance().enqueueglobalInboundQueue(msg, channel);
		}
		else if(msg.hasMessage()){
			System.out.println("Recieved -->"+msg.getMessage()+" from Cluster Id: "+msg.getGlobalHeader().getClusterId());
		} else if(msg.hasRequest()){
			if(EdgeMonitor.getLeaderId() == EdgeMonitor.getNodeId()) {
				QueueManager.getInstance().enqueueglobalInboundQueue(msg, channel);
			}
		} else if(msg.hasResponse()){
			if(EdgeMonitor.getLeaderId() == EdgeMonitor.getNodeId()) {
				System.out.println("I am sending to the client");
				QueueManager.getInstance().enqueueglobalInboundQueue(msg, channel);
			}
		} 

		if (debug)
			PrintUtil.printCommand(msg);
		try {


		} catch (Exception e) {
			logger.error("Error: Exception caught: " + e.getMessage());
			System.out.println("Caught Exception in Global Handler!!!!!!!!!!!!!!!!!!!!!!");
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

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}