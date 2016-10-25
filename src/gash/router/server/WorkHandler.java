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
package gash.router.server;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import gash.router.server.workChainHandler.FailureHandler;
import gash.router.server.workChainHandler.HeartBeatHandler;
import gash.router.server.workChainHandler.IWorkChainHandler;
import gash.router.server.workChainHandler.PingHandler;
import gash.router.server.workChainHandler.TaskHandler;
import gash.router.server.workChainHandler.WorkStateHandler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.edges.EdgeInfo;
import gash.router.server.edges.EdgeList;
import gash.router.util.RaftMessageBuilder;
import io.netty.channel.ChannelFuture;
import pipe.common.Common.Failure;
import pipe.common.Common.Header;
import pipe.election.Election;
import pipe.election.Election.LeaderStatus.LeaderQuery;
import pipe.election.Election.LeaderStatus.LeaderState;
import pipe.election.Election.RaftElectionMessage.ElectionMessageType;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.Task;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;

/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class WorkHandler extends SimpleChannelInboundHandler<WorkMessage> {
	protected static Logger logger = LoggerFactory.getLogger(WorkHandler.class);
	protected ServerState state;
	protected boolean debug = false;

	IWorkChainHandler hearBeatChainHandler;
	IWorkChainHandler pingMessageChainHandler;
	IWorkChainHandler failureMessageChainHandler;
	IWorkChainHandler taskMessageChainHandler;
	IWorkChainHandler workStateMessageChainHandler;
	public WorkHandler(ServerState state) {
		if (state != null) {
			this.state = state;
		}
		this.hearBeatChainHandler = new HeartBeatHandler();
		this.pingMessageChainHandler = new PingHandler();
		this.failureMessageChainHandler = new FailureHandler();
		this.taskMessageChainHandler = new TaskHandler();
		this.workStateMessageChainHandler = new WorkStateHandler();

		this.hearBeatChainHandler.setNextChain(pingMessageChainHandler);
		this.pingMessageChainHandler.setNextChain(failureMessageChainHandler);
		this.failureMessageChainHandler.setNextChain(taskMessageChainHandler);
		this.taskMessageChainHandler.setNextChain(workStateMessageChainHandler);
	}

	/**
	 * override this method to provide processing behavior. T
	 * 
	 * @param msg
	 */
	public void handleMessage(WorkMessage msg, Channel channel) {
		if (msg == null) {
			// TODO add logging
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}

		if (debug)
			PrintUtil.printWork(msg);

		// TODO How can you implement this without if-else statements?
		try {
			hearBeatChainHandler.handle(msg, channel);
			if(msg.hasRaftMessage() && msg.getRaftMessage().getType() == ElectionMessageType.VOTE_REQUEST) {
				state.getElectionCtx().getCurrentState().VoteRequestReceived(msg);
			} else if(msg.hasRaftMessage() && msg.getRaftMessage().getType() == ElectionMessageType.VOTE_RESPONSE) {
				state.getElectionCtx().getCurrentState().voteRecieved(msg);
			} else if(msg.hasRaftMessage() && msg.getRaftMessage().getType() == ElectionMessageType.LEADER_HEARTBEAT) {
				//System.out.println("Rceived heart beat");
				state.getElectionCtx().getCurrentState().getHearbeatFromLeader(msg);
			}else if (msg.hasNewNode()) {
				logger.info("NEW NODE TRYING TO CONNECT " + msg.getHeader().getNodeId());
				WorkMessage wm = state.getEmon().createRoutingMsg();

				ChannelFuture cf = channel.write(wm);
				channel.flush();
				cf.awaitUninterruptibly();
				if (cf.isDone() && !cf.isSuccess()) {
					logger.info("Failed to write the message to the channel ");
				}
			
				SocketAddress remoteAddress = channel.remoteAddress();
				InetSocketAddress addr = (InetSocketAddress) remoteAddress;

				state.getEmon().createInboundIfNew(msg.getHeader().getNodeId(), addr.getHostName(), 5100);
				state.getEmon().getInBoundEdgesList().getNode(msg.getHeader().getNodeId()).setChannel(channel);

			}else if (msg.hasFlagRouting()) {
				logger.info("Routing information recieved " + msg.getHeader().getNodeId());
				//logger.info("Routing Entries: " + msg.getRoutingEntries());

				System.out.println("CONNECTED TO NEW NODE---------------------------------");
				SocketAddress remoteAddress = channel.remoteAddress();
				InetSocketAddress addr = (InetSocketAddress) remoteAddress;

				state.getEmon().createOutboundIfNew(msg.getHeader().getNodeId(), addr.getHostName(), 5100);

				System.out.println(addr.getHostName());

			} else if (msg.hasLeader() && msg.getLeader().getAction() == LeaderQuery.WHOISTHELEADER  && state.getElectionCtx().getTerm()>0 ) {
				WorkMessage buildNewNodeLeaderStatusResponseMessage = RaftMessageBuilder
						.buildTheLeaderIsMessage(state.getElectionCtx().getLeaderId(),state.getElectionCtx().getTerm());
				
				ChannelFuture cf = channel.write(buildNewNodeLeaderStatusResponseMessage);
				channel.flush();
				cf.awaitUninterruptibly();
				if (cf.isDone() && !cf.isSuccess()) {
					logger.info("Failed to write the message to the channel ");
				}
				
				state.getEmon().getOutBoundEdgesList().getNode(msg.getHeader().getNodeId()).setChannel(channel);

				// Sent the newly discovered node all the data on this node.

			}else if (msg.hasLeader() && msg.getLeader().getAction() == LeaderQuery.THELEADERIS /*&& msg.getLeader().getState()==LeaderState.LEADERALIVE*/) {
				state.getElectionCtx().setLeaderId(msg.getLeader().getLeaderId());
				state.getElectionCtx().setTerm(msg.getLeader().getTerm());
				//NodeChannelManager.amIPartOfNetwork = true;
				//logger.info("The leader is " + state.getElectionCtx().getLeaderId());
			} else if(msg.hasRaftMessage() && msg.getRaftMessage().getType() == ElectionMessageType.LEADER_HB_ACK) {
				state.getElectionCtx().getCurrentState().sendHearbeatAck(msg);
			} 
		} catch (Exception e) {
			// TODO add logging
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(state.getConf().getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			WorkMessage.Builder rb = WorkMessage.newBuilder(msg);
			rb.setErr(eb);
			channel.write(rb.build());
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
	protected void channelRead0(ChannelHandlerContext ctx, WorkMessage msg) throws Exception {
		handleMessage(msg, ctx.channel());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}