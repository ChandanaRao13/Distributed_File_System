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
//
//C
//changes
package gash.router.cluster;

//change
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.GlobalRoutingConf.GlobalRoutingEntry;
import gash.router.cluster.GlobalServerState;
import gash.router.server.WorkInit;
import gash.router.server.edges.EdgeInfo;
import gash.router.server.election.RaftElectionContext;
import gash.router.server.message.generator.MessageGenerator;
import gash.router.server.queue.management.InternalChannelNode;
import gash.router.server.queue.management.LoadQueueManager;
import gash.router.server.queue.management.NodeLoad;
import gash.router.util.GlobalMessageBuilder;
import gash.router.util.RaftMessageBuilder;
import global.Global.GlobalMessage;
import io.netty.channel.ChannelFuture;
import pipe.common.Common.Header;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;

public class GlobalEdgeMonitor implements GlobalEdgeListener, Runnable {
	protected static Logger logger = LoggerFactory.getLogger("Global edge monitor");
	private GlobalEdgeList outboundEdges;
	private GlobalEdgeList inboundEdges;
	private GlobalServerState state;
	private boolean forever = true;
	private long dt = 2000;
	


	public GlobalEdgeMonitor(GlobalServerState state) {
		if (state == null)
			throw new RuntimeException("state is null");
		this.outboundEdges = new GlobalEdgeList();
		this.inboundEdges = new GlobalEdgeList();
		this.state = state;
		this.state.setEmon(this);
		if (state.getConf().getRouting() != null) {
			for (GlobalRoutingEntry e : state.getConf().getRouting()) {
				System.out.println("Cluster id " + e.getClusterId() + " Host " + e.getHost());
				outboundEdges.addNode(e.getClusterId(), e.getHost(), e.getPort());
			}
		}
		GlobalMessageBuilder.setGlobalRoutingConf(state.getConf());
	}

	public void createInboundIfNew(int ref, String host, int port) {
		inboundEdges.createIfNew(ref, host, port);
	}

	public void createOutboundIfNew(int ref, String host, int port) {
		outboundEdges.createIfNew(ref, host, port);
	}

	public GlobalEdgeList getOutBoundEdgesList(){
		return outboundEdges;
	}

	public GlobalEdgeList getInBoundEdgesList(){
		return inboundEdges;
	}

	public void shutdown() {
		forever = false;
	}

	@Override
	public void run() {
		while (forever) {
			try {

				for (GlobalEdgeInfo ei : this.outboundEdges.map.values()) {
					if (ei.isActive() && ei.getChannel() != null) {
						if(ei.getChannel()!=null){
							ei.getChannel().writeAndFlush(GlobalMessageBuilder.buildPingMessage());
						}

					} else if(ei.getChannel() == null){
						Channel channel = connectToChannel(ei.getHost(), ei.getPort());
						ei.setChannel(channel);
						ei.setActive(true);
						if (channel == null) {
							logger.info("trying to connect to cluster  " + ei.getRef());
						}
					}
				}
				Thread.sleep(dt);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
			}
		}
	}

	public Channel connectToChannel(String host, int port) {
		System.out.println("Host::"+host+"\n Port::"+port);
		Bootstrap b = new Bootstrap();
		Channel ch = null;
		try {
			b.group(new NioEventLoopGroup());
			b.channel(NioSocketChannel.class);
			b.handler(new GlobalInit(state, false));

			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);
			ch = b.connect(host, port).syncUninterruptibly().channel();
			Thread.sleep(dt);
		} catch (Exception e) {
			//e.printStackTrace();
			//logger.info("trying to connect to node"+host);

		}
		return ch;

	}
	public GlobalEdgeList getOutboundEdges() {
		return outboundEdges;
	}

	public void setOutboundEdges(GlobalEdgeList outboundEdgesInfo) {
		outboundEdges = outboundEdgesInfo;
	}

	public GlobalEdgeList getInboundEdges() {
		return inboundEdges;
	}

	public void setInboundEdges(GlobalEdgeList inboundEdgesInfo) {
		inboundEdges = inboundEdgesInfo;
	}

	@Override
	public synchronized void onAdd(GlobalEdgeInfo ei) {
		// TODO check connection
	}

	@Override
	public synchronized void onRemove(GlobalEdgeInfo ei) {
		// TODO ?
	}

	public void broadcastToClusterFriends(GlobalMessage msg){
		for(GlobalEdgeInfo ei : this.outboundEdges.map.values()){	
			if(ei.isActive() && ei.getChannel()!=null){
				ei.getChannel().writeAndFlush(msg);
			}
		}
	}
}


