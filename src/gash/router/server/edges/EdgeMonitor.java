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
package gash.router.server.edges;

//change
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf.RoutingEntry;
import gash.router.server.ServerState;
import gash.router.server.WorkInit;
import pipe.common.Common.Header;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;

public class EdgeMonitor implements EdgeListener, Runnable {
	protected static Logger logger = LoggerFactory.getLogger("edge monitor");
	public static ConcurrentHashMap<Integer, Channel> node2ChannelMap = new ConcurrentHashMap<Integer, Channel>();
	private EdgeList outboundEdges;
	private EdgeList inboundEdges;
	private long dt = 2000;
	private ServerState state;
	private boolean forever = true;

	public EdgeMonitor(ServerState state) {
		if (state == null)
			throw new RuntimeException("state is null");

		this.outboundEdges = new EdgeList();
		this.inboundEdges = new EdgeList();
		this.state = state;
		this.state.setEmon(this);

		if (state.getConf().getRouting() != null) {
			for (RoutingEntry e : state.getConf().getRouting()) {
				System.out.println("Node id " + e.getId() + " Host " + e.getHost());
				outboundEdges.addNode(e.getId(), e.getHost(), e.getPort());
			}
		}

		// cannot go below 2 sec
		if (state.getConf().getHeartbeatDt() > this.dt)
			this.dt = state.getConf().getHeartbeatDt();
		
		NodeMonitor nodeMonitor = new NodeMonitor();
		Thread thread = new Thread(nodeMonitor);
		thread.start();
	}


	
	public void createInboundIfNew(int ref, String host, int port) {
		inboundEdges.createIfNew(ref, host, port);
	}

	private WorkMessage createHB(EdgeInfo ei) {
		WorkState.Builder sb = WorkState.newBuilder();
		sb.setEnqueued(-1);
		sb.setProcessed(-1);

		Heartbeat.Builder bb = Heartbeat.newBuilder();
		bb.setState(sb);

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(state.getConf().getNodeId());
		hb.setDestination(-1);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setBeat(bb);
		wb.setSecret(1234);

		return wb.build();
	}

	public void shutdown() {
		forever = false;
	}

	@Override
	public void run() {
		while (forever) {
			try {
				for (EdgeInfo ei : this.outboundEdges.map.values()) {
					if (ei.isActive() && ei.getChannel() != null) {
						WorkMessage wm = createHB(ei);
						ei.getChannel().writeAndFlush(wm);
					} else if(ei.getChannel() == null){
						Channel channel = connectToChannel(ei.getHost(), ei.getPort());
						ei.setChannel(channel);
						ei.setActive(true);
						if (channel == null) {
							logger.info("trying to connect to node " + ei.getRef());
						}
					}
				}

				Thread.sleep(dt);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private Channel connectToChannel(String host, int port) {
		Bootstrap b = new Bootstrap();
		Channel ch = null;
		try {
			b.group(new NioEventLoopGroup());
			b.channel(NioSocketChannel.class);
			b.handler(new WorkInit(state, false));

			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);
			ch = b.connect(host, port).syncUninterruptibly().channel();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ch;

	}

	public EdgeList getOutboundEdges() {
		return outboundEdges;
	}

	public void setOutboundEdges(EdgeList outboundEdgesInfo) {
		outboundEdges = outboundEdgesInfo;
	}

	public EdgeList getInboundEdges() {
		return inboundEdges;
	}

	public void setInboundEdges(EdgeList inboundEdgesInfo) {
		inboundEdges = inboundEdgesInfo;
	}

	
	@Override
	public synchronized void onAdd(EdgeInfo ei) {
		// TODO check connection
	}

	@Override
	public synchronized void onRemove(EdgeInfo ei) {
		// TODO ?
	}
	
	// To continuously check addition and removal of nodes to the current node
	private class NodeMonitor implements Runnable {
		private boolean forever = true;

		@Override
		public void run() {
			try {
				while (forever) {
					addToNode2ChannelMap(getInboundEdges());
					addToNode2ChannelMap(getOutboundEdges());
				
				//	System.out.println("node2Channel Map : " + node2ChannelMap.toString());
					// Make it efficient
					// Thread.sleep(NodeChannelManager.delay);
				}

			} catch (Exception e) {
				logger.error("An error has occured ", e);
			}
		}

		private void addToNode2ChannelMap(EdgeList edges) {
			try {

				if (edges != null) {
					ConcurrentHashMap<Integer, EdgeInfo> edgeListMapper = edges.getEdgeListMap();
					if (edgeListMapper != null && !edgeListMapper.isEmpty()) {
						Set<Integer> nodeIdSet = edgeListMapper.keySet();
						if (nodeIdSet != null)
							for (Integer nodeId : nodeIdSet) {
								if (nodeId != null && !node2ChannelMap.containsKey(nodeId)
										&& edgeListMapper.containsKey(nodeId)
										&& edgeListMapper.get(nodeId).getChannel() != null) {
									logger.info("Added node " + nodeId + " " + edgeListMapper.get(nodeId).getHost()
											+ " to channel map. ");
									if(edgeListMapper.get(nodeId).getChannel() != null ) {
										node2ChannelMap.put(nodeId, edgeListMapper.get(nodeId).getChannel());
									}
									
								}
							}
					}
				}
			} catch (Exception exception) {
				logger.error("An Error has occured ", exception);
			}
		}

	}
}