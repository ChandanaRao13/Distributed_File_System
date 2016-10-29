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
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf.RoutingEntry;
import gash.router.discovery.NodeDiscoveryManager;
//import gash.router.server.NodeChannelManager;
import gash.router.server.ServerState;
import gash.router.server.WorkInit;
import gash.router.server.election.RaftElectionContext;
import gash.router.server.queue.management.InternalChannelNode;
import gash.router.server.queue.management.LoadQueueManager;
import gash.router.server.queue.management.NodeLoad;
import gash.router.util.RaftMessageBuilder;
import io.netty.channel.ChannelFuture;
import pipe.common.Common.Header;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;

public class EdgeMonitor implements EdgeListener, Runnable {
	protected static Logger logger = LoggerFactory.getLogger("edge monitor");
	public static ConcurrentHashMap<Integer, Channel> node2ChannelMap = new ConcurrentHashMap<Integer, Channel>();
	public static HashSet<Integer> loadNodeSet = new HashSet<Integer>();
	public static ConcurrentHashMap<String, InternalChannelNode> clientChannelMap = new ConcurrentHashMap<String, InternalChannelNode>();
	private EdgeList outboundEdges;
	private EdgeList inboundEdges;
	private long dt = 2000;
	private ServerState state;
	private RaftElectionContext electionCtx;
	private boolean forever = true;
	private ArrayList<InetAddress> liveIps;

	public EdgeMonitor(ServerState state) {
		if (state == null)
			throw new RuntimeException("state is null");
		this.electionCtx = state.getElectionCtx();
		this.electionCtx.setEmon(this);
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

		RaftMessageBuilder.setRoutingConf(state.getConf());
		
		if (outboundEdges.isEmpty() && inboundEdges.isEmpty()) {
			//NodeChannelManager.amIPartOfNetwork = false;
			System.out.println("No routing entries..possibly a new node");
			try {
				liveIps = NodeDiscoveryManager.checkHosts();
				Channel discoveryChannel = null;

				for (InetAddress oneIp : liveIps) {
					// Ignore own IP address
					if (!oneIp.getHostAddress().equals(InetAddress.getLocalHost().getHostAddress())) {
						System.out.println("Potential Server Node found of Network.. Trying to connect to:  "
								+ oneIp.getHostAddress());
						try {

							Channel newNodeChannel = connectToChannel(oneIp.getHostAddress(), 5100, this.state);

							if (newNodeChannel.isOpen() && newNodeChannel != null) {

								System.out.println("Channel connected to: " + oneIp.getHostAddress());
								WorkMessage wm = createNewNode();

								ChannelFuture cf = newNodeChannel.write(wm);
								newNodeChannel.flush();
								cf.awaitUninterruptibly();
								if (cf.isDone() && !cf.isSuccess()) {
									logger.info("Failed to write the message to the channel ");
								}
								if (discoveryChannel == null) {
									logger.info("Setting discovery channel for : "+oneIp.getHostAddress());
									discoveryChannel = newNodeChannel;
								}
							}
						} catch (Exception e) {
							System.out.println("Unable to connect to the potential client: " + oneIp.getHostAddress());
						}
					}

				}

				if (discoveryChannel != null) {
					// Send a discovery message to the first node that the new
					// node finds
					WorkMessage discoveryMessage = RaftMessageBuilder.buildWhoIsTheLeaderMessage();
					logger.info("I'm a new node. I shall now try to get the data from the cluster automatically");
					ChannelFuture cf = discoveryChannel.write(discoveryMessage);
					discoveryChannel.flush();
					cf.awaitUninterruptibly();
					if (cf.isDone() && !cf.isSuccess()) {
						logger.info("Failed to write the message to the channel ");
					}
					logger.debug("Writing to a node to get the leader status");
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		// cannot go below 2 sec
		if (state.getConf().getHeartbeatDt() > this.dt)
			this.dt = state.getConf().getHeartbeatDt();
		
		NodeMonitor nodeMonitor = new NodeMonitor();
		Thread thread = new Thread(nodeMonitor);
		thread.start();
	}

	private WorkMessage createNewNode() {

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(state.getConf().getNodeId());
		hb.setDestination(-1);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setNewNode(true);
		wb.setSecret(1234);
		//wb.setStateOfLeader(StateOfLeader.LEADERUNKNOWN);
		return wb.build();
	}
	
	public void createInboundIfNew(int ref, String host, int port) {
		inboundEdges.createIfNew(ref, host, port);
	}

	public void createOutboundIfNew(int ref, String host, int port) {
		outboundEdges.createIfNew(ref, host, port);
	}

	public EdgeList getOutBoundEdgesList(){
		return outboundEdges;
	}

	public EdgeList getInBoundEdgesList(){
		return inboundEdges;
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
				for (EdgeInfo ei : this.outboundEdges.map.values()) {
					if (ei.isActive() && ei.getChannel() != null) {
						if(electionCtx.getTerm()== 0){
							WorkMessage whoIsTheLeaderMsg = RaftMessageBuilder.buildWhoIsTheLeaderMessage();
							broadcast(whoIsTheLeaderMsg);
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

	public WorkMessage createRoutingMsg() {
		ArrayList<String> ipList = new ArrayList<String>();
		ArrayList<String> idList = new ArrayList<String>();

		for (RoutingEntry destIp : state.getConf().getRouting()) {
			ipList.add(destIp.getHost());
			idList.add(destIp.getId() + "");
		}
		//rb.addAllNodeId(idList);
		//rb.addAllNodeIp(ipList);

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(state.getConf().getNodeId());
		hb.setDestination(-1);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setSecret(1234);
		wb.setFlagRouting(true);
		//wb.setRoutingEntries(rb);
		// TODO Is the leader really alive?
		//wb.setStateOfLeader(StateOfLeader.LEADERKNOWN);
		return wb.build();
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
			Thread.sleep(dt);
		} catch (Exception e) {
				//e.printStackTrace();
				//logger.info("trying to connect to node"+host);
			
		}
		return ch;

	}

	private Channel connectToChannel(String host, int port, ServerState state) {
		Bootstrap b = new Bootstrap();
		NioEventLoopGroup nioEventLoopGroup = new NioEventLoopGroup();
		WorkInit workInit = new WorkInit(state, false);

		try {
			b.group(nioEventLoopGroup).channel(NioSocketChannel.class).handler(workInit);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);
			// Make the connection attempt.
		} catch (Exception e) {
			logger.error("Could not connect to the host " + host);
			return null;
		}
		return b.connect(host, port).syncUninterruptibly().channel();

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

	public void broadcast(WorkMessage msg){
		for(EdgeInfo ei:this.outboundEdges.map.values()){
			if (ei.isActive() && ei.getChannel() != null) 
				ei.getChannel().writeAndFlush(msg);

		}
		for(EdgeInfo ei:this.inboundEdges.map.values()){
			if (ei.isActive() && ei.getChannel() != null) 
				ei.getChannel().writeAndFlush(msg);

		}
	}
	@Override
	public synchronized void onAdd(EdgeInfo ei) {
		// TODO check connection
	}

	@Override
	public synchronized void onRemove(EdgeInfo ei) {
		// TODO ?
	}
	
	public static String clientInfoMap(InternalChannelNode commandMessageNode) {
		UUID uuid = UUID.randomUUID();
		String clientUidString = uuid.toString();
		clientChannelMap.put(clientUidString, commandMessageNode);
		return clientUidString;
	}
	
	public static synchronized InternalChannelNode getClientChannelFromMap(String clientId) {

		if (clientChannelMap.containsKey(clientId) && clientChannelMap.get(clientId) != null) {
			return clientChannelMap.get(clientId);
		} else {
			logger.info("Unable to find the channel for client id : " + clientId);
			return null;
		}
	}
	// To continuously check addition and removal of nodes to the current node
	private class NodeMonitor implements Runnable {
		private boolean forever = true;

		@Override
		public void run() {
			try {
				while (forever) {
					addToNode2ChannelMap(getInboundEdges());
					addToNode2ChannelMap(getOutboundEdges());				}
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
								
							    addNodeLoadToQueue(nodeId);
							}
					}
				}
			} catch (Exception exception) {
				logger.error("An Error has occured ", exception);
			}
		}
		
		
		private synchronized void addNodeLoadToQueue(Integer nodeId){
			if(nodeId != null && !loadNodeSet.contains(nodeId.intValue())){
				System.out.println("Adding loadNodeset: " + nodeId);
				loadNodeSet.add(nodeId.intValue());
				NodeLoad node = new NodeLoad(nodeId.intValue(), 0);
				LoadQueueManager.getInstance().insertNodeLoadInfo(node);
			} 
		}

	}
}


