package gash.router.server.workChainHandler;

import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.ServerState;
import gash.router.server.edges.EdgeInfo;
import gash.router.util.RaftMessageBuilder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import pipe.work.Work.WorkMessage;

public class NewNodeChainHandler implements IWorkChainHandler {
	private IWorkChainHandler nextChainHandler;
	protected ServerState state;
	protected static Logger logger = LoggerFactory.getLogger(NewNodeChainHandler.class);

	@Override
	public void setNextChain(IWorkChainHandler nextChain, ServerState state) {
		this.nextChainHandler = nextChain;
		this.state = state;
	}

	@Override
	public void handle(WorkMessage workMessage, Channel channel) {
		//System.out.println("Entered new Node chandler::"+workMessage.toString());
		if (workMessage.hasNewNodeMessage()) {
			logger.info("New Node. Node Id : " + workMessage.getHeader().getNodeId());

			// if new node is entering cluster for the first time send all other info 
			if(workMessage.getNewNodeMessage().getJoinCluster()==1){
				ArrayList<Integer> nodeIdList = new ArrayList<Integer>();
				ArrayList<String> hostList = new ArrayList<String>();
				ArrayList<Integer> portList = new ArrayList<Integer>();

				// getting all other node's info in the cluster and creating a list
				for(EdgeInfo ei : state.getEmon().getOutboundEdges().getEdgeListMap().values()){
					nodeIdList.add(ei.getRef());
					hostList.add(ei.getHost());
					portList.add(ei.getPort());
				}
				WorkMessage wm = RaftMessageBuilder.clusterNodeInfo(nodeIdList,hostList,portList);

				ChannelFuture cf = channel.write(wm);
				channel.flush();
				cf.awaitUninterruptibly();
				if (cf.isDone() && !cf.isSuccess()) {
					logger.info("Failed to write the message to the channel ");
				}
			}

			SocketAddress remoteAddress = channel.remoteAddress();
			InetSocketAddress addr = (InetSocketAddress) remoteAddress;

			state.getEmon().createInboundIfNew(workMessage.getHeader().getNodeId(), addr.getAddress().toString(), workMessage.getNewNodeMessage().getPortNo());
			state.getEmon().getInBoundEdgesList().getNode(workMessage.getHeader().getNodeId()).setChannel(channel);
			state.getEmon().getInBoundEdgesList().getNode(workMessage.getHeader().getNodeId()).setActive(true);

			state.getEmon().createOutboundIfNew(workMessage.getHeader().getNodeId(), addr.getAddress().toString(), workMessage.getNewNodeMessage().getPortNo());				
			state.getEmon().getOutBoundEdgesList().getNode(workMessage.getHeader().getNodeId()).setChannel(channel);
			state.getEmon().getOutBoundEdgesList().getNode(workMessage.getHeader().getNodeId()).setActive(true);

		}else if (workMessage.hasAllNodeInfo()) {				
			logger.info("New node received details about cluster : " + workMessage.getHeader().getNodeId());	
			for(int i=0;i<workMessage.getAllNodeInfo().getNodeIdList().size();i++){
				state.getEmon().createOutboundIfNew(workMessage.getAllNodeInfo().getNodeId(i), workMessage.getAllNodeInfo().getHostAddr(i), workMessage.getAllNodeInfo().getPortNo(i));
			}
			
		state.getEmon().connectToAll();
			
			for(EdgeInfo ei : state.getEmon().getOutboundEdges().getEdgeListMap().values()){
				
				if(ei.getRef()!= workMessage.getHeader().getNodeId()){
					WorkMessage wm = RaftMessageBuilder.addNewNodeInfo(0);
					while(true){
						if(ei.getChannel()!=null){
							ei.getChannel().writeAndFlush(wm);
							break;
						}
						try{
						Thread.sleep(1000);
						}catch(Exception e){
							
						}
					}
				}
			}
		} 
		else {
			this.nextChainHandler.handle(workMessage, channel);
		}
	}

}
