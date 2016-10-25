package gash.router.server.replication;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




import gash.router.server.edges.EdgeMonitor;
import gash.router.server.queue.management.QueueManager;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import pipe.common.Common.Header;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.Worktype;
import routing.Pipe.CommandMessage;
import routing.Pipe.FileTask;

public class DataReplicationManager {

	protected static Logger logger = LoggerFactory.getLogger("DataReplicationManager");

	protected static AtomicReference<DataReplicationManager> instance = new AtomicReference<DataReplicationManager>();

	public static DataReplicationManager initDataReplicationManager() {
		instance.compareAndSet(null, new DataReplicationManager());
		System.out.println(" --- Initializing Data Replication Manager --- ");
		return instance.get();
	}

	public static DataReplicationManager getInstance() throws Exception {
		if (instance != null && instance.get() != null) {
			return instance.get();
		}
		throw new Exception(" Data Replication Manager not started ");
	}


	public void broadcastReplication(CommandMessage message) {
		ConcurrentHashMap<Integer, Channel> node2ChannelMap = EdgeMonitor.node2ChannelMap;
		if (node2ChannelMap != null && !node2ChannelMap.isEmpty()) {

			Set<Integer> nodeIds = node2ChannelMap.keySet();
			for (Integer nodeId : nodeIds) {
				Channel channel = node2ChannelMap.get(nodeId);
				WorkMessage workMessage = generateReplicationRequestMsg(message, nodeId);
				QueueManager.getInstance().enqueueOutboundWorkWrite(workMessage, channel);
			}

		}
	}

	public WorkMessage generateReplicationRequestMsg(CommandMessage message, Integer nodeId){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(nodeId);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setSecret(1234);
		
		FileTask.Builder tb = FileTask.newBuilder(message.getFiletask());
		tb.setChunk(message.getFiletask().getChunk());
		tb.setChunkNo(message.getFiletask().getChunkNo());

		wb.setFiletask(tb);

		wb.setWorktype(Worktype.REPLICATE_REQUEST);
		return wb.build();
	}

	/*
	// TODO convert this to Future and Callable
	private class Replication implements Runnable {
		private CommandMessage commandMessage;
		private Channel nodeChannel;
		private int nodeId;

		public Replication(CommandMessage commandMessage, Channel nodeChannel, Integer nodeId) {
			// Command Message here contains the chunk ID and the chunk nos. and
			// the chunk byte, in its Task field
			this.commandMessage = commandMessage;
			this.nodeChannel = nodeChannel;
			this.nodeId = nodeId;
		}

		@Override
		public void run() {
			if (this.nodeChannel.isOpen() && this.nodeChannel.isActive()) {
				// this.nodeChannel.writeAndFlush(replicationInfo);

				// Generate the work message to send to the slave nodes
				WorkMessage workMsg = MessageGeneratorUtil.getInstance().generateReplicationReqMsg(commandMessage,
						nodeId);

				// Push this message to the outbound work queue
				try {
					logger.info("Sending message for replication ");
					QueueManager.getInstance().enqueueOutboundWork(workMsg, nodeChannel);
				} catch (Exception e) {
					logger.error(e.getMessage());
					e.printStackTrace();
				}
			} else {
				logger.error("The nodeChannel to " + nodeChannel.localAddress() + " is not active");
			}
		}

	}*/
}
