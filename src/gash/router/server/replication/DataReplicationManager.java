package gash.router.server.replication;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.server.edges.EdgeMonitor;
import gash.router.server.message.generator.MessageGenerator;
import gash.router.server.queue.management.QueueManager;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

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
				WorkMessage workMessage = MessageGenerator.getInstance().generateReplicationRequestMsg(message, nodeId);
				QueueManager.getInstance().enqueueOutboundWorkWrite(workMessage, channel);
			}

		}
	}
	
	public void broadcastDeletion(CommandMessage message) {
		ConcurrentHashMap<Integer, Channel> node2ChannelMap = EdgeMonitor.node2ChannelMap;
		if (node2ChannelMap != null && !node2ChannelMap.isEmpty()) {

			Set<Integer> nodeIds = node2ChannelMap.keySet();
			for (Integer nodeId : nodeIds) {
				Channel channel = node2ChannelMap.get(nodeId);
				WorkMessage workMessage = MessageGenerator.getInstance().generateDeletionRequestMsg(message, nodeId);
				QueueManager.getInstance().enqueueOutboundWorkWrite(workMessage, channel);
			}

		}
	}
}
