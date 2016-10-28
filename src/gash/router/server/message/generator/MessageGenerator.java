package gash.router.server.message.generator;

import gash.router.container.RoutingConf;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pipe.common.Common.Header;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.Worktype;
import routing.Pipe.CommandMessage;
import routing.Pipe.FileTask;

public class MessageGenerator {
	private static RoutingConf conf;

	protected static Logger logger = LoggerFactory.getLogger(MessageGenerator.class);
	protected static AtomicReference<MessageGenerator> instance = new AtomicReference<MessageGenerator>();

	public static MessageGenerator initGenerator() {
		instance.compareAndSet(null, new MessageGenerator());
		return instance.get();
	}

	private MessageGenerator() {

	}

	public static MessageGenerator getInstance(){
		MessageGenerator messageGenerator = instance.get();
		if(messageGenerator == null){
			logger.error(" Error while getting instance of MessageGenerator ");
		}
		return messageGenerator;
	}
	
	public static void setRoutingConf(RoutingConf routingConf) {
		MessageGenerator.conf = routingConf;
	}
	
	public CommandMessage generateClientResponseMsg(boolean isSuccess){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		if(isSuccess)
			rb.setMessage("File is stored in the database");
		else
			rb.setMessage("File is not stored in the database, please retry");

		return rb.build();

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
}

