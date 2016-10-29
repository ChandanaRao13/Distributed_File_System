package gash.router.server.message.generator;

import gash.router.container.RoutingConf;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import pipe.common.Common.Header;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.Worktype;
import routing.Pipe.CommandMessage;
import routing.Pipe.FileTask;
import routing.Pipe.FileTask.FileTaskType;

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
	
	public WorkMessage generateReadRequestMessage(CommandMessage commandMessage, String clientID, int nodeId){
		Header.Builder hb = Header.newBuilder();

		hb.setNodeId(conf.getNodeId());
		hb.setDestination(nodeId);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setFiletask(commandMessage.getFiletask());
		
		wb.setSecret(1234);
		wb.setRequestId(clientID);
		wb.setWorktype(Worktype.READ_REQUEST);
		return wb.build();
	}
	
	public WorkMessage generateReadRequestResponseMessage(FileTask filetask, String line, int chunkId, int totalChunks, String requestId, int nodeId){
		Header.Builder hb = Header.newBuilder();

		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());
		hb.setDestination(nodeId);

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());

		wb.setSecret(1234);
		wb.setRequestId(requestId);
		wb.setWorktype(Worktype.READ_REQUEST_RESPONSE);

		FileTask.Builder ft = FileTask.newBuilder();
		ft.setChunkNo(chunkId);
		ft.setChunk(line);
		ft.setChunkCounts(totalChunks);
		ft.setFileTaskType(FileTaskType.READ);
		ft.setFilename(ft.getFilename());
		wb.setFiletask(ft.build());

		return wb.build();
	}
	
	
	public CommandMessage forwardChunkToClient(WorkMessage message){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		CommandMessage.Builder cb = CommandMessage.newBuilder();
		cb.setHeader(hb.build());
		cb.setFiletask(message.getFiletask());
		cb.setMessage("Fowarded chunk success : " + message.getFiletask().getChunkNo());

		return cb.build();
	}
}

