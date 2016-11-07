package gash.router.server.message.generator;

import gash.router.container.RoutingConf;

import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import pipe.common.Common.Header;
import pipe.work.Work.READ_STEAL;
import pipe.work.Work.Steal;
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
	
	public CommandMessage generateClientResponseMsg(String msg){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		CommandMessage.Builder rb = CommandMessage.newBuilder();
		rb.setHeader(hb);
		rb.setMessage(msg);

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
	
	public WorkMessage generateUpdateReplicationRequestMsg(CommandMessage message, Integer nodeId){
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

		wb.setWorktype(Worktype.UPDATE_REPLICATE_REQUEST);
		return wb.build();
	}
	
	public WorkMessage generateDeletionRequestMsg(CommandMessage message, Integer nodeId){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(nodeId);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setSecret(1234);
		
		FileTask.Builder tb = FileTask.newBuilder(message.getFiletask());
		wb.setFiletask(tb);

		wb.setWorktype(Worktype.DELETE_REQUEST);
		return wb.build();
	}
	
	public WorkMessage generateUpdateDeletionRequestMsg(CommandMessage message, Integer nodeId){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(nodeId);
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setSecret(1234);
		
		FileTask.Builder tb = FileTask.newBuilder(message.getFiletask());
		wb.setFiletask(tb);

		wb.setWorktype(Worktype.UPDATE_DELETE_REQUEST);
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

	public WorkMessage generateReadRequestMessage(CommandMessage commandMessage, String clientID, int nodeId, int chunkId){
		Header.Builder hb = Header.newBuilder();

		hb.setNodeId(conf.getNodeId());
		hb.setDestination(nodeId);
		hb.setTime(System.currentTimeMillis());

		FileTask.Builder fb = FileTask.newBuilder();
		fb.setFilename(commandMessage.getFiletask().getFilename());
		fb.setChunkNo(chunkId);
		fb.setFileTaskType(FileTaskType.READ);
		fb.setSender(commandMessage.getFiletask().getSender());
		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setFiletask(fb.build());
		
		wb.setSecret(1234);
		wb.setRequestId(clientID);
		wb.setWorktype(Worktype.READ_REQUEST);
		return wb.build();
	}

	public WorkMessage generateReadRequestResponseMessage(FileTask filetask, ByteString line, int chunkId, int totalChunks, String requestId, int nodeId, String filename){
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
		ft.setFilename(filename);
		ft.setChunkNo(chunkId);
		ft.setChunk(line);
		ft.setChunkCounts(totalChunks);
		ft.setFileTaskType(FileTaskType.READ);
		ft.setSender(filetask.getSender());
		//ft.setFilename(ft.getFilename());
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
	
	public WorkMessage generateReplicationAcknowledgementMessage(WorkMessage message){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setWorktype(Worktype.REPLICATE_RESPONSE);
		wb.setSecret(1234);
		
		FileTask.Builder ft = FileTask.newBuilder();
		ft.setFilename(message.getFiletask().getFilename());
		ft.setFileTaskType(FileTaskType.WRITE);
		ft.setSender(message.getFiletask().getSender());
		wb.setFiletask(ft.build());
		return wb.build();
	}

	public WorkMessage generateDeletionAcknowledgementMessage(WorkMessage message){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setWorktype(Worktype.DELETE_RESPONSE);
		wb.setSecret(1234);
		
		FileTask.Builder ft = FileTask.newBuilder();
		ft.setFilename(message.getFiletask().getFilename());
		ft.setFileTaskType(FileTaskType.WRITE);
		ft.setSender(message.getFiletask().getSender());
		wb.setFiletask(ft.build());
		return wb.build();
	}
	
	public WorkMessage generateUpdateDeletionAcknowledgementMessage(WorkMessage message){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setWorktype(Worktype.UPDATE_DELETE_RESPONSE);
		wb.setSecret(1234);
		
		FileTask.Builder ft = FileTask.newBuilder();
		ft.setFilename(message.getFiletask().getFilename());
		ft.setFileTaskType(FileTaskType.WRITE);
		ft.setSender(message.getFiletask().getSender());
		wb.setFiletask(ft.build());
		return wb.build();
	}

	public WorkMessage generateUpdateReplicationAcknowledgementMessage(WorkMessage message){
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setWorktype(Worktype.UPDATE_REPLICATE_RESPONSE);
		wb.setSecret(1234);
		
		FileTask.Builder ft = FileTask.newBuilder();
		ft.setFilename(message.getFiletask().getFilename());
		ft.setFileTaskType(FileTaskType.WRITE);
		ft.setSender(message.getFiletask().getSender());
		wb.setFiletask(ft.build());
		return wb.build();
	}
	
	public WorkMessage generateWorkReadStealMessage() {
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(conf.getNodeId());
		hb.setTime(System.currentTimeMillis());

		Steal.Builder sb = Steal.newBuilder();
		sb.setReadSteal(READ_STEAL.READ_STEAL_REQUEST);

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb.build());
		wb.setSteal(sb.build());
		wb.setSecret(1234);
		return wb.build();
	}

	public WorkMessage generateWorkReadStealResponseMsg(WorkMessage workMessage) {
		WorkMessage.Builder wb = WorkMessage.newBuilder(workMessage);
		Steal.Builder sb = Steal.newBuilder();
		sb.setReadSteal(READ_STEAL.READ_STEAL_RESPONSE);
		wb.setSteal(sb.build());
		System.out.println("SEnding chunkId: " + workMessage.getFiletask().getChunkNo());
/*		Header.Builder hb = Header.newBuilder();
		//System.out.println("Setting main server nodeId as header for steal read id: " + workMessage.getHeader().getDestination());
		try {
			System.out.println("Setting main server nodeId as header for steal read id: " + workMessage.getHeader().getNodeId());
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		hb.setNodeId(workMessage.getHeader().getNodeId());
		hb.setTime(System.currentTimeMillis());

		Steal.Builder sb = Steal.newBuilder();
		sb.setReadSteal(READ_STEAL.READ_STEAL_RESPONSE);

		//System.out.println("SEnding filename: " + workMessage.getFiletask().getFilename());
		System.out.println("SEnding chunkId: " + workMessage.getFiletask().getChunkNo());
		//System.out.println("SEnding chunkCount: " + workMessage.getFiletask().getChunkCounts());
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		FileTask.Builder ft  = FileTask.newBuilder();
		ft.setFilename(workMessage.getFiletask().getFilename());
		ft.setChunkNo(workMessage.getFiletask().getChunkNo());
		
		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setFiletask(ft.build());
		wb.setHeader(hb.build());
		wb.setSteal(sb.build());
		wb.setSecret(1234);*/
		return wb.build();
	}
}

