package gash.router.util;

import java.util.ArrayList;

import com.google.protobuf.ByteString;

import gash.router.container.GlobalRoutingConf;
import global.Global.File;
import global.Global.GlobalHeader;
import global.Global.GlobalMessage;
import global.Global.Request;
import global.Global.RequestType;
import global.Global.Response;
import pipe.common.Common.Header;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.Worktype;
import pipe.work.Work.WorkState;
import routing.Pipe.CommandMessage;
import routing.Pipe.FileTask;
import routing.Pipe.FileTask.FileTaskType;


public class GlobalMessageBuilder {
	private static GlobalRoutingConf conf;

	public static GlobalRoutingConf getGlobalRoutingConf() {
		System.out.println("entered RaftMsg Bilder constrctiu");
		return conf;
	}

	public static void setGlobalRoutingConf(GlobalRoutingConf conf) {
		GlobalMessageBuilder.conf = conf;

	}

	public static GlobalMessage buildPingMessage(){

		GlobalHeader.Builder header = GlobalHeader.newBuilder();
		header.setClusterId(conf.getClusterId());
		header.setTime(System.currentTimeMillis());
		header.setDestinationId(conf.getClusterId());

		GlobalMessage.Builder gm = GlobalMessage.newBuilder();
		gm.setGlobalHeader(header);
		gm.setPing(true);

		return gm.build();
	}
	
	public static GlobalMessage generateForwardGlobalPingMessage(CommandMessage msg){
		System.out.println("In command");
		GlobalHeader.Builder gh = GlobalHeader.newBuilder();
		gh.setClusterId(conf.getClusterId());
		gh.setDestinationId(conf.getClusterId());
		gh.setTime(System.currentTimeMillis());
		
		GlobalMessage.Builder gb = GlobalMessage.newBuilder(); 
		gb.setGlobalHeader(gh);
		gb.setPing(true);
		return gb.build();
	}
	
	public static GlobalMessage buildSimpleMessage(String message){

		GlobalHeader.Builder header = GlobalHeader.newBuilder();
		header.setClusterId(conf.getClusterId());
		header.setTime(System.currentTimeMillis());
		header.setDestinationId(conf.getClusterId());

		GlobalMessage.Builder gm = GlobalMessage.newBuilder();
		gm.setGlobalHeader(header);
		gm.setMessage(message);

		return gm.build();
	}
	
	/*
	public static GlobalMessage buildWhoIsTheLeaderMessage(){

		GlobalHeader.Builder header = GlobalHeader.newBuilder();
		header.setClusterId(conf.getClusterId());
		header.setTime(System.currentTimeMillis());
		header.setDestinationId(conf.getClusterId());

		WhoIsLeader.Builder whoIsTheLeaderMessage = WhoIsLeader.newBuilder();
		whoIsTheLeaderMessage.setRequesterIp(conf.getGlobalHost());
		whoIsTheLeaderMessage.setRequesterPort(conf.getGlobalPort());

		GlobalMessage.Builder gm = GlobalMessage.newBuilder();
		gm.setGlobalHeader(header);
		gm.setWhoIsClusterLeader(whoIsTheLeaderMessage);	

		return gm.build();
	}

	public static GlobalMessage buildTheLeaderIsMessage(String host,int port){

		GlobalHeader.Builder header = GlobalHeader.newBuilder();
		header.setClusterId(conf.getClusterId());
		header.setTime(System.currentTimeMillis());
		header.setDestinationId(conf.getClusterId());

		LeaderInfo.Builder theLeaderIs = LeaderInfo.newBuilder();
		theLeaderIs.setLeaderIp(host);
		theLeaderIs.setLeaderPort(port);

		GlobalMessage.Builder gm = GlobalMessage.newBuilder();
		gm.setGlobalHeader(header);
		gm.setClusterLeaderInfo(theLeaderIs);	

		return gm.build();
	}

	public static GlobalMessage buildLetsConnectMessage() {
		GlobalHeader.Builder header = GlobalHeader.newBuilder();
		header.setClusterId(conf.getClusterId());
		header.setTime(System.currentTimeMillis());
		
		LetsConnect.Builder letsConnectMessage = LetsConnect.newBuilder();
		letsConnectMessage.setPort(conf.getGlobalPort());
		
		GlobalMessage.Builder gm = GlobalMessage.newBuilder();
		gm.setGlobalHeader(header);
		gm.setLetsConnect(letsConnectMessage);
		
		return gm.build();
	}*/
	
	
   public static GlobalMessage generateGlobalRequestMessage(String filename,String clientId) {
	GlobalHeader.Builder hb = GlobalHeader.newBuilder();
	hb.setClusterId(conf.getClusterId());
	hb.setDestinationId(conf.getClusterId());
	hb.setTime(System.currentTimeMillis());

	Request.Builder rb = Request.newBuilder();
	rb.setRequestId(clientId);
	rb.setRequestType(RequestType.READ);
	rb.setFileName(filename);
	
	GlobalMessage.Builder gb = GlobalMessage.newBuilder();
	gb.setGlobalHeader(hb);
	gb.setRequest(rb);

	return gb.build();
}

public static GlobalMessage generateGlobalForwardRequestMessage(GlobalMessage message) {
	GlobalHeader.Builder hb = GlobalHeader.newBuilder(message.getGlobalHeader());
	hb.setClusterId(conf.getClusterId());
	hb.setTime(System.currentTimeMillis());

	Request.Builder rb = Request.newBuilder(message.getRequest());		
	GlobalMessage.Builder gb = GlobalMessage.newBuilder();
	gb.setGlobalHeader(hb);
	gb.setRequest(rb);

	return gb.build();
}

public static WorkMessage generateGlobalInternalWorkMessage(CommandMessage cmdMessage,String clientId) {
	Header.Builder hb = Header.newBuilder();
	hb.setTime(System.currentTimeMillis());
	hb.setNodeId(-1);

	FileTask.Builder ft = FileTask.newBuilder();
	ft.setFilename(cmdMessage.getFiletask().getFilename());
	ft.setFileTaskType(FileTaskType.READ);
	
	WorkMessage.Builder wb = WorkMessage.newBuilder();
	wb.setHeader(hb);
	wb.setRequestId(clientId);
	wb.setSecret(1234);
	wb.setFiletask(ft);
	wb.setWorktype(Worktype.GLOBAL_READ_REQUEST);

	return wb.build();
}

public WorkMessage generateGlobalReadRequestMessage(GlobalMessage globalMessage, String clientID, int nodeId, int chunkId){
	Header.Builder hb = Header.newBuilder();

	hb.setNodeId(-1);
	hb.setDestination(nodeId);
	hb.setTime(System.currentTimeMillis());

	FileTask.Builder fb = FileTask.newBuilder();
	fb.setFilename(globalMessage.getRequest().getFileName());
	fb.setChunkNo(chunkId);
	fb.setFileTaskType(FileTaskType.READ);
	fb.setSender(clientID);
	WorkMessage.Builder wb = WorkMessage.newBuilder();
	wb.setHeader(hb.build());
	wb.setFiletask(fb.build());

	wb.setSecret(1234);
	wb.setRequestId(clientID);
	wb.setWorktype(Worktype.READ_REQUEST);
	return wb.build();
}

public static GlobalMessage generateGlobalReadResponseMessage(GlobalMessage message, int chunkId, ByteString data, int totalChunks){
	GlobalHeader.Builder gh = GlobalHeader.newBuilder(message.getGlobalHeader());
	gh.setTime(System.currentTimeMillis());
	
	File.Builder fb = File.newBuilder(); 
	fb.setChunkId(chunkId);
	fb.setData(data);
	fb.setFilename(message.getRequest().getFileName());
	fb.setTotalNoOfChunks(totalChunks);
		
	
	Response.Builder rb = Response.newBuilder();
	rb.setRequestId(message.getRequest().getRequestId());
	rb.setRequestType(RequestType.READ);
	rb.setFile(fb.build());

	GlobalMessage.Builder gb = GlobalMessage.newBuilder();
	gb.setGlobalHeader(gh.build());
	gb.setResponse(rb);
	return gb.build();
}

public static CommandMessage forwardChunkToClient(GlobalMessage message){
	Header.Builder hb = Header.newBuilder();
	hb.setNodeId(-1);
	hb.setTime(System.currentTimeMillis());

	FileTask.Builder ft = FileTask.newBuilder();
	File file = message.getResponse().getFile();
	ft.setChunkNo(file.getChunkId());
	ft.setChunk(file.getData());
	ft.setChunkCounts(file.getTotalNoOfChunks());
	ft.setFilename(file.getFilename()); 
	ft.setFileTaskType(FileTaskType.READ);
	
	CommandMessage.Builder cb = CommandMessage.newBuilder();
	cb.setHeader(hb.build());
	cb.setFiletask(ft);
	cb.setMessage("Fowarded chunk success : " + file.getChunkId());

	return cb.build();
}

public static GlobalMessage generateGlobalForwardReadResponseMessage(GlobalMessage message){
	GlobalHeader.Builder gh = GlobalHeader.newBuilder(message.getGlobalHeader());
	gh.setClusterId(conf.getClusterId());
	gh.setTime(System.currentTimeMillis());
	
	
	Response.Builder rb = Response.newBuilder(message.getResponse());		
	GlobalMessage.Builder gb = GlobalMessage.newBuilder(message);
	gb.setGlobalHeader(gh);
	gb.setResponse(rb);
	
	return gb.build();
} 

public static GlobalMessage generateGlobalUpdateRequestMessage(CommandMessage message, String clientId) {
	GlobalHeader.Builder hb = GlobalHeader.newBuilder();
	hb.setClusterId(conf.getClusterId());
	hb.setDestinationId(conf.getClusterId());
	hb.setTime(System.currentTimeMillis());

	File.Builder fb = File.newBuilder();
	fb.setChunkId(message.getFiletask().getChunkNo());
	fb.setData(message.getFiletask().getChunk());
	fb.setFilename(message.getFiletask().getFilename());
	fb.setTotalNoOfChunks(message.getFiletask().getChunkCounts());
	
	Request.Builder rb = Request.newBuilder();
	rb.setRequestId(clientId);
	rb.setRequestType(RequestType.UPDATE);
	rb.setFile(fb.build());
	
	GlobalMessage.Builder gb = GlobalMessage.newBuilder();
	gb.setGlobalHeader(hb);
	gb.setRequest(rb);
	return gb.build();
}

public static GlobalMessage generateGlobalWriteRequestMessage(CommandMessage message, String clientId) {
	GlobalHeader.Builder hb = GlobalHeader.newBuilder();
	hb.setClusterId(conf.getClusterId());
	hb.setDestinationId(conf.getClusterId());
	hb.setTime(System.currentTimeMillis());

	File.Builder fb = File.newBuilder();
	fb.setChunkId(message.getFiletask().getChunkNo());
	fb.setData(message.getFiletask().getChunk());
	fb.setFilename(message.getFiletask().getFilename());
	fb.setTotalNoOfChunks(message.getFiletask().getChunkCounts());
	
	Request.Builder rb = Request.newBuilder();
	rb.setRequestId(clientId);
	rb.setRequestType(RequestType.WRITE);
	rb.setFile(fb.build());
	
	GlobalMessage.Builder gb = GlobalMessage.newBuilder();
	gb.setGlobalHeader(hb);
	gb.setRequest(rb);
	return gb.build();
}

public static GlobalMessage generateGlobalDeleteRequestMessage(String filename,String clientId) {
	GlobalHeader.Builder hb = GlobalHeader.newBuilder();
	hb.setClusterId(conf.getClusterId());
	hb.setDestinationId(conf.getClusterId());
	hb.setTime(System.currentTimeMillis());

	Request.Builder rb = Request.newBuilder();
	rb.setRequestId(clientId);
	rb.setRequestType(RequestType.DELETE);
	rb.setFileName(filename);
	
	GlobalMessage.Builder gb = GlobalMessage.newBuilder();
	gb.setGlobalHeader(hb);
	gb.setRequest(rb);

	return gb.build();
}

}
