package gash.router.server.workChainHandler;

import java.io.IOException;

import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import gash.router.database.DatabaseHandler;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.manage.exceptions.EmptyConnectionPoolException;
import gash.router.server.manage.exceptions.FileChunkNotFoundException;
import gash.router.server.manage.exceptions.FileNotFoundException;
import gash.router.server.message.generator.MessageGenerator;
import gash.router.server.queue.management.InternalChannelNode;
import gash.router.server.queue.management.QueueManager;
import io.netty.channel.Channel;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkMessage.Worktype;
import routing.Pipe.CommandMessage;
import routing.Pipe.FileTask;

public class TaskHandler implements IWorkChainHandler{
	private IWorkChainHandler nextChainHandler;
	protected static Logger logger = LoggerFactory.getLogger("work");
	@Override
	public void setNextChain(IWorkChainHandler nextChain) {
		// TODO Auto-generated method stub
		this.nextChainHandler = nextChain;
	}

	@Override
	public void handle(WorkMessage workMessage, Channel channel) {
		// TODO Auto-generated method stub
		if(workMessage.hasFiletask()) {
			logger.info("Recieved replicate work message");
			if(workMessage.getWorktype() == Worktype.READ_REQUEST){
				logger.info("Received message to read a file");
				FileTask ft = workMessage.getFiletask();
				try {
					int chunkCount = DatabaseHandler.getFilesChunkCount(ft.getFilename());
					for (int i = 1; i <= chunkCount; i++) {
						
						try {
							ByteString content = DatabaseHandler.getFileChunkContentWithChunkId(ft.getFilename(), i);
							System.out.println("Content recieved :" + content);
							WorkMessage msg = MessageGenerator.getInstance().generateReadRequestResponseMessage(ft, content, i, 
									chunkCount, workMessage.getRequestId(), workMessage.getHeader().getNodeId(), ft.getFilename());
							QueueManager.getInstance().enqueueOutboundRead(msg, channel);
						} catch (FileChunkNotFoundException e) {
							e.printStackTrace();
						}
						
					}
				}catch (FileNotFoundException | IOException | ParseException e) {
					e.printStackTrace();
				} catch (EmptyConnectionPoolException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			} else if (workMessage.getWorktype() == Worktype.READ_REQUEST_RESPONSE){
				logger.info("Response from slave node for client read request");	

				InternalChannelNode clientInfo = EdgeMonitor.getClientChannelFromMap(workMessage.getRequestId());
				Channel clientChannel =  clientInfo.getChannel();

				CommandMessage outputMsg = MessageGenerator.getInstance().forwardChunkToClient(workMessage);
				QueueManager.getInstance().enqueueOutboundCommand(outputMsg, clientChannel);
				
			}
		} else {
			this.nextChainHandler.handle(workMessage, channel);
		}
	}
}