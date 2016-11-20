package gash.router.util;

import java.util.ArrayList;

import gash.router.container.GlobalRoutingConf;
import global.Global.GlobalHeader;
import global.Global.GlobalMessage;
import global.Global.LeaderInfo;
import global.Global.WhoIsLeader;
import pipe.common.Common.Header;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;


public class GlobalMessageBuilder {
	private static GlobalRoutingConf conf;

	public static GlobalRoutingConf getGlobalRoutingConf() {
		System.out.println("entered RaftMsg Bilder constrctiu");
		return conf;
	}

	public static void setGlobalRoutingConf(GlobalRoutingConf conf) {
		GlobalMessageBuilder.conf = conf;

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
}
