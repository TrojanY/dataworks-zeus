package com.taobao.zeus.socket.master.reqresp;

import java.util.Date;

import io.netty.channel.Channel;

import com.google.protobuf.InvalidProtocolBufferException;
import com.taobao.zeus.socket.master.MasterContext;
import com.taobao.zeus.socket.master.MasterWorkerHolder;
import com.taobao.zeus.socket.master.MasterWorkerHolder.HeartBeatInfo;
import com.taobao.zeus.socket.protocol.Protocol.HeartBeatMessage;
import com.taobao.zeus.socket.protocol.Protocol.Request;

public class MasterBeHeartBeat {
	public void beHeartBeat(MasterContext context,Channel channel,Request request) {
		MasterWorkerHolder worker=context.getWorkers().get(channel);
		HeartBeatInfo heartBeatInfo=worker.new HeartBeatInfo();
		HeartBeatMessage hbm;
		try {
			hbm = HeartBeatMessage.newBuilder().mergeFrom(request.getBody()).build();
			heartBeatInfo.memRate=hbm.getMemRate();
			heartBeatInfo.runnings=hbm.getRunningsList();
			heartBeatInfo.debugRunnings=hbm.getDebugRunningsList();
			heartBeatInfo.manualRunnings=hbm.getManualRunningsList();
			heartBeatInfo.timestamp=new Date(hbm.getTimestamp());
			heartBeatInfo.host=hbm.getHost();
			heartBeatInfo.cpuLoadPerCore=hbm.getCpuLoadPerCore();
			if(worker.heart==null || heartBeatInfo.timestamp.getTime()>worker.heart.timestamp.getTime()){
				worker.heart=heartBeatInfo;
			}
		} catch (InvalidProtocolBufferException e) {
			e.printStackTrace();
		}
	}
}
