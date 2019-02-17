package com.taobao.zeus.socket.master;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import com.taobao.zeus.schedule.mvc.ScheduleInfoLog;
import com.taobao.zeus.socket.SocketLog;
import com.taobao.zeus.socket.master.reqresp.MasterBeHeartBeat;
import com.taobao.zeus.socket.master.reqresp.MasterBeUpdate;
import com.taobao.zeus.socket.master.reqresp.MasterBeWebCancel;
import com.taobao.zeus.socket.master.reqresp.MasterBeWebDebug;
import com.taobao.zeus.socket.master.reqresp.MasterBeWebExecute;
import com.taobao.zeus.socket.protocol.Protocol.Operate;
import com.taobao.zeus.socket.protocol.Protocol.Request;
import com.taobao.zeus.socket.protocol.Protocol.Response;
import com.taobao.zeus.socket.protocol.Protocol.SocketMessage;
import com.taobao.zeus.socket.protocol.Protocol.WebOperate;
import com.taobao.zeus.socket.protocol.Protocol.WebRequest;
import com.taobao.zeus.socket.protocol.Protocol.WebResponse;
import com.taobao.zeus.socket.protocol.Protocol.SocketMessage.Kind;


public class MasterHandler extends SimpleChannelInboundHandler<SocketMessage> {

	private CompletionService<ChannelResponse> completionService=new ExecutorCompletionService<>(Executors.newCachedThreadPool());

	private class ChannelResponse{
		Channel channel;
		WebResponse resp;
		ChannelResponse(Channel channel,WebResponse resp){
			this.channel=channel;
			this.resp=resp;
		}
	}
	private MasterContext context;

	public MasterHandler(MasterContext context){
		this.context=context;
		new Thread(() -> {
            while(true){
                try {
                    Future<ChannelResponse> f=completionService.take();
                    ChannelResponse resp=f.get();
                    resp.channel.write(wrapper(resp.resp));
                } catch (Exception e) {
                    ScheduleInfoLog.error("master handler,future take", e);
                }
            }
        }).start();
	}
	private SocketMessage wrapper(WebResponse resp){
		return SocketMessage.newBuilder().setKind(Kind.WEB_RESPONSE).setBody(resp.toByteString()).build();
	}
	private MasterBeHeartBeat beHeartBeat=new MasterBeHeartBeat();
	private MasterBeUpdate beUpdate=new MasterBeUpdate();
	private MasterBeWebCancel beWebCancel=new MasterBeWebCancel();
	private MasterBeWebExecute beWebExecute=new MasterBeWebExecute();
	private MasterBeWebDebug beDebug=new MasterBeWebDebug();
	
	@Override
	public void channelRead0(ChannelHandlerContext ctx, SocketMessage sm)
			throws Exception {
		final Channel channel=ctx.channel();
		if(sm.getKind()==Kind.REQUEST){
			final Request request=Request.newBuilder().mergeFrom(sm.getBody()).build();
			if(request.getOperate()==Operate.HeartBeat){
				beHeartBeat.beHeartBeat(context, channel, request);
			}
		}else if(sm.getKind()==Kind.WEB_REUQEST){
			final WebRequest request=WebRequest.newBuilder().mergeFrom(sm.getBody()).build();
			 if(request.getOperate()==WebOperate.ExecuteJob){
				completionService.submit(() -> new ChannelResponse(channel,beWebExecute.beWebExecute(context,request)));
			}else if(request.getOperate()==WebOperate.CancelJob){
				completionService.submit(() -> new ChannelResponse(channel,beWebCancel.beWebCancel(context,request)));
			}else if(request.getOperate()==WebOperate.UpdateJob){
				completionService.submit(() -> new ChannelResponse(channel,beUpdate.beWebUpdate(context,request)));
			}else if(request.getOperate()==WebOperate.ExecuteDebug){
				completionService.submit(() -> new ChannelResponse(channel, beDebug.beWebExecute(context, request)));
			}
		}else if(sm.getKind()==Kind.RESPONSE){
			for(ResponseListener lis:new ArrayList<>(listeners)){
				lis.onResponse(Response.newBuilder().mergeFrom(sm.getBody()).build());
			}
		}else if(sm.getKind()==Kind.WEB_RESPONSE){
			for(ResponseListener lis:new ArrayList<>(listeners)){
				lis.onWebResponse(WebResponse.newBuilder().mergeFrom(sm.getBody()).build());
			}
		}
		
		super.channelRead(ctx, sm);
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		context.getWorkers().put(ctx.channel(), new MasterWorkerHolder(ctx.channel()));
		Channel channel=ctx.channel();
		SocketAddress addr=channel.remoteAddress();
		SocketLog.info("worker connected , :"+addr.toString());
		super.channelActive(ctx);
	}
	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		SocketLog.info("worker disconnect :"+ctx.channel().remoteAddress().toString());
		context.getMaster().workerDisconnectProcess(ctx.channel());
		super.channelInactive(ctx);
	}
	private List<ResponseListener> listeners=new CopyOnWriteArrayList<>();
	public void addListener(ResponseListener listener){
		listeners.add(listener);
	}
	public void removeListener(ResponseListener listener){
		listeners.remove(listener);
	}
	public interface ResponseListener{
	    void onResponse(Response resp);
	    void onWebResponse(WebResponse resp);
	}
}
