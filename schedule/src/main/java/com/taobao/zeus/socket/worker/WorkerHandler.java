package com.taobao.zeus.socket.worker;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import com.taobao.zeus.model.DebugHistory;
import com.taobao.zeus.model.JobHistory;
import com.taobao.zeus.schedule.mvc.ScheduleInfoLog;
import com.taobao.zeus.socket.SocketLog;
import com.taobao.zeus.socket.protocol.Protocol.Operate;
import com.taobao.zeus.socket.protocol.Protocol.Request;
import com.taobao.zeus.socket.protocol.Protocol.Response;
import com.taobao.zeus.socket.protocol.Protocol.SocketMessage;
import com.taobao.zeus.socket.protocol.Protocol.WebResponse;
import com.taobao.zeus.socket.protocol.Protocol.SocketMessage.Kind;
import com.taobao.zeus.socket.worker.reqresp.WorkerBeCancel;
import com.taobao.zeus.socket.worker.reqresp.WorkerBeExecute;

public class WorkerHandler extends SimpleChannelInboundHandler<SocketMessage> {

	private CompletionService<Response> completionService=new ExecutorCompletionService<>(Executors.newCachedThreadPool());
	private WorkerContext context;
	public WorkerHandler(final WorkerContext context){
		this.context=context;
		context.setHandler(this);
		new Thread(){
			public void run() {
				while(true){
					try {
						Future<Response> f=completionService.take();
						Response resp=f.get();
						if(context.getServerChannel()!=null){
							context.getServerChannel().write(wapper(resp));
						}
					} catch (Exception e) {
						ScheduleInfoLog.error("take future", e);
					}
				}
			};
		}.start();
	}
	private SocketMessage wapper(Response resp){
		return SocketMessage.newBuilder().setKind(Kind.RESPONSE).setBody(resp.toByteString()).build();
	}
	@Override
	public void channelRead0(ChannelHandlerContext ctx, SocketMessage sm)
			throws Exception {
		if(sm.getKind()==Kind.REQUEST){
			final Request request=Request.newBuilder().mergeFrom(sm.getBody()).build();
			Operate op=request.getOperate();
			if(op==Operate.Schedule || op==Operate.Manual || op==Operate.Debug){
				completionService.submit(new Callable<Response>() {
					private WorkerBeExecute execute=new WorkerBeExecute();
					public Response call() throws Exception {
						return execute.execute(context, request).get();
					}
				});
			}else if(request.getOperate()==Operate.Cancel){
				completionService.submit(new Callable<Response>() {
					private WorkerBeCancel cancel=new WorkerBeCancel();
					public Response call() throws Exception {
						return cancel.execute(context, request).get();
					}
				});
			}
		}else if(sm.getKind()==Kind.RESPONSE){
			final Response resp=Response.newBuilder().mergeFrom(sm.getBody()).build();
			for(ResponseListener lis:listeners){
				lis.onResponse(resp);
			}
		}else if(sm.getKind()==Kind.WEB_RESPONSE){
			final WebResponse resp=WebResponse.newBuilder().mergeFrom(sm.getBody()).build();
			for(ResponseListener lis:listeners){
				lis.onWebResponse(resp);
			}
		}
		super.channelRead(ctx, sm);
	}

	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		super.channelInactive(ctx);
		this.context.setServerChannel(null);
		SocketLog.info("worker disconnect to master");
		//断开连接，如果还有运行中的job，将这些job取消掉
		try{
			for(String jobId:new HashSet<>(context.getRunnings().keySet())){
				JobHistory his = context.getRunnings().get(jobId).getJobContext().getJobHistory();
				if(his != null){
					his.setIllustrate("worker断开连接，主动取消该任务");
					his.getLog().appendZeus("worker与master断开连接，worker主动取消该任务");
					context.getClientWorker().cancelScheduleJob(jobId);
				}
	
			}
			for(String debugId:new HashSet<>(context.getDebugRunnings().keySet())){
				DebugHistory his = context.getDebugRunnings().get(debugId).getJobContext().getDebugHistory();
				if(his != null){
					his.getLog().appendZeus("worker与master断开连接，worker主动取消该任务");
					context.getClientWorker().cancelDebugJob(debugId);
				}
			}
			for(String historyId:new HashSet<>(context.getManualRunnings().keySet())){
				JobHistory his = context.getManualRunnings().get(historyId).getJobContext().getJobHistory();
				if(his != null){
					his.setIllustrate("worker断开连接，主动取消该任务");
					his.getLog().appendZeus("worker与master断开连接，worker主动取消该任务");
					context.getClientWorker().cancelManualJob(historyId);
				}
			}
			SocketLog.info("job cancel successfully");
		}catch(Exception ex){
			SocketLog.error("job cancel exception failed");
			ex.printStackTrace();
		}
	}
	private List<ResponseListener> listeners=new CopyOnWriteArrayList<>();
	public interface ResponseListener{
		void onResponse(Response resp);
		void onWebResponse(WebResponse resp);
	}
	public void addListener(ResponseListener listener){
		listeners.add(listener);
	}
	public void removeListener(ResponseListener listener){
		listeners.remove(listener);
	}
}
