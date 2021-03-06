package com.taobao.zeus.socket.worker.reqresp;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;
import com.taobao.zeus.jobs.Job;
import com.taobao.zeus.jobs.JobContext;
import com.taobao.zeus.model.DebugHistory;
import com.taobao.zeus.model.JobHistory;
import com.taobao.zeus.model.JobStatus;
import com.taobao.zeus.socket.SocketLog;
import com.taobao.zeus.socket.protocol.Protocol.DebugMessage;
import com.taobao.zeus.socket.protocol.Protocol.ExecuteMessage;
import com.taobao.zeus.socket.protocol.Protocol.ManualMessage;
import com.taobao.zeus.socket.protocol.Protocol.Operate;
import com.taobao.zeus.socket.protocol.Protocol.Request;
import com.taobao.zeus.socket.protocol.Protocol.Response;
import com.taobao.zeus.socket.protocol.Protocol.Status;
import com.taobao.zeus.socket.worker.WorkerContext;
import com.taobao.zeus.store.JobBean;
import com.taobao.zeus.util.Environment;
import com.taobao.zeus.util.JobUtils;

public class WorkerBeExecute {
	private static Logger log = LoggerFactory.getLogger(WorkerBeExecute.class);
	public Future<Response> execute(final WorkerContext context,
			final Request req) throws InvalidProtocolBufferException {
		if (req.getOperate() == Operate.Debug) {
			return debug(context, req);
		} else if (req.getOperate() == Operate.Manual) {
			return manual(context, req);
		} else if (req.getOperate() == Operate.Schedule) {
			return schedule(context, req);
		}
		return null;
	}

	public Future<Response> manual(final WorkerContext context,
			final Request req) throws InvalidProtocolBufferException {
		ManualMessage mm = ManualMessage.newBuilder().mergeFrom(req.getBody()).build();
		SocketLog.info("receive master to worker manual request,rid="
				+ req.getRid() + ",historyId=" + mm.getHistoryId());
		final String historyId = mm.getHistoryId();
		final JobHistory history = context.getJobHistoryManager()
				.findJobHistory(historyId);
		return context.getThreadPool().submit(
				() -> {
					history.setExecuteHost(WorkerContext.host);
					history.setStartTime(new Date());
					context.getJobHistoryManager()
							.updateJobHistory(history);

					String date = new SimpleDateFormat("yyyy-MM-dd")
							.format(new Date());
					File directory = new File(Environment.getDownloadPath()
							+ File.separator + date + File.separator
							+ "manual-" + history.getId());
					if (!directory.exists()) {
						directory.mkdirs();
					}
					JobBean jb = context.getGroupManager()
							.getUpstreamJobBean(history.getJobId());

					final Job job = JobUtils.createJob(new JobContext(JobContext.MANUAL_RUN),
							jb, history, directory.getAbsolutePath(),
							context.getApplicationContext());
					context.getManualRunnings().put(historyId, job);

					Integer exitCode = -1;
					Exception exception = null;
					try {
						exitCode = job.run();
					} catch (Exception e) {
						exception = e;
						history.getLog().appendZeusException(e);
					} finally {
						JobHistory jobHistory = context
								.getJobHistoryManager()
								.findJobHistory(history.getId());
						jobHistory.setEndTime(new Date());
						if (exitCode == 0) {
							jobHistory
									.setStatus(JobStatus.Status.SUCCESS);
						} else {
							jobHistory
									.setStatus(JobStatus.Status.FAILED);
						}
						context.getJobHistoryManager().updateJobHistory(
								jobHistory);
						history.getLog().appendZeus("exitCode=" + exitCode);
						try{
							context.getJobHistoryManager().updateJobHistoryLog(
									history.getId(),
									history.getLog().getContent());
						}catch(Exception ex){
							log.error("update manual job log exception:", ex);
						}
						context.getManualRunnings().remove(historyId);
					}

					Status status = Status.OK;
					String errorText = "";
					if (exitCode != 0) {
						status = Status.ERROR;
					}
					if (exception != null && exception.getMessage() != null) {
						errorText = exception.getMessage();
					}
					Response resp = Response.newBuilder().setRid(
							req.getRid()).setOperate(Operate.Manual)
							.setStatus(status).setErrorText(errorText)
							.build();
					SocketLog
							.info("send manual response,manual complete,rid="
									+ req.getRid()
									+ ",historyId="
									+ historyId);
					return resp;
				});
	}

	public Future<Response> debug(final WorkerContext context, final Request req) throws InvalidProtocolBufferException {
		DebugMessage dm = DebugMessage.newBuilder().mergeFrom(req.getBody()).build();
		SocketLog.info("receive master to worker debug request,rid="
				+ req.getRid() + ",debugId=" + dm.getDebugId());
		final String debugId = dm.getDebugId();
		final DebugHistory history = context.getDebugHistoryManager()
				.findDebugHistory(debugId);
		return context.getThreadPool().submit(
				() -> {
					history.setExecuteHost(WorkerContext.host);
					history.setStartTime(new Date());
					context.getDebugHistoryManager().updateDebugHistory(
							history);

					String date = new SimpleDateFormat("yyyy-MM-dd")
							.format(new Date());
					File direcotry = new File(Environment.getDownloadPath()
							+ File.separator + date + File.separator
							+ "debug-" + history.getId());
					if (!direcotry.exists()) {
						direcotry.mkdirs();
					}
					final Job job = JobUtils.createDebugJob(
							new JobContext(JobContext.DEBUG_RUN), history, direcotry
									.getAbsolutePath(), context
									.getApplicationContext());
					context.getDebugRunnings().put(debugId, job);

					Integer exitCode = -1;
					Exception exception = null;
					try {
						exitCode = job.run();
					} catch (Exception e) {
						exception = e;
						history.getLog().appendZeusException(e);
					} finally {
						DebugHistory debugHistory = context
								.getDebugHistoryManager()
								.findDebugHistory(history.getId());
						debugHistory.setEndTime(new Date());
						if (exitCode == 0) {
							debugHistory
									.setStatus(JobStatus.Status.SUCCESS);
						} else {
							debugHistory
									.setStatus(JobStatus.Status.FAILED);
						}
						context.getDebugHistoryManager()
								.updateDebugHistory(debugHistory);
						history.getLog().appendZeus("exitCode=" + exitCode);
						try{
							context.getDebugHistoryManager()
									.updateDebugHistoryLog(history.getId(),
											history.getLog().getContent());
						}catch(Exception ex){
							log.error("update debug log exception:", ex);
						}
						context.getDebugRunnings().remove(debugId);
					}

					Status status = Status.OK;
					String errorText = "";
					if (exitCode != 0) {
						status = Status.ERROR;
					}
					if (exception != null && exception.getMessage() != null) {
						errorText = exception.getMessage();
					}
					Response resp = Response.newBuilder().setRid(
							req.getRid()).setOperate(Operate.Debug)
							.setStatus(status).setErrorText(errorText)
							.build();
					SocketLog
							.info("send debug response,debug complete,rid="
									+ req.getRid() + ",debugId=" + debugId);
					return resp;
				});
	}

	private Future<Response> schedule(final WorkerContext context,
			final Request req) throws InvalidProtocolBufferException {
		// 查找该job是否在运行中，如果在，响应ERROR
		// 如果不在，开始执行job，等待执行完毕后，发送完毕请求
		ExecuteMessage em = ExecuteMessage.newBuilder().mergeFrom(req.getBody()).build();
		SocketLog.info("receive master to worker execute request,rid="
				+ req.getRid() + ",jobId=" + em.getJobId());
		final String jobId = em.getJobId();
		if (context.getRunnings().containsKey(jobId)) {
			SocketLog
					.info("send execute response,job is running and can't run again,rid="
							+ req.getRid() + ",jobId=" + em.getJobId());
			return context.getThreadPool().submit(() -> Response.newBuilder().setRid(req.getRid())
					.setOperate(Operate.Schedule).setStatus(
							Status.ERROR).build());
		}
		final JobStatus js = context.getGroupManager().getJobStatus(jobId);
		final JobHistory history = context.getJobHistoryManager()
				.findJobHistory(js.getHistoryId());
		return context.getThreadPool().submit(
				() -> {
					history.setExecuteHost(WorkerContext.host);
					history.setStartTime(new Date());
					context.getJobHistoryManager()
							.updateJobHistory(history);

					JobBean jb = context.getGroupManager()
							.getUpstreamJobBean(history.getJobId());
					String date = new SimpleDateFormat("yyyy-MM-dd")
							.format(new Date());
					File direcotry = new File(Environment.getDownloadPath()
							+ File.separator + date + File.separator
							+ history.getId());
					if (!direcotry.exists()) {
						direcotry.mkdirs();
					}

					final Job job = JobUtils.createJob(new JobContext(JobContext.SCHEDULE_RUN),
							jb, history, direcotry.getAbsolutePath(),
							context.getApplicationContext());
					context.getRunnings().put(jobId, job);

					Integer exitCode = -1;
					Exception exception = null;
					try {
						exitCode = job.run();
					} catch (Exception e) {
						exception = e;
						history.getLog().appendZeusException(e);
					} finally {
						JobHistory jobHistory = context
								.getJobHistoryManager()
								.findJobHistory(history.getId());
						jobHistory.setEndTime(new Date());
						if (exitCode == 0) {
							jobHistory
									.setStatus(JobStatus.Status.SUCCESS);
						} else {
							jobHistory
									.setStatus(JobStatus.Status.FAILED);
						}
						context.getJobHistoryManager().updateJobHistory(
								jobHistory);
						history.getLog().appendZeus("exitCode=" + exitCode);
						try{
							context.getJobHistoryManager().updateJobHistoryLog(
									history.getId(),
									history.getLog().getContent());
						}catch(Exception ex){
							log.error("update schedule job log exception:", ex);
						}
						context.getRunnings().remove(jobId);
					}

					Status status = Status.OK;
					String errorText = "";
					if (exitCode != 0) {
						status = Status.ERROR;
					}
					if (exception != null) {
						errorText = exception.getMessage();
					}
					Response resp = Response.newBuilder().setRid(
							req.getRid()).setOperate(Operate.Schedule)
							.setStatus(status).setErrorText(errorText)
							.build();
					SocketLog
							.info("send execute response,execute complete,rid="
									+ req.getRid()
									+ ",jobId="
									+ history.getJobId());
					return resp;
				});
	}
}
