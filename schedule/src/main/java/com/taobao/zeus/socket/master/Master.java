package com.taobao.zeus.socket.master;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.taobao.zeus.store.mysql.persistence.JobTaskAction;
import com.taobao.zeus.store.mysql.persistence.JobTask;
import io.netty.channel.Channel;
import org.quartz.JobKey;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.zeus.broadcast.alarm.MailAlarm;
import com.taobao.zeus.broadcast.alarm.SMSAlarm;
import com.taobao.zeus.client.ZeusException;
import com.taobao.zeus.model.DebugHistory;
import com.taobao.zeus.model.FileDescriptor;
import com.taobao.zeus.model.HostGroupCache;
import com.taobao.zeus.model.JobDescriptor;
import com.taobao.zeus.model.JobHistory;
import com.taobao.zeus.model.JobStatus;
import com.taobao.zeus.model.JobStatus.TriggerType;
import com.taobao.zeus.model.Profile;
import com.taobao.zeus.mvc.Controller;
import com.taobao.zeus.mvc.Dispatcher;
import com.taobao.zeus.schedule.mvc.AddJobListener;
import com.taobao.zeus.schedule.mvc.DebugInfoLog;
import com.taobao.zeus.schedule.mvc.DebugListener;
import com.taobao.zeus.schedule.mvc.JobController;
import com.taobao.zeus.schedule.mvc.JobFailListener;
import com.taobao.zeus.schedule.mvc.JobSuccessListener;
import com.taobao.zeus.schedule.mvc.ScheduleInfoLog;
import com.taobao.zeus.schedule.mvc.StopScheduleJobListener;
import com.taobao.zeus.schedule.mvc.ZeusJobException;
import com.taobao.zeus.schedule.mvc.event.DebugFailEvent;
import com.taobao.zeus.schedule.mvc.event.DebugSuccessEvent;
import com.taobao.zeus.schedule.mvc.event.Events;
import com.taobao.zeus.schedule.mvc.event.JobFailedEvent;
import com.taobao.zeus.schedule.mvc.event.JobLostEvent;
import com.taobao.zeus.schedule.mvc.event.JobMaintenanceEvent;
import com.taobao.zeus.schedule.mvc.event.JobSuccessEvent;
import com.taobao.zeus.socket.SocketLog;
import com.taobao.zeus.socket.master.MasterWorkerHolder.HeartBeatInfo;
import com.taobao.zeus.socket.master.reqresp.MasterExecuteJob;
import com.taobao.zeus.socket.protocol.Protocol.ExecuteKind;
import com.taobao.zeus.socket.protocol.Protocol.Response;
import com.taobao.zeus.socket.protocol.Protocol.Status;
import com.taobao.zeus.store.GroupBean;
import com.taobao.zeus.store.JobBean;
import com.taobao.zeus.util.CronExpParser;
import com.taobao.zeus.util.Environment;
import com.taobao.zeus.util.Tuple;
import com.taobao.zeus.util.ZeusDateTool;


public class Master {

	private MasterContext context;
	private static Logger log = LoggerFactory.getLogger(Master.class);
	private Map<Long, JobTaskAction> actionDetails;

	public Master(final MasterContext context) {
		this.context = context;
		GroupBean root = context.getGroupManager().getGlobeGroupBean();

		if (Environment.isPrePub()) {
			// 如果是预发环境，添加stop listener，阻止自动调度执行
			context.getDispatcher().addDispatcherListener(
					new StopScheduleJobListener());
		}
		context.getDispatcher().addDispatcherListener(
				new AddJobListener(context, this));
		context.getDispatcher().addDispatcherListener(
				new JobFailListener(context));
		context.getDispatcher().addDispatcherListener(
				new DebugListener(context));
		context.getDispatcher().addDispatcherListener(
				new JobSuccessListener(context));

		Map<String, JobBean> allJobBeans = root.getAllSubJobBeans();
		for (String id : allJobBeans.keySet()) {
			context.getDispatcher().addController(
					new JobController(context, this, id));
		}
		// 初始化
		context.getDispatcher().forwardEvent(Events.Initialize);
		context.setMaster(this);
		//刷新host分组关系列表
		context.refreshHostGroupCache();
		log.info("refresh HostGroup Cache");

		context.getSchedulePool().scheduleAtFixedRate(() -> {
            //刷新host分组
            context.refreshHostGroupCache();
            log.info("refresh HostGroup Cache");

            //开始漏跑检测、清理schedule
            try{
                //取当前日期
                Date now = new Date();
                SimpleDateFormat dfDateTime=new SimpleDateFormat("yyyyMMddHHmmss0000");
                String currentDateStr = dfDateTime.format(now);

                //取当前日期的后一天.
                Calendar cal = Calendar.getInstance();
                cal.add(Calendar.DAY_OF_MONTH, +1);
                SimpleDateFormat dfNextDate=new SimpleDateFormat("yyyyMMdd0000000000");
                String nextDateStr = dfNextDate.format(cal.getTime());

                Dispatcher dispatcher=context.getDispatcher();
                if(dispatcher != null){
                    Map<Long, JobTaskAction> actionDetailsNew = actionDetails;
                    if(actionDetailsNew != null && actionDetailsNew.size() > 0){
                        //增加controller，并修改event
                        List<Long> rollBackActionId = new ArrayList<>();
                        for (Long id : actionDetailsNew.keySet()) {
                            if(id < (Long.parseLong(currentDateStr)-15000000)){
                                //当前时间15分钟之前JOB的才检测漏跑
                                int loopCount = 0;
                                rollBackLostJob(id, actionDetailsNew, loopCount, rollBackActionId);
                            }
                        }
                        log.info("roll back lost job ok");
                        log.info("roll back action count:"+actionDetailsNew.size());

                        //清理schedule
                        List<Controller> controllers = dispatcher.getControllers();
                        if(controllers!=null && controllers.size()>0){
                            Iterator<Controller> itController = controllers.iterator();
                            while(itController.hasNext()){
                                JobController jobc = (JobController)itController.next();
                                String jobId = jobc.getJobId();
                                if(Long.parseLong(jobId) < (Long.parseLong(currentDateStr)-15000000)){
                                    try {
                                        context.getScheduler().deleteJob(new JobKey(jobId, "zeus"));
                                    } catch (SchedulerException e) {
                                        e.printStackTrace();
                                    }
                                }else if(Long.parseLong(jobId) >= Long.parseLong(currentDateStr) && Long.parseLong(jobId) < Long.parseLong(nextDateStr)){
                                    try {
                                        if(!actionDetailsNew.containsKey(Long.valueOf(jobId))){
                                            context.getScheduler().deleteJob(new JobKey(jobId, "zeus"));
                                            context.getJobManager().removeJob(Long.valueOf(jobId));
                                            itController.remove();
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                        }
                        log.info("clear job scheduler ok");
                    }
                }
            }catch(Exception e){
                log.error("roll back lost job failed or clear job schedule failed !", e);
            }
        }, 1, 1, TimeUnit.HOURS);
		
		//***************2014-09-15 定时扫描JOB表,生成action表********************
		context.getSchedulePool().scheduleAtFixedRate(() -> {
            try{
                Date now = new Date();

                SimpleDateFormat dfHour=new SimpleDateFormat("HH");
                SimpleDateFormat dfMinute=new SimpleDateFormat("mm");
                int execHour = Integer.parseInt(dfHour.format(now));
                int execMinute = Integer.parseInt(dfMinute.format(now));

                if((execHour == 0 && execMinute == 0)
                        || (execHour == 0 && execMinute == 35)
                        || (execHour > 7 && execMinute == 20)
                        || (execHour > 7 && execHour < 22 && execMinute == 50)){
                    //取当前时间
                    SimpleDateFormat dfDate=new SimpleDateFormat("yyyy-MM-dd");
                    SimpleDateFormat dfDateTime=new SimpleDateFormat("yyyyMMddHHmmss");
                    String currentDateStr = dfDateTime.format(now)+"0000";

                    if(execHour == 23){
                        //取当前日期的后一天.
                        Calendar cal = Calendar.getInstance();
                        cal.add(Calendar.DAY_OF_MONTH, +1);
                        SimpleDateFormat dfNextDate=new SimpleDateFormat("yyyyMMdd0000000000");
                        currentDateStr = dfNextDate.format(cal.getTime());
                        now = cal.getTime();
                    }
                    log.info("start to action, date：" + currentDateStr);
                    List<JobTask> jobTaskDetails = context.getJobManager().getAllJobs();
                    Map<Long, JobTaskAction> actionDetailsNew = new HashMap<>();
                    //首先，生成独立任务action
                    runScheduleJobToAction(jobTaskDetails, now, dfDate, actionDetailsNew, currentDateStr);
                    //其次，生成依赖任务action
                    runDependencesJobToAction(jobTaskDetails, actionDetailsNew, currentDateStr, 0);

                    if(execHour < 23){
                        actionDetails = actionDetailsNew;
                    }
                    log.info("run job to action ok");
                    log.info("job to action count:"+actionDetailsNew.size());
                    Dispatcher dispatcher=context.getDispatcher();
                    if(dispatcher != null){
                        //增加controller，并修改event
                        if (actionDetailsNew.size() > 0) {
                            for (Long id : actionDetailsNew.keySet()) {
                                dispatcher.addController(
                                        new JobController(context, context.getMaster(),
                                                id.toString()));
                                if (id > Long.parseLong(currentDateStr)) {
                                    context.getDispatcher().forwardEvent(
                                            new JobMaintenanceEvent(Events.UpdateJob,
                                                    id.toString()));
                                }
                            }
                        }
                    }
                    log.info("add job to scheduler ok");
                }
            }catch(Exception e){
                log.error("job to action failed !", e);
            }
        }, 1, 1, TimeUnit.MINUTES);
				
		// 定时扫描等待队列
		log.info("The scan rate is " + Environment.getScanRate());
		context.getSchedulePool().scheduleAtFixedRate(() -> {
            try {
                scan();
            } catch (Exception e) {
                log.error("get job from queue failed!", e);
            }
        }, 0, Environment.getScanRate(), TimeUnit.MILLISECONDS);
	
		log.info("The scan exception rate is " + Environment.getScanExceptionRate());
		context.getSchedulePool().scheduleAtFixedRate(() -> {
            try {
                scanExceptionQueue();
            } catch (Exception e) {
                log.error("get job from exception queue failed!", e);
            }
        }, 0, Environment.getScanExceptionRate(), TimeUnit.MILLISECONDS);
		
		// 定时扫描worker channel，心跳超过1分钟没有连接就主动断掉
		context.getSchedulePool().scheduleAtFixedRate(() -> {
            Date now = new Date();
            for (MasterWorkerHolder holder : new ArrayList<>(
                    context.getWorkers().values())) {
                log.info("schedule worker start:"+holder.getDebugRunnings().size());
                try {
                    if (holder.getHeart().timestamp == null
                            || (now.getTime() - holder.getHeart().timestamp
                                    .getTime()) > 1000 * 60) {
                        holder.getChannel().close();
                    }
                } catch (Exception e) {
                    log.error("holder:"+holder+" is in error",e);
                }

                log.info("schedule worker end:"+holder.getDebugRunnings().size());
            }
        }, 30, 30, TimeUnit.SECONDS);
		
		context.getSchedulePool().scheduleAtFixedRate(() -> {
            try {
                // 检测任务超时
                checkTimeOver();
            } catch (Exception e) {
                log.error("error occurs in checkTimeOver",e);
            }
        }, 0, 3, TimeUnit.SECONDS);
	}

	//重新调度漏跑的JOB
	public void rollBackLostJob(Long id, final Map<Long, JobTaskAction> actionDetails, int loopCount, List<Long> rollBackActionId){
		loopCount ++;
		try {
			JobTaskAction lostJob = actionDetails.get(id);
			if(lostJob != null){
				String jobDependStr = lostJob.getDependencies();
				if (jobDependStr != null && jobDependStr.trim().length() > 0) {
					String[] jobDependencies = jobDependStr.split(",");
					boolean isAllComplete = true;
					if(jobDependencies.length > 0){
						for (String jobDepend : jobDependencies) {
							if(actionDetails.get(Long.parseLong(jobDepend)) != null){
								if (actionDetails.get(Long.parseLong(jobDepend)).getStatus() == null
										|| actionDetails.get(Long.parseLong(jobDepend)).getStatus().equals("wait")) {
									isAllComplete = false;
									// 递归查询
									if (loopCount < 30 && rollBackActionId.contains(Long.parseLong(jobDepend))) {
										rollBackLostJob(Long.parseLong(jobDepend), actionDetails, loopCount, rollBackActionId);
									}
								} else if (actionDetails.get(Long.parseLong(jobDepend)).getStatus().equals("failed")) {
									isAllComplete = false;
								}
							}
						}
					}
					if(isAllComplete){
						if(!rollBackActionId.contains(id)){
							context.getDispatcher().forwardEvent(
									new JobLostEvent(Events.UpdateJob, id.toString()));
							rollBackActionId.add(id);
//							log.info("roll back lost jobID :" + id.toString());
						}
					}
				} else {
					if(!rollBackActionId.contains(id)){
						context.getDispatcher().forwardEvent(
								new JobLostEvent(Events.UpdateJob, id.toString()));
						rollBackActionId.add(id);
//						log.info("roll back lost jobID :" + id.toString());
					}
				}
			}
		} catch (Exception e) {
			log.error("roll back lost job failed !", e);
		}
	}
	
	//获取可用的worker
//	private MasterWorkerHolder getRunableWorker() {
//		MasterWorkerHolder selectWorker = null;
//		Float selectMemRate = null;
//		for (MasterWorkerHolder worker : context.getWorkers().values()) {
//			if(worker != null){
//				HeartBeatInfo heart = worker.getHeart();
//				log.info("worker a : heart :" + heart.memRate);
//				if (heart != null && heart.memRate != null && heart.memRate < HostIndex.MAX_MEM_RATE && heart.cpuLoadPerCore < HostIndex.MAX_CPULOAD_PER_CORE) {
//					if (selectWorker == null) {
//						selectWorker = worker;
//						selectMemRate = heart.memRate;
//						log.info("worker b : heart :"+ selectMemRate);
//					} else if (selectMemRate > heart.memRate) {
//						selectWorker = worker;
//						selectMemRate = heart.memRate;
//						log.info("worker c : heart :"+ selectMemRate);
//					}
//				}
//			}
//		}
//		return selectWorker;
//	}

//	private MasterWorkerHolder getRunableWorker(String host) {
//		MasterWorkerHolder selectWorker = null;
//		Float selectMemRate = null;
//		if (host != null && !"".equals(host)) {
//			boolean isWorkerHost = false;
//			for (MasterWorkerHolder worker : context.getWorkers().values()) {
//				if(worker != null){
//					HeartBeatInfo heart = worker.getHeart();
//					log.info("worker a : host :" + host + " heart :" + heart.memRate);
//					if (heart != null && heart.memRate != null
//							&& heart.memRate < 0.8 && host.equals(heart.host)) {
//						isWorkerHost = true;
//						if (selectWorker == null) {
//							selectWorker = worker;
//							selectMemRate = heart.memRate;
//							log.info("worker b : host :" + host+ " heart :" + selectMemRate);
//						} else if (selectMemRate > heart.memRate) {
//							selectWorker = worker;
//							selectMemRate = heart.memRate;
//							log.info("worker c : host :" + host+ " heart :" + selectMemRate);
//						}
//					}
//				}
//			}
//			if(!isWorkerHost){
//				return this.getRunableWorker();
//			}
//			return selectWorker;
//		}
//
//		else {
//			return this.getRunableWorker();
//		}
//
//	}
	
//	private MasterWorkerHolder getRunableWorker(String hostGroupId) {
//		if (hostGroupId == null) {
//			hostGroupId = Environment.getDefaultWorkerGroupId();
//		}
//		MasterWorkerHolder selectWorker = null;
//		Float selectMemRate = null;
//		Set<String> workersGroup = getWorkersByGroupId(hostGroupId);
//		for (MasterWorkerHolder worker : context.getWorkers().values()) {
//			try {
//				if (worker!=null && worker.getHeart()!=null && workersGroup.contains(worker.getHeart().host)) {
//					HeartBeatInfo heart = worker.getHeart();
//					if (heart != null && heart.memRate != null && heart.memRate < Environment.getMaxMemRate() && heart.cpuLoadPerCore < Environment.getMaxCpuLoadPerCore() ) {
//						if (selectWorker == null) {
//							selectWorker = worker;
//							selectMemRate = heart.memRate;
//							log.info("worker b : host " + heart.host + ",heart "+ selectMemRate);
//						} else if (selectMemRate > heart.memRate) {
//							selectWorker = worker;
//							selectMemRate = heart.memRate;
//							log.info("worker c : host " + heart.host + ",heart "+ selectMemRate);
//						}
//					}
//				}else {
//					if(worker == null){
//						log.error("worker is null");
//					}else if(worker!=null && worker.getHeart()==null){
//						log.error("worker " + worker.getChannel().toString()+" heart is null");
//					}
//				}
//			} catch (Exception e) {
//				log.error("worker failed",e);
//			}
//		}
//		if (selectWorker != null) {
//			log.info("select worker: " + selectWorker.getHeart().host + ", for HostGroupId " + hostGroupId);
//		}else {
//			log.error("can not find proper workers");
//		}
//		return selectWorker;
//	}
	
//	private Boolean hasWorkerInHostGroup(String id){
//		Set<String> workersGroup = getWorkersByGroupId(id);
//		for (MasterWorkerHolder worker : context.getWorkers().values()) {
//			if (worker!=null && workersGroup.contains(worker.getHeart().host)){
//				return true;
//			}
//		}
//		return false;
//	}
	
//	private Set<String> getWorkersByGroupId(String hostGroupId){
//		Set<String> workers = new HashSet<String>();
//		for(HostGroupCache hostgroup : context.getHostGroupCache()){
//			if (hostgroup.getId().equals(hostGroupId) ) {
//				for (String host : hostgroup.getHosts()) {
//					workers.add(host);
//				}
//				break;
//			}
//		}
//		return workers;
//	}
	
	private synchronized MasterWorkerHolder getRunableWorker(String hostGroupId) {
		if (hostGroupId == null) {
			hostGroupId = Environment.getDefaultWorkerGroupId();
		}
		MasterWorkerHolder selectWorker = null;
		if (context.getHostGroupCache()!=null) {
			HostGroupCache hostGroupCache = context.getHostGroupCache().get(hostGroupId);
			if (hostGroupCache != null && hostGroupCache.getHosts()!=null && hostGroupCache.getHosts().size()>0) {
				int size = hostGroupCache.getHosts().size();
				for (int i = 0; i < size && selectWorker == null; i++) {
					String host = hostGroupCache.selectHost();
					if (host == null) {
						break;
					}
					for (MasterWorkerHolder worker : context.getWorkers().values()) {
						try {
							if (worker!=null && worker.getHeart()!=null && worker.getHeart().host.equals(host)) {
								HeartBeatInfo heart = worker.getHeart();
								if (heart != null && heart.memRate != null && heart.cpuLoadPerCore!=null && heart.memRate < Environment.getMaxMemRate() && heart.cpuLoadPerCore < Environment.getMaxCpuLoadPerCore()){
									selectWorker = worker;
									break;
								}
							}
							else {
								if(worker == null){
									log.error("worker is null");
								}else if(worker.getHeart()==null && worker.getChannel()!=null){
									log.error("worker " + worker.getChannel().toString()+" heart is null");
								}
							}	
						} catch (Exception e) {
							log.error("worker failed",e);
						}
					}
				}
			}
		}
		if (selectWorker != null) {
			log.info("select worker: " + selectWorker.getHeart().host + ", for HostGroupId " + hostGroupId);
		}else {
			log.error("can not find proper workers for hostGroupId:"+hostGroupId);
		}
		return selectWorker;
	}
	
	
 	//扫描可用的worker，给worker分配JOB任务
	private void scan() {

		if (!context.getQueue().isEmpty()) {
			log.info("schedule queue :" +context.getQueue().size());
			final JobElement e = context.getQueue().poll();
            assert e != null;
            log.info("priority level :"+e.getPriorityLevel()+"; JobID :"+e.getJobID());
			runScheduleAction(e);
		}
		
		if (!context.getManualQueue().isEmpty()) {
			log.info("manual queue :" +context.getManualQueue().size());
			final JobElement e = context.getManualQueue().poll();
            assert e != null;
            log.info("priority level: "+e.getPriorityLevel()+"; JobID:"+e.getJobID());
			MasterWorkerHolder selectWorker = getRunableWorker(e.getHostGroupId());

			if (selectWorker == null) {
				context.getManualQueue().offer(e);
				log.info("HostGroupId : "  + e.getHostGroupId() + ","+e.getJobID() +" is offered back to queue");
			} else {
				runManualJob(selectWorker, e.getJobID());
				log.info("HostGroupId : "  + e.getHostGroupId() + ",schedule selectWorker : " +selectWorker+",host :"+selectWorker.getHeart().host);
			}
		}
		
		if (!context.getDebugQueue().isEmpty()) {
			log.info("debug queue :" +context.getDebugQueue().size() );
			final JobElement e = context.getDebugQueue().poll();
            assert e != null;
            log.info("priority level:null; JobID:"+e.getJobID());
			MasterWorkerHolder selectWorker = getRunableWorker(e.getHostGroupId());
			if (selectWorker == null) {
				context.getDebugQueue().offer(e);
				log.info("HostGroupId : "  + e.getHostGroupId() + ","+e.getJobID() +" is offered back to queue");
			} else {
				runDebugJob(selectWorker, e.getJobID());
				log.info("HostGroupId : "  + e.getHostGroupId() + ",schedule selectWorker : " +selectWorker+",host :"+selectWorker.getHeart().host);
			}
		}
		
//		 检测任务超时
//		checkTimeOver();
	}

	private void runScheduleAction(final JobElement e) {
		MasterWorkerHolder selectWorker = getRunableWorker(e.getHostGroupId());
		if (selectWorker == null) {
				context.getExceptionQueue().offer(e);
				log.info("HostGroupId : "  + e.getHostGroupId() + ","+e.getJobID() +" is offered to exceptionQueue");
		} else {
			runScheduleJob(selectWorker, e.getJobID());
			log.info("HostGroupId : "  + e.getHostGroupId() + ",schedule selectWorker : " +selectWorker+",host :"+selectWorker.getHeart().host);
		}
	}
	
	private void scanExceptionQueue(){
		if(!context.getExceptionQueue().isEmpty()){
			log.info("exception queue :" +context.getExceptionQueue().size());
			final JobElement e = context.getExceptionQueue().poll();
            assert e != null;
            runScheduleAction(e);
		}
	}

	private void runDebugJob(MasterWorkerHolder selectWorker, final String jobID) {
		final MasterWorkerHolder w = selectWorker;
		//final JobElement debugId = context.getDebugQueue().poll();
		SocketLog.info("master scan and poll debugId=" + jobID
				+ " and run!");

		new Thread(() -> {
            DebugHistory history = context.getDebugHistoryManager()
                    .findDebugHistory(jobID);
            history.getLog().appendZeus(
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                            .format(new Date()) + " 开始运行");
            context.getDebugHistoryManager().updateDebugHistoryLog(
                    jobID, history.getLog().getContent());
            Exception exception = null;
            Response resp = null;
            try {
                Future<Response> f = new MasterExecuteJob().executeJob(
                        context, w, ExecuteKind.DebugKind,
                        jobID);
                resp = f.get();
            } catch (Exception e) {
                exception = e;
                DebugInfoLog.error(
                        String.format("debugId:%s run failed",
                                jobID), e);
            }
            finally {
                assert resp != null;
                boolean success = resp.getStatus() == Status.OK;

                if (!success) {
                    // 运行失败，更新失败状态，发出失败消息
                    if (exception != null) {
                        exception = new ZeusException(String.format(
                                "fileId:%s run failed ", history.getFileId()),
                                exception);
                    } else {
                        exception = new ZeusException(String.format(
                                "fileId:%s run failed ", history.getFileId()));
                    }
                    DebugInfoLog.info("debugId:" + jobID + " run fail ");
                    history = context.getDebugHistoryManager()
                            .findDebugHistory(jobID);
                    DebugFailEvent jfe = new DebugFailEvent(
                            history.getFileId(), history, exception);
                    context.getDispatcher().forwardEvent(jfe);
                } else {
                    // 运行成功，发出成功消息
                    DebugInfoLog.info("debugId:" + jobID + " run success");
                    DebugSuccessEvent dse = new DebugSuccessEvent(
                            history.getFileId(), history);
                    context.getDispatcher().forwardEvent(dse);
                }
            }
        }).start();
	}

	private void runManualJob(MasterWorkerHolder selectWorker,final String jobID) {
		final MasterWorkerHolder w = selectWorker;
		//final JobElement historyId = context.getManualQueue().poll();
		SocketLog.info("master scan and poll historyId=" + jobID
				+ " and run!");
		new Thread(() -> {
            JobHistory history = context.getJobHistoryManager()
                    .findJobHistory(jobID);
            history.getLog().appendZeus(
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                            .format(new Date()) + " 开始运行");
            context.getJobHistoryManager().updateJobHistoryLog(
                    jobID, history.getLog().getContent());
            //更新手动运行状态
            JobStatus jobstatus = context.getJobManager().getJobStatus(history.getJobId());
            jobstatus.setStatus(JobStatus.Status.RUNNING);
            jobstatus.setHistoryId(jobID);
            context.getJobManager().updateJobStatus(jobstatus);

            Exception exception = null;
            Response resp = null;
            try {
                Future<Response> f = new MasterExecuteJob().executeJob(
                        context, w, ExecuteKind.ManualKind,
                        jobID);
                resp = f.get();
            } catch (Exception e) {
                exception = e;
                ScheduleInfoLog.error("JobId:" + history.getJobId()
                        + " run failed", e);
                jobstatus.setStatus(JobStatus.Status.FAILED);
                context.getJobManager().updateJobStatus(jobstatus);
            }
            assert resp != null;
            boolean success = resp.getStatus() == Status.OK;

            if (!success) {
                // 运行失败，更新失败状态，发出失败消息
                ZeusJobException jobException = null;
                if (exception != null) {
                    jobException = new ZeusJobException(history.getJobId(),
                            String.format("JobId:%s run failed ",
                                    history.getJobId()), exception);
                } else {
                    jobException = new ZeusJobException(history.getJobId(),
                            String.format("JobId:%s run failed ",
                                    history.getJobId()));
                }
                ScheduleInfoLog.info("jobId:" + history.getJobId()
                        + " run fail ");
                history = context.getJobHistoryManager().findJobHistory(
                        jobID);

                jobstatus.setStatus(JobStatus.Status.FAILED);
                JobFailedEvent jfe = new JobFailedEvent(history.getJobId(),
                        history.getTriggerType(), history, jobException);
                if (history.getIllustrate() == null
                        || !history.getIllustrate().contains("手动取消该任务")) {
                    context.getDispatcher().forwardEvent(jfe);
                }
            } else {
                // 运行成功，发出成功消息
                ScheduleInfoLog.info("manual jobId::" + history.getJobId()
                        + " run success");
                jobstatus.setStatus(JobStatus.Status.SUCCESS);
                JobSuccessEvent jse = new JobSuccessEvent(
                        history.getJobId(), history.getTriggerType(),
                        jobID);
                context.getDispatcher().forwardEvent(jse);
            }
            context.getJobManager().updateJobStatus(jobstatus);
        }).start();
	}

	private void runScheduleJob(MasterWorkerHolder selectWorker,final String jobID) {
		final MasterWorkerHolder w = selectWorker;
		//final JobElement jobId = context.getQueue().poll();
		SocketLog.info("master scan and poll jobId=" + jobID + " and run!");
		new Thread(() -> {
            int runCount = 0;
            int rollBackTimes = 0;
            int rollBackWaitTime = 1;
            try{
                JobDescriptor jobDes = context.getJobManager().getJobDescriptor(jobID).getX();
                Map<String,String> properties = jobDes.getProperties();
                if(properties!=null && properties.size()>0){
                    rollBackTimes = Integer.parseInt(properties.get("roll.back.times")==null ? "0" : properties.get("roll.back.times"));
                    rollBackWaitTime = Integer.parseInt(properties.get("roll.back.wait.time")==null ? "1" : properties.get("roll.back.wait.time"));
                }
            }catch(Exception ex){
                rollBackTimes = 0;
                rollBackWaitTime = 1;
            }
            try{
                runScheduleJobContext(w, jobID, runCount, rollBackTimes, rollBackWaitTime);
            }catch(Exception ex){
                log.error("roll back failed job failed !",ex);
            }
        }).start();
	}

	//schedule任务运行，失败后重试
	private void runScheduleJobContext(MasterWorkerHolder w, final String jobID, int runCount, final int rollBackTimes, final int rollBackWaitTime){
		runCount++;
		boolean isCancelJob = false;
		if(runCount > 1){
			try {
				Thread.sleep(rollBackWaitTime*60*1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// 先根据任务ID，查询出任务上次执行的历史记录（jobID->historyid->JobTaskHistory)
		JobHistory his;
		TriggerType type;
		if(runCount == 1){
			his = context.getJobHistoryManager().findJobHistory(
					context.getJobManager()
							.getJobStatus(jobID).getHistoryId());
			type = his.getTriggerType();
			ScheduleInfoLog.info("JobId:" + jobID + " run start");
			his.getLog().appendZeus(
					new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
							.format(new Date()) + " 开始运行");
		}else{
			JobDescriptor jobDescriptor = context.getJobManager().getJobDescriptor(jobID).getX();
			his = new JobHistory();
			his.setIllustrate("失败任务重试，开始执行");
			his.setTriggerType(TriggerType.SCHEDULE);
			type = his.getTriggerType();
			his.setJobId(jobDescriptor.getJobId());
			his.setOperator(jobDescriptor.getOwner());
			his.setActionId(jobDescriptor.getActionId());
			his.setTimezone(jobDescriptor.getTimezone());
			his.setStatus(com.taobao.zeus.model.JobStatus.Status.RUNNING);
			his.setHostGroupId(jobDescriptor.getHostGroupId());
			context.getJobHistoryManager().addJobHistory(his);
			his.getLog().appendZeus(
					new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
							.format(new Date()) + " 第" + (runCount-1) + "次重试运行");
		}
		context.getJobHistoryManager().updateJobHistoryLog(his.getId(),
				his.getLog().getContent());
		JobStatus jobstatus = context.getJobManager().getJobStatus(his.getJobId());
		jobstatus.setHistoryId(his.getId());
		jobstatus.setStatus(com.taobao.zeus.model.JobStatus.Status.RUNNING);
		context.getJobManager().updateJobStatus(jobstatus);
		
		Exception exception = null;
		Response resp = null;
		try {
			Future<Response> f = new MasterExecuteJob().executeJob(
					context, w, ExecuteKind.ScheduleKind, his.getId());
			resp = f.get();
		} catch (Exception e) {
			exception = e;
			ScheduleInfoLog.error(
					String.format("JobId:%s run failed", jobID), e);
			jobstatus.setStatus(com.taobao.zeus.model.JobStatus.Status.FAILED);
			context.getJobManager().updateJobStatus(jobstatus);
		}
        assert resp != null;
        boolean success = resp.getStatus() == Status.OK;
		if (success
				&& (his.getTriggerType() == TriggerType.SCHEDULE || his
						.getTriggerType() == TriggerType.MANUAL_RECOVER)) {
			ScheduleInfoLog.info("JobId:" + jobID
					+ " clear ready dependency");
			jobstatus.setReadyDependency(new HashMap<>());
		}
		if (!success) {
			// 运行失败，更新失败状态，发出失败消息
			ZeusJobException jobException;
			if (exception != null) {
				jobException = new ZeusJobException(jobID,
						String.format("JobId:%s run failed ",
								jobID), exception);
			} else {
				jobException = new ZeusJobException(jobID,
						String.format("JobId:%s run failed ",
								jobID));
			}
			ScheduleInfoLog.info("JobId:" + jobID
					+ " run fail and dispatch the fail event");
			jobstatus.setStatus(com.taobao.zeus.model.JobStatus.Status.FAILED);
			JobHistory jobHistory = context.getJobHistoryManager().findJobHistory(his.getId());
			JobFailedEvent jfe = new JobFailedEvent(jobID, type, jobHistory, jobException);
			jfe.setRollBackTime(rollBackTimes);
			jfe.setRunCount(runCount);
			if (jobHistory != null && jobHistory.getIllustrate() != null
					&& jobHistory.getIllustrate().contains("手动取消该任务")) {
				isCancelJob = true;
			} else {
				context.getDispatcher().forwardEvent(jfe);
			}
		} else {
			// 运行成功，发出成功消息
			ScheduleInfoLog.info("JobId:" + jobID
					+ " run success and dispatch the success event");
			jobstatus.setStatus(com.taobao.zeus.model.JobStatus.Status.SUCCESS);
			JobSuccessEvent jse = new JobSuccessEvent(jobID,
					his.getTriggerType(), his.getId());
			jse.setStatisEndTime(his.getStatisEndTime());
			context.getDispatcher().forwardEvent(jse);
		}
		context.getJobManager().updateJobStatus(jobstatus);
		if(runCount < (rollBackTimes + 1) && !success && !isCancelJob){
			runScheduleJobContext(w, jobID, runCount, rollBackTimes, rollBackWaitTime);
		}
	}
	
	/**
	 * 检查任务超时
	 */
	private void checkTimeOver() {
		for (MasterWorkerHolder w : context.getWorkers().values()) {
			checkScheduleTimeOver(w);
			//TODO 未测试
//			checkManualTimeOver(w);
//			checkDebugTimeOver(w);
		}
	}

	private void checkDebugTimeOver(MasterWorkerHolder w) {
		for (Map.Entry<String, Boolean> entry : w.getDebugRunnings().entrySet()) {
			if (entry.getValue() != null && entry.getValue()) {
				continue;
			}
			String historyId = entry.getKey();
			DebugHistory his = context.getDebugHistoryManager()
					.findDebugHistory(historyId);
			long maxTime;
			FileDescriptor fd;
			try {
				fd = context.getFileManager().getFile(his.getFileId());
				Profile pf = context.getProfileManager().findByUid(
						fd.getOwner());
				String maxTimeString = pf.getHadoopConf().get(
						"zeus.job.maxtime");
				if (maxTimeString == null || maxTimeString.trim().isEmpty()) {
					continue;
				}
				maxTime = Long.parseLong(maxTimeString);

				if (maxTime < 0) {
					continue;
				}
			} catch (Exception e) {
				continue;
			}
			long runTime = (System.currentTimeMillis() - his.getStartTime()
					.getTime()) / 1000 / 60;
			if (runTime > maxTime) {
				if (timeOverAlarm(null, fd, runTime, maxTime, 2, null)) {
					w.getDebugRunnings().replace(historyId, false, true);
				}
			}
		}
	}

	private void checkManualTimeOver(MasterWorkerHolder w) {
		for (Map.Entry<String, Boolean> entry : w.getManualRunnings()
				.entrySet()) {
			if (entry.getValue() != null && entry.getValue()) {
				continue;
			}
			String historyId = entry.getKey();
			JobHistory his = context.getJobHistoryManager().findJobHistory(
					historyId);
			JobDescriptor jd = context.getJobManager()
					.getJobDescriptor(his.getJobId()).getX();
			long maxTime;
			try {
				String maxTimeString = jd.getProperties().get(
						"zeus.job.maxtime");
				if (maxTimeString == null || maxTimeString.trim().isEmpty()) {
					continue;
				}
				maxTime = Long.parseLong(maxTimeString);

				if (maxTime < 0) {
					continue;
				}
			} catch (Exception e) {
				continue;
			}
			long runTime = (System.currentTimeMillis() - his.getStartTime()
					.getTime()) / 1000 / 60;
			if (runTime > maxTime) {
				if (timeOverAlarm(his, null, runTime, maxTime, 1, jd)) {
					w.getManualRunnings().replace(historyId, false, true);
				}
			}
		}
	}

	private void checkScheduleTimeOver(MasterWorkerHolder w) {
		for (Map.Entry<String, Boolean> entry : w.getRunnings().entrySet()) {
			if (entry.getValue() != null && entry.getValue()) {
				continue;
			}
			String jobId = entry.getKey();
			JobDescriptor jd = context.getJobManager()
					.getJobDescriptor(jobId).getX();
			String maxTimeString = jd.getProperties().get("zeus.job.maxtime");
			long maxTime;
			try {
				if (maxTimeString == null || maxTimeString.trim().isEmpty()) {
					continue;
				}
				maxTime = Long.parseLong(maxTimeString);

				if (maxTime < 0) {
					continue;
				}
			} catch (Exception e) {
				continue;
			}

			JobHistory his = context.getJobHistoryManager().findJobHistory(
					context.getJobManager().getJobStatus(jobId)
							.getHistoryId());
			if (his != null && his.getStartTime() != null) {
				long runTime = (System.currentTimeMillis() - his.getStartTime()
						.getTime()) / 1000 / 60;
				if (runTime > maxTime) {
					log.info("send the timeOverAlarm of job: " + jobId);
					if (timeOverAlarm(his, null, runTime, maxTime, 0, jd)) {
						w.getRunnings().replace(jobId, false, true);
					}
				}
			}
		}
	}

	private boolean timeOverAlarm(final JobHistory his, FileDescriptor fd,
			long runTime, long maxTime, int type, JobDescriptor jd) {
		final MailAlarm mailAlarm = (MailAlarm) context.getApplicationContext()
				.getBean("mailAlarm");
		SMSAlarm smsAlarm = (SMSAlarm) context.getApplicationContext().getBean(
				"smsAlarm");

		final StringBuffer title = new StringBuffer("宙斯任务超时[");
		switch (type) {
		case 0:
			title.append("自动调度").append("] jobID=").append(his.getJobId());
			break;
		case 1:
			title.append("手动调度").append("] jobID=").append(his.getJobId());
			break;
		case 2:
			title.append("调试任务").append("] 脚本名称：").append(fd.getName());
		}
		final StringBuffer content = new StringBuffer(title);
		if(jd != null){
			title.append(" (").append(jd.getName()).append(")");
			content.append("\nJOB任务名称：").append(jd.getName());
			Map<String, String> properties=jd.getProperties();
			if(properties != null){
				String plevel=properties.get("run.priority.level");
				if("1".equals(plevel)){
					content.append("\nJob任务优先级: ").append("low");
				}else if("2".equals(plevel)){
					content.append("\nJob任务优先级: ").append("medium");
				}else if("3".equals(plevel)){
					content.append("\nJob任务优先级: ").append("high");
				}
			}
			content.append("\nJOB任务Owner：").append(jd.getOwner());
		}
		content.append("\n已经运行时间：").append(runTime).append("分钟")
				.append("\n设置最大运行时间：").append(maxTime).append("分钟");
		if(his != null){
			content.append("\n运行日志：\n").append(his.getLog().getContent().replaceAll("\\n", "<br/>"));
		}
		try {
			if (type == 2) {
				// 此处可以发送IM消息
			} else {
				// 此处可以发送IM消息
				new Thread(() -> {
                    try {
                        Thread.sleep(6000);
                        assert his != null;
                        mailAlarm
                                .alarm(his.getId(),
                                        title.toString(),
                                        content.toString()
                                                .replace("\n", "<br/>"));
                    } catch (Exception e) {
                        log.error("send run timeover mail alarm failed", e);
                    }
                }).start();
				if (type == 0) {
					String priorityLevel = "3";
					if(jd != null){
						priorityLevel = jd.getProperties().get("run.priority.level");
					}
					if(priorityLevel == null || !priorityLevel.trim().equals("1")){
						Calendar now = Calendar.getInstance();
						int hour = now.get(Calendar.HOUR_OF_DAY);
						int day = now.get(Calendar.DAY_OF_WEEK);
						if (day == Calendar.SATURDAY || day == Calendar.SUNDAY
								|| hour < 9 || hour > 18) {
							smsAlarm.alarm(his.getId(), title.toString(),content.toString(), null);
							//mailAlarm.alarm(his.getId(), title.toString(),content.toString(), null);
						}
					}
				}
			}
			return true;
		} catch (Exception e) {
			log.error("send run timeover alarm failed", e);
			return false;
		}
	}

	public void workerDisconnectProcess(Channel channel) {
		MasterWorkerHolder holder = context.getWorkers().get(channel);
		if (holder != null) {
//			SocketLog.info("worker disconnect, ip:" + channel.getRemoteAddress().toString());
			context.getWorkers().remove(channel);
			final List<JobHistory> hiss = new ArrayList<>();
			Map<String, Tuple<JobDescriptor, JobStatus>> map = context
					.getJobManager().getJobDescriptor(
							holder.getRunnings().keySet());
			for (String key : map.keySet()) {
				JobStatus js = map.get(key).getY();
				if (js.getHistoryId() != null) {
					JobHistory his = context.getJobHistoryManager().findJobHistory(
							js.getHistoryId());
					if(his != null){
						hiss.add(his);
					}
				}
				/*js.setStatus(com.taobao.zeus.model.JobStatus.Status.FAILED);
				context.getGroupManager().updateJobStatus(js);*/
			}
			new Thread(() -> {
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for (JobHistory his : hiss) {
                    String jobId = his.getJobId();
                    his.setEndTime(new Date());
                    his.setStatus(JobStatus.Status.FAILED);
                    his.setIllustrate("worker断线，任务失败");
                    context.getJobHistoryManager().updateJobHistory(his);
                    JobHistory history = new JobHistory();
                    history.setJobId(jobId);
                    history.setActionId(his.getActionId());
                    history.setTriggerType(his.getTriggerType());
                    history.setIllustrate("worker断线，重新跑任务");
                    history.setOperator(his.getOperator());
                    history.setHostGroupId(his.getHostGroupId());
                    context.getJobHistoryManager().addJobHistory(history);
                    Master.this.run(history);
                }
            }).start();

		}
	}

	public void debug(DebugHistory debug) {
		JobElement element = new JobElement(debug.getId(), debug.gethostGroupId());
		debug.setStatus(com.taobao.zeus.model.JobStatus.Status.RUNNING);
		debug.setStartTime(new Date());
		context.getDebugHistoryManager().updateDebugHistory(debug);
		debug.getLog().appendZeus(
				new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
						+ " 进入任务队列");
		context.getDebugHistoryManager().updateDebugHistoryLog(debug.getId(),
				debug.getLog().getContent());
		context.getDebugQueue().offer(element);
		System.out.println("offer debug queue :" +context.getDebugQueue().size()+ " element :"+element.getJobID());
	}

	public JobHistory run(JobHistory history) {
		String jobId = history.getJobId();
		int priorityLevel = 3;
		try{
			JobDescriptor jd = context.getJobManager().getJobDescriptor(jobId).getX();
			String priorityLevelStr = jd.getProperties().get("run.priority.level");
			if(priorityLevelStr!=null){
				priorityLevel = Integer.parseInt(priorityLevelStr);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
		JobElement element = new JobElement(jobId, history.getHostGroupId(), priorityLevel);
		history.setStatus(com.taobao.zeus.model.JobStatus.Status.RUNNING);
		if (history.getTriggerType() == TriggerType.MANUAL_RECOVER) {
			for (JobElement e : new ArrayList<>(context.getQueue())) {
				if (e.getJobID().equals(jobId)) {
					history.getLog().appendZeus("已经在队列中，无法再次运行");
					history.setStartTime(new Date());
					history.setEndTime(new Date());
					history.setStatus(com.taobao.zeus.model.JobStatus.Status.FAILED);
					break;
				}
			}
			for (Channel key : context.getWorkers().keySet()) {
				MasterWorkerHolder worker = context.getWorkers().get(key);
				if (worker.getRunnings().containsKey(jobId)) {
					history.getLog().appendZeus("已经在运行中，无法再次运行");
					history.setStartTime(new Date());
					history.setEndTime(new Date());
					history.setStatus(com.taobao.zeus.model.JobStatus.Status.FAILED);
					break;
				}
			}
		}

		if (history.getStatus() == com.taobao.zeus.model.JobStatus.Status.RUNNING) {
			history.getLog().appendZeus(
					new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
							.format(new Date()) + " 进入任务队列");
			context.getJobHistoryManager().updateJobHistoryLog(history.getId(),
					history.getLog().getContent());
			if (history.getTriggerType() == TriggerType.MANUAL) {
				element.setJobID(history.getId());
				context.getManualQueue().offer(element);
			} else {
				JobStatus js = context.getJobManager().getJobStatus(
						history.getJobId());
				js.setStatus(com.taobao.zeus.model.JobStatus.Status.RUNNING);
				js.setHistoryId(history.getId());
				context.getJobManager().updateJobStatus(js);
				context.getQueue().offer(element);
			}
		}
		context.getJobHistoryManager().updateJobHistory(history);
		context.getJobHistoryManager().updateJobHistoryLog(history.getId(),
				history.getLog().getContent());
		return history;
	}
	
	//将定时任务生成action、以及没有依赖关系的周期任务生成action
	public void runScheduleJobToAction(List<JobTask> jobTaskDetails, Date now, SimpleDateFormat dfDate, Map<Long, JobTaskAction> actionDetails, String currentDateStr){
		for(JobTask jobTaskDetail : jobTaskDetails){
			//ScheduleType: 0 独立任务; 1依赖任务; 2周期任务
			if(jobTaskDetail.getScheduleType() != null && jobTaskDetail.getScheduleType()==0){
				try{
					String jobCronExpression = jobTaskDetail.getCronExpression();
					String cronDate= dfDate.format(now);
					List<String> lTime = new ArrayList<>();
					if(jobCronExpression != null && jobCronExpression.trim().length() > 0){
						//定时调度
						boolean isCronExp;
						try{
							isCronExp = CronExpParser.Parser(jobCronExpression, cronDate, lTime);
						}catch(Exception ex){
							isCronExp = false;
						}
						if (!isCronExp) {
							log.error("无法生成Cron表达式：日期," + cronDate + ";不符合规则cron表达式：" + jobCronExpression);
						}
                        for (String aLTime : lTime) {
                            String actionDateStr = ZeusDateTool.StringToDateStr(aLTime, "yyyy-MM-dd HH:mm:ss", "yyyyMMddHHmm");
                            String actionCronExpr = ZeusDateTool.StringToDateStr(aLTime, "yyyy-MM-dd HH:mm:ss", "0 m H d M") + " ?";

                            JobTaskAction actionPer = new JobTaskAction();
                            actionPer.setId(Long.parseLong(actionDateStr) * 1000000 + jobTaskDetail.getId());//update action id
                            actionPer.setJobId(jobTaskDetail.getId());
                            actionPer.setAuto(jobTaskDetail.getAuto());
                            actionPer.setConfigs(jobTaskDetail.getConfigs());
                            actionPer.setCronExpression(actionCronExpr);//update action cron expression
                            actionPer.setCycle(jobTaskDetail.getCycle());
                            String jobDependencies = jobTaskDetail.getDependencies();
                            actionPer.setDependencies(jobDependencies);
                            actionPer.setJobDependencies(jobDependencies);
                            actionPer.setDescr(jobTaskDetail.getDescr());
                            actionPer.setGmtCreate(jobTaskDetail.getGmtCreate());
                            actionPer.setGmtModified(new Date());
                            actionPer.setGroupId(jobTaskDetail.getGroupId());
                            actionPer.setHistoryId(jobTaskDetail.getHistoryId());
                            actionPer.setHost(jobTaskDetail.getHost());
                            actionPer.setHostGroupId(jobTaskDetail.getHostGroupId());
                            actionPer.setLastEndTime(jobTaskDetail.getLastEndTime());
                            actionPer.setLastResult(jobTaskDetail.getLastResult());
                            actionPer.setName(jobTaskDetail.getName());
                            actionPer.setOffset(jobTaskDetail.getOffset());
                            actionPer.setOwner(jobTaskDetail.getOwner());
                            actionPer.setPostProcessers(jobTaskDetail.getPostProcessers());
                            actionPer.setPreProcessers(jobTaskDetail.getPreProcessers());
                            actionPer.setReadyDependency(jobTaskDetail.getReadyDependency());
                            actionPer.setResources(jobTaskDetail.getResources());
                            actionPer.setRunType(jobTaskDetail.getRunType());
                            actionPer.setScheduleType(jobTaskDetail.getScheduleType());
                            /*							actionPer.setScript(jobTaskDetail.getScript());*/
                            actionPer.setStartTime(jobTaskDetail.getStartTime());
                            actionPer.setStartTimestamp(jobTaskDetail.getStartTimestamp());
                            actionPer.setStatisStartTime(jobTaskDetail.getStatisStartTime());
                            actionPer.setStatisEndTime(jobTaskDetail.getStatisEndTime());
                            actionPer.setStatus(jobTaskDetail.getStatus());
                            actionPer.setTimezone(jobTaskDetail.getTimezone());
                            try {
                                //System.out.println("定时任务JobId: " + jobTaskDetail.getId()+";  ActionId: " +actionPer.getId());
                                log.info("定时任务JobId: " + jobTaskDetail.getId() + ";  ActionId: " + actionPer.getId());
                                //if(actionPer.getId()>Long.parseLong(currentDateStr)){
                                context.getJobManager().saveJobAction(actionPer);
                                //	System.out.println("success");
                                log.info("success");
                                actionDetails.put(actionPer.getId(), actionPer);
                                //}
                            } catch (ZeusException e) {
                                //	System.out.println("failed");
                                log.error("定时任务JobId:" + jobTaskDetail.getId() + " 生成Action" + actionPer.getId() + "失败", e);
                            }
                        }
					}
				}catch(Exception ex){
					log.error("定时任务生成Action失败",ex);
				}
			}
			/**
			 * 取消生成周期任务
			if(jobTaskDetail.getScheduleType() != null && jobTaskDetail.getScheduleType()==2){
				try{
					if(jobTaskDetail.getDependencies()==null || jobTaskDetail.getDependencies().trim().length()==0){
						Date date = null;
						try {
							date = DateUtil.timestamp2Date(jobTaskDetail.getStartTimestamp(),
									DateUtil.getDefaultTZStr());
							//System.out.println(date);
						} catch (ParseException e) {
							date = new Date();
							log.error("parse job start timestamp to date failed,", e);
						}
						SimpleDateFormat dfTime=new SimpleDateFormat("HHmmss");
						SimpleDateFormat dfDate=new SimpleDateFormat("yyyyMMdd");
						SimpleDateFormat dfMinute=new SimpleDateFormat("mmss");
						String currentDate = dfDate.format(new Date());
						String startTime = dfTime.format(date);
						String startMinute = dfMinute.format(date);
						//天-周期调度
						if(jobTaskDetail.getCycle().equals("day")){
							Date newStartDate = ZeusDateTool.StringToDate(currentDate+startTime, "yyyyMMddHHmmss");
							Calendar calendar = Calendar.getInstance();
							calendar.setTimeInMillis(newStartDate.getTime());
							calendar.add(Calendar.DATE, -1);
							calendar.getTime();
							Date statisStartTime = calendar.getTime();
							calendar.add(Calendar.MINUTE, 60*23+59);
							Date statisEndTime = calendar.getTime();
							
							JobTaskAction actionPer = new JobTaskAction();
							actionPer.setId(Long.parseLong(currentDate+startTime)*10000+jobTaskDetail.getId());//update action id
							actionPer.setToJobId(jobTaskDetail.getId());
							actionPer.setAuto(jobTaskDetail.getAuto());
							actionPer.setConfigs(jobTaskDetail.getConfigs());
							actionPer.setCronExpression(jobTaskDetail.getCronExpression());
							actionPer.setCycle(jobTaskDetail.getCycle());
							String jobDependencies = jobTaskDetail.getDependencies();
							actionPer.setDependencies(jobDependencies);
							actionPer.setJobDependencies(jobDependencies);
							actionPer.setDescr(jobTaskDetail.getDescr());
							actionPer.setGmtCreate(jobTaskDetail.getGmtCreate());
							actionPer.setGmtModified(new Date());
							actionPer.setGroupId(jobTaskDetail.getGroupId());
							actionPer.setHistoryId(jobTaskDetail.getHistoryId());
							actionPer.setHost(jobTaskDetail.getHost());
							actionPer.setLastEndTime(jobTaskDetail.getLastEndTime());
							actionPer.setLastResult(jobTaskDetail.getLastResult());
							actionPer.setName(jobTaskDetail.getName());
							actionPer.setOffset(jobTaskDetail.getOffset());
							actionPer.setOwner(jobTaskDetail.getOwner());
							actionPer.setPostProcessors(jobTaskDetail.getPostProcessors());
							actionPer.setPreProcessors(jobTaskDetail.getPreProcessors());
							actionPer.setReadyDependency(jobTaskDetail.getReadyDependency());
							actionPer.setResources(jobTaskDetail.getResources());
							actionPer.setRunType(jobTaskDetail.getRunType());
							actionPer.setScheduleType(jobTaskDetail.getScheduleType());
							actionPer.setScript(jobTaskDetail.getScript());
							actionPer.setStartTime(newStartDate);
							actionPer.setStartTimestamp(newStartDate.getTime());
							actionPer.setStatisStartTime(statisStartTime);
							actionPer.setStatisEndTime(statisEndTime);
							actionPer.setStatus(jobTaskDetail.getStatus());
							actionPer.setHostGroupId(jobTaskDetail.getHostGroupId());
							actionPer.setTimezone(jobTaskDetail.getTimezone());
							try {
								//System.out.println("周期任务（天）JobId: " + jobTaskDetail.getId()+";  ActionId: " +actionPer.getId());
								//if(actionPer.getId()>Long.parseLong(currentDateStr)){
									context.getGroupManager().saveJob(actionPer);
									System.out.println("success");
									actionDetails.put(actionPer.getId(),actionPer);
								//}
								
							} catch (ZeusException e) {
								System.out.println("failed");
								log.error("周期任务（天）JobId:" + jobTaskDetail.getId() + " 生成Action" +actionPer.getId() + "失败", e);
							}
						}
						if(jobTaskDetail.getCycle().equals("hour")){
							for (int i = 0; i < 24; i++) {		
								String startHour = String.valueOf(i);
								if(startHour.trim().length()<2){
									startHour = "0"+startHour;
								}
								Date newStartDate = ZeusDateTool.StringToDate(currentDate+startHour+startMinute, "yyyyMMddHHmmss");
								Calendar calendar = Calendar.getInstance();
								calendar.setTimeInMillis(newStartDate.getTime());
								calendar.add(Calendar.HOUR, -1);
								calendar.getTime();
								Date statisStartTime = calendar.getTime();
								calendar.add(Calendar.MINUTE, 59);
								Date statisEndTime = calendar.getTime();
								
								JobTaskAction actionPer = new JobTaskAction();
								actionPer.setId(Long.parseLong(currentDate+startHour+startMinute)*10000+jobTaskDetail.getId());//update action id
								actionPer.setToJobId(jobTaskDetail.getId());
								actionPer.setAuto(jobTaskDetail.getAuto());
								actionPer.setConfigs(jobTaskDetail.getConfigs());
								actionPer.setCronExpression(jobTaskDetail.getCronExpression());
								actionPer.setCycle(jobTaskDetail.getCycle());
								String jobDependencies = jobTaskDetail.getDependencies();
								actionPer.setDependencies(jobDependencies);
								actionPer.setJobDependencies(jobDependencies);
								actionPer.setDescr(jobTaskDetail.getDescr());
								actionPer.setGmtCreate(jobTaskDetail.getGmtCreate());
								actionPer.setGmtModified(new Date());
								actionPer.setGroupId(jobTaskDetail.getGroupId());
								actionPer.setHistoryId(jobTaskDetail.getHistoryId());
								actionPer.setHost(jobTaskDetail.getHost());
								actionPer.setLastEndTime(jobTaskDetail.getLastEndTime());
								actionPer.setLastResult(jobTaskDetail.getLastResult());
								actionPer.setName(jobTaskDetail.getName());
								actionPer.setOffset(jobTaskDetail.getOffset());
								actionPer.setOwner(jobTaskDetail.getOwner());
								actionPer.setPostProcessors(jobTaskDetail.getPostProcessors());
								actionPer.setPreProcessors(jobTaskDetail.getPreProcessors());
								actionPer.setReadyDependency(jobTaskDetail.getReadyDependency());
								actionPer.setResources(jobTaskDetail.getResources());
								actionPer.setRunType(jobTaskDetail.getRunType());
								actionPer.setScheduleType(jobTaskDetail.getScheduleType());
								actionPer.setScript(jobTaskDetail.getScript());
								actionPer.setStartTime(newStartDate);
								actionPer.setStartTimestamp(newStartDate.getTime());
								actionPer.setStatisStartTime(statisStartTime);
								actionPer.setStatisEndTime(statisEndTime);
								actionPer.setStatus(jobTaskDetail.getStatus());
								actionPer.setTimezone(jobTaskDetail.getTimezone());
								actionPer.setHostGroupId(jobTaskDetail.getHostGroupId());
								try {
									System.out.println("周期任务（时）JobId: " + jobTaskDetail.getId()+";  ActionId: " +actionPer.getId());
									//if(actionPer.getId()>Long.parseLong(currentDateStr)){
										context.getGroupManager().saveJob(actionPer);
										System.out.println("success");
										actionDetails.put(actionPer.getId(),actionPer);
									//}
									
								} catch (ZeusException e) {
									System.out.println("failed");
									log.error("周期任务（时）JobId:" + jobTaskDetail.getId() + " 生成Action" +actionPer.getId() + "失败", e);
								}
							}
						}
					}
				}catch(Exception ex){
					log.error("周期任务生成Action失败",ex);
				}
			}*/
		}
	}
	
	//将依赖任务生成action
	public void runDependencesJobToAction(List<JobTask> jobTaskDetails, Map<Long, JobTaskAction> actionDetails, String currentDateStr, int loopCount){
		int noCompleteCount = 0;
		loopCount ++;
//		System.out.println("loopCount："+loopCount);
		for(JobTask jobTaskDetail : jobTaskDetails){
			//ScheduleType: 0 独立任务; 1依赖任务; 2周期任务
			if((jobTaskDetail.getScheduleType() != null && jobTaskDetail.getScheduleType()==1)
					|| (jobTaskDetail.getScheduleType() != null && jobTaskDetail.getScheduleType()==2)){
				try{
					String jobDependencies = jobTaskDetail.getDependencies();
					String actionDependencies;
					if(jobDependencies != null && jobDependencies.trim().length()>0){
	
						//计算这些依赖任务的版本数
						Map<String,List<JobTaskAction>> dependActionList = new HashMap<>();
						String[] dependStrs = jobDependencies.split(",");
						for(String deps : dependStrs){
							List<JobTaskAction> dependActions = new ArrayList<>();
                            for (JobTaskAction action : actionDetails.values()) {
                                if (action.getJobId().toString().equals(deps)) {
                                    dependActions.add(action);
                                }
                            }
							dependActionList.put(deps, dependActions);
							if(loopCount > 20){
								if(!jobTaskDetail.getConfigs().contains("sameday")){
									if(dependActionList.get(deps).size()==0){
										List<JobTaskAction> lastJobTaskActions = context.getJobManager().getLastJobAction(deps);
										if(lastJobTaskActions != null && lastJobTaskActions.size()>0){
											actionDetails.put(lastJobTaskActions.get(0).getId(), lastJobTaskActions.get(0));
											dependActions.add(lastJobTaskActions.get(0));
											dependActionList.put(deps, dependActions);
										}else{
											break;
										}
									}
								}
							}
						}
						//判断是否有未完成的
						boolean isComplete = true;
						String actionMostDeps = "";
						for(String deps : dependStrs){
							if(dependActionList.get(deps).size()==0){
								isComplete = false;
								noCompleteCount ++;
								break;
							}
							if(actionMostDeps.trim().length()==0){
								actionMostDeps = deps;
							}
							if(dependActionList.get(deps).size()>dependActionList.get(actionMostDeps).size()){
								actionMostDeps = deps;
							}else if(dependActionList.get(deps).size()==dependActionList.get(actionMostDeps).size()){
								if(dependActionList.get(deps).get(0).getId()<dependActionList.get(actionMostDeps).get(0).getId()){
									actionMostDeps = deps;
								}
							}
						}
						if(!isComplete){
							continue;
						}else{
							List<JobTaskAction> actions = dependActionList.get(actionMostDeps);
							if(actions != null && actions.size()>0){
								for(JobTaskAction actionModel : actions){
									actionDependencies = String.valueOf(actionModel.getId());
									for(String deps : dependStrs){
										if(!deps.equals(actionMostDeps)){
											List<JobTaskAction> actionOthers = dependActionList.get(deps);
											Long actionOtherId = actionOthers.get(0).getId();
											for(JobTaskAction actionOtherModel : actionOthers){
												if(Math.abs((actionOtherModel.getId()-actionModel.getId()))<Math.abs((actionOtherId-actionModel.getId()))){
													actionOtherId = actionOtherModel.getId();
												}
											}
											if(actionDependencies.trim().length()>0){
												actionDependencies += ",";
											}
											actionDependencies += String.valueOf((actionOtherId/1000000)*1000000 + Long.parseLong(deps));
										}
									}
									//保存多版本的action
									JobTaskAction actionPer = new JobTaskAction();
									actionPer.setId((actionModel.getId()/1000000)*1000000+ jobTaskDetail.getId());//update action id
									actionPer.setJobId(jobTaskDetail.getId());
									actionPer.setAuto(jobTaskDetail.getAuto());
									actionPer.setConfigs(jobTaskDetail.getConfigs());
									actionPer.setCronExpression(jobTaskDetail.getCronExpression());//update action cron expression
									actionPer.setCycle(jobTaskDetail.getCycle());
									actionPer.setDependencies(actionDependencies);
									actionPer.setJobDependencies(jobDependencies);
									actionPer.setDescr(jobTaskDetail.getDescr());
									actionPer.setGmtCreate(jobTaskDetail.getGmtCreate());
									actionPer.setGmtModified(new Date());
									actionPer.setGroupId(jobTaskDetail.getGroupId());
									actionPer.setHistoryId(jobTaskDetail.getHistoryId());
									actionPer.setHost(jobTaskDetail.getHost());
									actionPer.setHostGroupId(jobTaskDetail.getHostGroupId());
									actionPer.setLastEndTime(jobTaskDetail.getLastEndTime());
									actionPer.setLastResult(jobTaskDetail.getLastResult());
									actionPer.setName(jobTaskDetail.getName());
									actionPer.setOffset(jobTaskDetail.getOffset());
									actionPer.setOwner(jobTaskDetail.getOwner());
									actionPer.setPostProcessers(jobTaskDetail.getPostProcessers());
									actionPer.setPreProcessers(jobTaskDetail.getPreProcessers());
									actionPer.setReadyDependency(jobTaskDetail.getReadyDependency());
									actionPer.setResources(jobTaskDetail.getResources());
									actionPer.setRunType(jobTaskDetail.getRunType());
									actionPer.setScheduleType(jobTaskDetail.getScheduleType());
/*									actionPer.setScript(jobTaskDetail.getScript());*/
									actionPer.setStartTime(jobTaskDetail.getStartTime());
									actionPer.setStartTimestamp(jobTaskDetail.getStartTimestamp());
									actionPer.setStatisStartTime(jobTaskDetail.getStatisStartTime());
									actionPer.setStatisEndTime(jobTaskDetail.getStatisEndTime());
									actionPer.setStatus(jobTaskDetail.getStatus());
									actionPer.setTimezone(jobTaskDetail.getTimezone());
									try {
										if(!actionDetails.containsKey(actionPer.getId())){
											//System.out.println("依赖任务JobId: " + jobTaskDetail.getId()+";  ActionId: " +actionPer.getId());
											//log.info("依赖任务JobId: " + jobTaskDetail.getId()+";  ActionId: " +actionPer.getId());
											//if(actionPer.getId()>Long.parseLong(currentDateStr)){
												context.getJobManager().saveJobAction(actionPer);
												//System.out.println("success");
												//log.info("success");
											//}
											actionDetails.put(actionPer.getId(),actionPer);
										}
									} catch (ZeusException e) {
										log.error("依赖任务JobId:" + jobTaskDetail.getId() + " 生成Action" +actionPer.getId() + "失败", e);
									}
								}
							}
						}
					}
				}catch(Exception ex){
					log.error("依赖任务生成Action失败", ex);
				}
			}
		}

		if(noCompleteCount > 0 && loopCount < 40){
			runDependencesJobToAction(jobTaskDetails, actionDetails, currentDateStr, loopCount);
		}
	}
}
