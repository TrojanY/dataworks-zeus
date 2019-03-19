package com.taobao.zeus.socket.master;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.taobao.zeus.store.mysql.manager.*;
import io.netty.channel.Channel;
import java.util.concurrent.ConcurrentHashMap;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import com.taobao.zeus.model.HostGroupCache;
import com.taobao.zeus.mvc.Dispatcher;
import com.taobao.zeus.schedule.mvc.ScheduleInfoLog;

public class MasterContext {

	private static Logger log = LoggerFactory.getLogger(MasterContext.class);
	private Map<Channel, MasterWorkerHolder> workers=new ConcurrentHashMap<>();
	private ApplicationContext applicationContext;
	private Master master;
	private Scheduler scheduler;
	private Dispatcher dispatcher;
	private Map<String,HostGroupCache> hostGroupCache;
	//调度任务 jobId
//	private Queue<JobElement> queue=new ArrayBlockingQueue<JobElement>(10000);
	private Queue<JobElement> queue=new PriorityBlockingQueue<>(10000, (je1, je2) -> {
		int number_a = je1.getPriorityLevel();
		int number_b = je2.getPriorityLevel();
		return Integer.compare(number_b, number_a);
	});
	
	private Queue<JobElement> exceptionQueue = new LinkedBlockingQueue<>();
	
	
	//调试任务  debugId
	private Queue<JobElement> debugQueue=new ArrayBlockingQueue<>(1000);
	//手动任务  historyId
	private Queue<JobElement> manualQueue=new ArrayBlockingQueue<>(1000);
//	private Queue<JobElement> manualQueue=new PriorityBlockingQueue<JobElement>(1000, new Comparator<JobElement>() {
//		public int compare(JobElement je1, JobElement je2) {
//			int numbera = je1.getPriorityLevel();
//			int numberb = je2.getPriorityLevel();
//			if (numberb > numbera) {
//				return 1;
//			} else if (numberb < numbera) {
//				return -1;
//			} else {
//				return 0;
//			}
//		}
//	});
	private MasterHandler handler;
	private MasterServer server;
	private ExecutorService threadPool=Executors.newCachedThreadPool();
	private ScheduledExecutorService schedulePool=Executors.newScheduledThreadPool(12);
	
	public MasterContext(ApplicationContext applicationContext){
		this.applicationContext=applicationContext;
	}
	public void init(int port) throws InterruptedException {
		log.info("init begin");
		try {
			StdSchedulerFactory stdSchedulerFactory = new StdSchedulerFactory();
			stdSchedulerFactory.initialize("zeusQuartz.properties");
			scheduler = stdSchedulerFactory.getScheduler();
			scheduler.start();
		} catch (SchedulerException e) {
			ScheduleInfoLog.error("schedule start fail", e);
		}
		dispatcher=new Dispatcher();
		handler=new MasterHandler(this);
		server=new MasterServer(handler);
		server.start(port);
		master=new Master(this);
		log.info("init finish");
	}
	public void destory(){
		threadPool.shutdown();
		schedulePool.shutdown();
		if(server!=null){
			server.shutdown();
		}
		if(scheduler!=null){
			try {
				scheduler.shutdown();
			} catch (SchedulerException e) {
				e.printStackTrace();
			}
		}
		ScheduleInfoLog.info("destory finish");
	}
	
	public Map<Channel, MasterWorkerHolder> getWorkers() {
		return workers;
	}
	public void setWorkers(Map<Channel, MasterWorkerHolder> workers) {
		this.workers = workers;
	}
	public Scheduler getScheduler() {
		return scheduler;
	}
	public void setScheduler(Scheduler scheduler) {
		this.scheduler = scheduler;
	}
	public Dispatcher getDispatcher() {
		return dispatcher;
	}
	public void setDispatcher(Dispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}
	public HostGroupManager getHostGroupManager(){
		return (HostGroupManager) applicationContext.getBean("hostGroupManager");
	}
	public JobHistoryManager getJobHistoryManager() {
		return (JobHistoryManager) applicationContext.getBean("jobHistoryManager");
	}
	public DebugHistoryManager getDebugHistoryManager(){
		return (DebugHistoryManager)applicationContext.getBean("debugHistoryManager");
	}
	public FileManager getFileManager(){
		return (FileManager) applicationContext.getBean("fileManager");
	}
	public ProfileManager getProfileManager(){
		return (ProfileManager) applicationContext.getBean("profileManager");
	}
	public Queue<JobElement> getQueue() {
		return queue;
	}
	public void setQueue(Queue<JobElement> queue) {
		this.queue = queue;
	}
	public JobManager getJobManager() {
		return (JobManager) applicationContext.getBean("jobManager");
	}
	public GroupManager getGroupManager() {
		return (GroupManager) applicationContext.getBean("groupManager");
	}
	public MasterHandler getHandler() {
		return handler;
	}
	public void setHandler(MasterHandler handler) {
		this.handler = handler;
	}
	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}
	public MasterServer getServer() {
		return server;
	}
	public void setServer(MasterServer server) {
		this.server = server;
	}
	public ExecutorService getThreadPool() {
		return threadPool;
	}
	public Master getMaster() {
		return master;
	}
	public void setMaster(Master master) {
		this.master = master;
	}
	public ScheduledExecutorService getSchedulePool() {
		return schedulePool;
	}
	public Queue<JobElement> getDebugQueue() {
		return debugQueue;
	}
	public void setDebugQueue(Queue<JobElement> debugQueue) {
		this.debugQueue = debugQueue;
	}
	public Queue<JobElement> getManualQueue() {
		return manualQueue;
	}
	
	public synchronized void refreshHostGroupCache(){
		try {
			hostGroupCache = getHostGroupManager().getAllHostGroupInfomations();
		} catch (Exception e) {
			ScheduleInfoLog.error("refresh hostgroupcache error", e);
		}
		
	}
	public synchronized Map<String,HostGroupCache> getHostGroupCache() {
		return hostGroupCache;
	}
	public Queue<JobElement> getExceptionQueue() {
		return exceptionQueue;
	}
}
