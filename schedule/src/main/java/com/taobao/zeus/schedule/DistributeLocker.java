package com.taobao.zeus.schedule;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.hibernate.query.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;

import com.taobao.zeus.schedule.mvc.ScheduleInfoLog;
import com.taobao.zeus.socket.worker.ClientWorker;
import com.taobao.zeus.store.HostGroupManager;
import com.taobao.zeus.store.mysql.persistence.DistributeLock;
import com.taobao.zeus.util.Environment;

/**
 * 分布式服务器的检测器
 * 每隔一分钟查询一次数据库的zeus_lock表
 * @author zhoufang
 *
 */
public class DistributeLocker extends HibernateDaoSupport{

	private static Logger log=LogManager.getLogger(DistributeLocker.class);
	
	public static String host=UUID.randomUUID().toString();
	@Autowired
	private HostGroupManager hostGroupManager;
	@Autowired
	private ApplicationContext applicationContext;
	@Autowired
	private ClientWorker worker;
	
	private ZeusSchedule zeusSchedule;
	
	private int port=9887;
	
	static{
		try {
			host=InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			//ignore
		}
	}
	
	public DistributeLocker(String port){
		try {
			this.port=Integer.valueOf(port);
		} catch (NumberFormatException e) {
			log.error("port must be a number", e);
		}
	}
	
	public void init(){
		zeusSchedule=new ZeusSchedule(applicationContext);
		ScheduledExecutorService service=Executors.newScheduledThreadPool(3);
		service.scheduleAtFixedRate(() -> {
			try {
				update();
			} catch (Exception e) {
				log.error(e);
			}
		}, 20, 60, TimeUnit.SECONDS);
	}
	/**
	 * 定时扫描任务
	 * 每隔一分钟扫描一次zeus_lock表
	 * 判断ScheduleServer是否正常运行
	 * @author zhoufang
	 *
	 */
	private void update(){
		DistributeLock lock=getHibernateTemplate().execute(session -> {
			Query query=session.createQuery("from com.taobao.zeus.store.mysql.persistence.DistributeLock where subgroup=? order by id desc");
			query.setParameter(0, Environment.getScheduleGroup());
			query.setMaxResults(1);
			DistributeLock lock1 = (DistributeLock) query.uniqueResult();
			if(lock1 ==null){
				lock1 =new DistributeLock();
				lock1.setHost(host);
				lock1.setServerUpdate(new Date());
				lock1.setSubgroup(Environment.getScheduleGroup());
				session.save(lock1);
				lock1 =(DistributeLock) query.uniqueResult();
			}
			return lock1;
		});
		

		try {
			if(host.equals(lock.getHost())){
				lock.setServerUpdate(new Date());
				getHibernateTemplate().update(lock);
				log.info("hold the locker and update time");
				zeusSchedule.startup(port);
			}else{//其他服务器抢占了锁
				log.info("not my locker");
				//如果最近更新时间在5分钟以上，则认为抢占的Master服务器已经失去连接，属于抢占组的服务器主动进行抢占
				if(System.currentTimeMillis()-lock.getServerUpdate().getTime()>1000*60*5L && isPreemptionHost()){
					lock.setHost(host);
					lock.setServerUpdate(new Date());
					lock.setSubgroup(Environment.getScheduleGroup());
					getHibernateTemplate().update(lock);
					log.error("rob the locker and update");
					zeusSchedule.startup(port);
				}else{//如果Master服务器没有问题，本服务器停止server角色
					zeusSchedule.shutdown();
				}
			}
			worker.connect(lock.getHost(),port);
		} catch (Exception e) {
			ScheduleInfoLog.error("start up worker fail", e);
		}
	}
	//判断该host是否属于抢占组
	public boolean isPreemptionHost(){
		List<String> preemptionhosts = hostGroupManager.getPreemptionHost();
		if (preemptionhosts.contains(host)) {
			return true;
		}else {
			ScheduleInfoLog.info(host + " is not in master gourp: " + preemptionhosts.toString());
			return false;
		}
	}
	
}
