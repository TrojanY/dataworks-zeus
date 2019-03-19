package com.taobao.zeus.broadcast.alarm;

import java.util.ArrayList;
import java.util.List;

import com.taobao.zeus.store.mysql.manager.FollowManager;
import com.taobao.zeus.store.mysql.manager.JobManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.taobao.zeus.model.JobHistory;
import com.taobao.zeus.model.JobStatus.TriggerType;
import com.taobao.zeus.model.ZeusFollow;
import com.taobao.zeus.schedule.mvc.JobFailListener.ChainException;
import com.taobao.zeus.store.mysql.manager.JobHistoryManager;

public abstract class AbstractZeusAlarm implements ZeusAlarm{
	protected static Logger log=LoggerFactory.getLogger(AbstractZeusAlarm.class);
	@Autowired
	protected JobHistoryManager jobHistoryManager;
	@Autowired
	@Qualifier("followManager")
	protected FollowManager followManager;
	@Autowired
	@Qualifier("jobManager")
	protected JobManager jobManager;
/*	
	@Override
	public void alarm(String historyId, String title, String content,ChainException chain)
			throws Exception {
		JobTaskHistory history=jobHistoryManager.findJobHistory(historyId);
		TriggerType type=history.getTriggerType();
		//获得action_id
		String jobId=history.getJobId();
		//获得job_id
		String tojobId=history.getToJobId();
		List<String> users=new ArrayList<String>();
		if(type==TriggerType.SCHEDULE){
			users=followManagerOld.findActualJobFollowers(tojobId);
		}else{
			users.add(groupManager.getJobDescriptor(tojobId).getX().getOwner());
			if(history.getOperator()!=null){
				if(!users.contains(history.getOperator())){
					users.add(history.getOperator());
				}
			}
		}
		List<String> result=new ArrayList<String>();
		if(chain==null){
			result=users;
		}else{
			for(String uid:users){
				Integer count=chain.getUserCountMap().get(uid);
				if(count==null){
					count=1;
					chain.getUserCountMap().put(uid, count);
				}
				if(count<20){//一个job失败，最多发给同一个人20个报警
					chain.getUserCountMap().put(uid, ++count);
					result.add(uid);
				}
			}
		}
		alarm(jobId, result, title, content);
	}
*/
	@Override
	public void alarm(String historyId, String title, String content,ChainException chain)
			throws Exception {
		JobHistory history=jobHistoryManager.findJobHistory(historyId);
		TriggerType type=history.getTriggerType();

		//获得job_id
		String jobId=history.getJobId();
		List<String> users=new ArrayList<>();
		if(type==TriggerType.SCHEDULE){
			List<ZeusFollow> zeusFollowers = followManager.findAllFollowers(jobId);
			List<ZeusFollow> importantContacts = new ArrayList<>();
			List<ZeusFollow> otherFollowers = new ArrayList<>();
			for(ZeusFollow zf : zeusFollowers){
				if (zf.isImportant() && ZeusFollow.JobType.equals(zf.getType())) {
					importantContacts.add(zf);
				}else {
					otherFollowers.add(zf);
				}
			}
			String owner = jobManager.getJobDescriptor(jobId).getX().getOwner();
			
			//首先添加重要联系人，然后是job本身的owner，最后是关注者。
			for(ZeusFollow person : importantContacts){
				if (!users.contains(person.getUid())) {
					users.add(person.getUid());
				}
			}
			if (!users.contains(owner)) {
				users.add(owner);
			}
			for (ZeusFollow other : otherFollowers) {
				if (!users.contains(other.getUid())) {
					users.add(other.getUid());
				}
			}
		}else{
			users.add(jobManager.getJobDescriptor(jobId).getX().getOwner());
			if(history.getOperator()!=null){
				if(!users.contains(history.getOperator())){
					users.add(history.getOperator());
				}
			}
		}
		List<String> result=new ArrayList<>();
		if(chain==null){
			result=users;
		}else{
			for(String uid:users){
				Integer count = chain.getUserCountMap().computeIfAbsent(uid, k -> 1);
				if(count<20){//一个job失败，最多发给同一个人20个报警
					chain.getUserCountMap().put(uid, ++count);
					result.add(uid);
				}
			}
		}
		alarm(jobId, result, title, content);
	}
	
	@Override
	public void alarm(String historyId, String title, String content)
			throws Exception {
		alarm(historyId, title, content, null);
	}
	/**
	 * @param jobId anction_id
	 * @param users 用户域账号id
	 * @param title
	 * @param content
	 * @throws Exception
	 */
	public abstract void alarm(String jobId, List<String> users,String title,String content) throws Exception;

}