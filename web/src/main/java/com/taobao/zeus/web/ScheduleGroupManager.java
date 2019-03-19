package com.taobao.zeus.web;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.taobao.zeus.store.mysql.manager.GroupManager;
import com.taobao.zeus.store.mysql.manager.JobManager;
import com.taobao.zeus.store.mysql.persistence.JobTask;
import com.taobao.zeus.store.mysql.persistence.ZeusWorker;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import com.taobao.zeus.client.ZeusException;
import com.taobao.zeus.model.GroupDescriptor;
import com.taobao.zeus.model.JobStatus;
import com.taobao.zeus.socket.worker.ClientWorker;
import com.taobao.zeus.util.Tuple;
/**
 * 在操作数据库的同时，向调度系统发出更新命令，保证调度系统的数据是最新的
 * 此类主要是给Web界面使用
 * @author zhoufang
 *
 */
public class ScheduleGroupManager implements GroupManager, JobManager {

	private static Logger log=LogManager.getLogger(ScheduleGroupManager.class);
	private GroupManager groupManager;
	public void setGroupManager(GroupManager groupManager) {
		this.groupManager = groupManager;
	}

	@Autowired
	private ClientWorker worker;
	@Override
	public GroupDescriptor createGroup(String user, String groupName,
			String parentGroup, boolean isDirectory) throws ZeusException {
		return groupManager.createGroup(user, groupName, parentGroup, isDirectory);
	}

	@Override
	public JobDescriptorOld createJob(String user, String jobName,
			String parentGroup, JobRunTypeOld jobType) throws ZeusException {
		JobDescriptorOld jd=groupManager.createJob(user, jobName, parentGroup, jobType);
//		try {
//			zeusWorker.updateJobFromWeb(jd.getId());
//		} catch (Exception e) {
//			String msg="创建Job成功，但是调度Job失败";
//			log.error(msg,e);
//			throw new ZeusException(msg,e);
//		}
		return jd;
	}

	@Override
	public void deleteGroup(String user, String groupId) throws ZeusException {
		groupManager.deleteGroup(user, groupId);
	}

	@Override
	public void deleteJob(String user, String jobId) throws ZeusException {
		groupManager.deleteJob(user, jobId);
//		try {
//			zeusWorker.updateJobFromWeb(jobId);
//		} catch (Exception e) {
//			String msg="删除Job成功，但是调度Job失败";
//			log.error(msg,e);
//			throw new ZeusException(msg, e);
//		}
	}

	@Override
	public GroupBeanOld getDownstreamGroupBean(String groupId) {
		return groupManager.getDownstreamGroupBean(groupId);
	}

	@Override
	public GroupBeanOld getGlobeGroupBean() {
		return groupManager.getGlobeGroupBean();
	}

	@Override
	public GroupDescriptor getGroupDescriptor(String groupId) {
		return groupManager.getGroupDescriptor(groupId);
	}

	@Override
	public Tuple<JobDescriptorOld, JobStatus> getJobDescriptor(String jobId) {
		return groupManager.getJobDescriptor(jobId);
	}


	@Override
	public String getRootGroupId() {
		return groupManager.getRootGroupId();
	}

	@Override
	public GroupBeanOld getUpstreamGroupBean(String groupId) {
		return groupManager.getUpstreamGroupBean(groupId);
	}

	@Override
	public JobBeanOld getUpstreamJobBean(String jobId) {
		return groupManager.getUpstreamJobBean(jobId);
	}

	@Override
	public void updateGroup(String user, GroupDescriptor group)
			throws ZeusException {
		groupManager.updateGroup(user, group);
	}

	@Override
	public void updateJob(String user, JobDescriptorOld job) throws ZeusException {
		groupManager.updateJob(user, job);
		groupManager.updateActionList(job);
		try {
			worker.updateJobFromWeb(job.getId());
		} catch (Exception e) {
			String msg="更新Job成功，但是调度Job失败";
			log.error(msg,e);
			throw new ZeusException(msg,e);
		}
	}

	@Override
	public Map<String, Tuple<JobDescriptorOld, JobStatus>> getJobDescriptor(Collection<String> jobIds) {
		return groupManager.getJobDescriptor(jobIds);
	}

	@Override
	public void updateJobStatus(JobStatus jobStatus){
		throw new UnsupportedOperationException("ScheduleJobManager 不支持此操作");
	}

	@Override
	public JobStatus getJobStatus(String jobId) {
		return groupManager.getJobStatus(jobId);
	}

	@Override
	public void grantGroupOwner(String granter, String uid, String groupId)
			throws ZeusException {
		groupManager.grantGroupOwner(granter, uid, groupId);
	}

	@Override
	public void grantJobOwner(String granter, String uid, String jobId)
			throws ZeusException {
		groupManager.grantJobOwner(granter, uid, jobId);
	}

	@Override
	public List<GroupDescriptor> getChildrenGroup(String groupId) {
		return groupManager.getChildrenGroup(groupId);
	}

	@Override
	public List<Tuple<JobDescriptorOld, JobStatus>> getChildrenJob(String groupId) {
		return groupManager.getChildrenJob(groupId);
	}

	@Override
	public GroupBeanOld getDownstreamGroupBean(GroupBeanOld parent) {
		return groupManager.getDownstreamGroupBean(parent);
	}

	@Override
	public void moveJob(String uid, String jobId, String groupId)
			throws ZeusException {
		groupManager.moveJob(uid, jobId, groupId);
	}

	@Override
	public void moveGroup(String uid, String groupId, String newParentGroupId)
			throws ZeusException {
		groupManager.moveGroup(uid, groupId, newParentGroupId);
	}

	@Override
	public List<String> getHosts() throws ZeusException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void replaceWorker(ZeusWorker zeusWorker) throws ZeusException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeWorker(String host) throws ZeusException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public List<JobTask> getAllJobs() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getAllDependencied(String jobID) {
		// TODO Auto-generated method stub
		return groupManager.getAllDependencied(jobID);
	}

	@Override
	public List<String> getAllDependencies(String jobID) {
		// TODO Auto-generated method stub
		return groupManager.getAllDependencies(jobID);
	}

	@Override
	public void updateActionList(JobDescriptorOld job) {
		groupManager.updateActionList(job);
	}
	
	
	


}
