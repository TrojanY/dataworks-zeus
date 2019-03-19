package com.taobao.zeus.web;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.taobao.zeus.store.mysql.manager.JobManager;
import com.taobao.zeus.store.mysql.persistence.JobTaskAction;
import com.taobao.zeus.store.mysql.persistence.ZeusWorker;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import com.taobao.zeus.client.ZeusException;
import com.taobao.zeus.model.GroupDescriptor;
import com.taobao.zeus.model.JobDescriptor;
import com.taobao.zeus.model.JobDescriptor.JobRunType;
import com.taobao.zeus.model.JobStatus;
import com.taobao.zeus.socket.worker.ClientWorker;
import com.taobao.zeus.store.GroupBean;
import com.taobao.zeus.store.JobBean;
import com.taobao.zeus.util.Tuple;
/**
 * 在操作数据库的同时，向调度系统发出更新命令，保证调度系统的数据是最新的
 * 此类主要是给Web界面使用
 * @author zhoufang
 *
 */
public class ScheduleJobManager implements JobManager {

	private static Logger log=LogManager.getLogger(ScheduleJobManager.class);
	private JobManager jobManager;
	public void setJobManager(JobManager jobManager) {
		this.jobManager = jobManager;
	}

	@Autowired
	private ClientWorker worker;
	@Override
	public GroupDescriptor createGroup(String user, String groupName,
			String parentGroup, boolean isDirectory) throws ZeusException {
		return jobManager.createGroup(user, groupName, parentGroup, isDirectory);
	}

	@Override
	public JobDescriptor createJob(String user, String jobName,
			String parentGroup, JobRunType jobType) throws ZeusException {
		JobDescriptor jd= jobManager.createJob(user, jobName, parentGroup, jobType);
		try {
			worker.updateJobFromWeb(jd.getId());
		} catch (Exception e) {
			String msg="创建Job成功，但是调度Job失败";
			log.error(msg,e);
			throw new ZeusException(msg,e);
		}
		return jd;
	}

	@Override
	public void deleteGroup(String user, String groupId) throws ZeusException {
		jobManager.deleteGroup(user, groupId);
	}

	@Override
	public void deleteJob(String user, String jobId) throws ZeusException {
		jobManager.deleteJob(user, jobId);
		try {
			worker.updateJobFromWeb(jobId);
		} catch (Exception e) {
			String msg="删除Job成功，但是调度Job失败";
			log.error(msg,e);
			throw new ZeusException(msg, e);
		}
	}

	@Override
	public GroupBean getDownstreamGroupBean(String groupId) {
		return jobManager.getDownstreamGroupBean(groupId);
	}

	@Override
	public GroupBean getGlobeGroupBean() {
		return jobManager.getGlobeGroupBean();
	}

	@Override
	public GroupDescriptor getGroupDescriptor(String groupId) {
		return jobManager.getGroupDescriptor(groupId);
	}

	@Override
	public Tuple<JobDescriptor, JobStatus> getJobDescriptor(String jobId) {
		return jobManager.getJobDescriptor(jobId);
	}


	@Override
	public String getRootGroupId() {
		return jobManager.getRootGroupId();
	}

	@Override
	public GroupBean getUpstreamGroupBean(String groupId) {
		return jobManager.getUpstreamGroupBean(groupId);
	}

	@Override
	public JobBean getUpstreamJobBean(String jobId) {
		return jobManager.getUpstreamJobBean(jobId);
	}

	@Override
	public void updateGroup(String user, GroupDescriptor group)
			throws ZeusException {
		jobManager.updateGroup(user, group);
	}

	@Override
	public void updateJob(String user, JobDescriptor job) throws ZeusException {
		jobManager.updateJob(user, job);
		try {
			worker.updateJobFromWeb(job.getId());
		} catch (Exception e) {
			String msg="更新Job成功，但是调度Job失败";
			log.error(msg,e);
			throw new ZeusException(msg,e);
		}
	}

	@Override
	public Map<String, Tuple<JobDescriptor, JobStatus>> getJobDescriptor(Collection<String> jobIds) {
		return jobManager.getJobDescriptor(jobIds);
	}

	@Override
	public void updateJobStatus(JobStatus jobStatus){
		throw new UnsupportedOperationException("ScheduleJobManager 不支持此操作");
	}

	@Override
	public JobStatus getJobStatus(String jobId) {
		return jobManager.getJobStatus(jobId);
	}

	@Override
	public void grantGroupOwner(String granter, String uid, String groupId)
			throws ZeusException {
		jobManager.grantGroupOwner(granter, uid, groupId);
	}

	@Override
	public void grantJobOwner(String granter, String uid, String jobId)
			throws ZeusException {
		jobManager.grantJobOwner(granter, uid, jobId);
	}

	@Override
	public List<GroupDescriptor> getChildrenGroup(String groupId) {
		return jobManager.getChildrenGroup(groupId);
	}

	@Override
	public List<Tuple<JobDescriptor, JobStatus>> getChildrenJob(String groupId) {
		return jobManager.getChildrenJob(groupId);
	}

	@Override
	public GroupBean getDownstreamGroupBean(GroupBean parent) {
		return jobManager.getDownstreamGroupBean(parent);
	}

	@Override
	public void moveJob(String uid, String jobId, String groupId)
			throws ZeusException {
		jobManager.moveJob(uid, jobId, groupId);
	}

	@Override
	public void moveGroup(String uid, String groupId, String newParentGroupId)
			throws ZeusException {
		jobManager.moveGroup(uid, groupId, newParentGroupId);
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
	public void saveJob(JobTaskAction actionPer) throws ZeusException {
		jobManager.saveJob(actionPer);
	}

	@Override
	public List<JobTaskAction> getLastJobAction(String jobId) {
		return jobManager.getLastJobAction(jobId);
	}

	@Override
	public void updateAction(JobDescriptor actionPer) throws ZeusException {
		jobManager.updateAction(actionPer);
	}

	@Override
	public List<Tuple<JobDescriptor, JobStatus>> getActionList(String jobId) {
		return jobManager.getActionList(jobId);
	}
	
	@Override
	public void removeJob(Long actionId) throws ZeusException {
		jobManager.removeJob(actionId);
	}

	@Override
	public boolean IsExistedBelowRootGroup(String GroupName) {
		return jobManager.IsExistedBelowRootGroup(GroupName);
	}
}
