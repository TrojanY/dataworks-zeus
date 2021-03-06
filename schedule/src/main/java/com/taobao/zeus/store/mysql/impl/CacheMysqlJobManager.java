package com.taobao.zeus.store.mysql.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.zeus.store.mysql.manager.GroupManager;
import com.taobao.zeus.store.mysql.manager.JobManager;
import com.taobao.zeus.store.mysql.persistence.JobTask;
import com.taobao.zeus.store.mysql.persistence.JobTaskAction;
import com.taobao.zeus.store.mysql.persistence.JobGroup;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;

import com.taobao.zeus.client.ZeusException;
import com.taobao.zeus.model.GroupDescriptor;
import com.taobao.zeus.model.JobDescriptor;
import com.taobao.zeus.model.JobDescriptor.JobRunType;
import com.taobao.zeus.model.JobStatus;
import com.taobao.zeus.store.GroupBean;
import com.taobao.zeus.store.mysql.tool.GroupManagerTool;
import com.taobao.zeus.store.JobBean;
import com.taobao.zeus.store.mysql.persistence.ZeusWorker;
import com.taobao.zeus.store.mysql.tool.Judge;
import com.taobao.zeus.store.mysql.tool.PersistenceAndBeanConvert;
import com.taobao.zeus.util.Tuple;

import javax.transaction.Transactional;

@Transactional
public class CacheMysqlJobManager extends HibernateDaoSupport implements JobManager, GroupManager {
	private Judge jobjudge=new Judge();
	private Judge groupjudge=new Judge();
	private Map<String, JobTaskAction> cacheJobMap=new HashMap<>();
	private Map<String, JobGroup> cacheGroupMap=new HashMap<>();
	
	private JobManager jobManager;
	private GroupManager groupManager;
	private Map<String, JobTaskAction> getCacheJobs(){
		assert getHibernateTemplate() != null;
		Judge realTime=getHibernateTemplate()
				.execute(session -> {
					Object[] o=(Object[]) session.createSQLQuery("select count(*),max(id),max(gmt_modified) from zeus_action").uniqueResult();
					if(o!=null){
						Judge j=new Judge();
						j.count=((Number)o[0]).intValue();
						j.maxId=((Number)o[1]).intValue();
						j.lastModified=(Date) o[2];
						j.stamp=new Date();
						return j;
					}
					return null;
				});
		
		if(realTime!=null && realTime.count.equals(jobjudge.count)
                && realTime.maxId.equals(jobjudge.maxId)
                && realTime.lastModified.equals(jobjudge.lastModified)){
			jobjudge.stamp=new Date();
			return cacheJobMap;
		}else{
			List<JobTaskAction> list=getHibernateTemplate().loadAll(JobTaskAction.class);
			Map<String, JobTaskAction> newMap=new HashMap<>();
			for(JobTaskAction p:list){
				newMap.put(p.getId().toString(), p);
			}
			cacheJobMap=newMap;
			jobjudge=realTime;
			return cacheJobMap;
		}
	}
	private Map<String, JobGroup> getCacheGroups(){
		Judge realTime=getHibernateTemplate()
				.execute(session -> {
					Object[] o=(Object[]) session.createSQLQuery("select count(*),max(id),max(gmt_modified) from zeus_group").uniqueResult();
					if(o!=null){
						Judge j=new Judge();
						j.count=((Number) o[0]).intValue();
						j.maxId=((Number)o[1]).intValue();
						j.lastModified=(Date) o[2];
						j.stamp=new Date();
						return j;
					}
					return null;
				});
		if(realTime!=null && realTime.count.equals(groupjudge.count) && realTime.maxId.equals(groupjudge.maxId) && realTime.lastModified.equals(groupjudge.lastModified)){
			groupjudge.stamp=new Date();
			return cacheGroupMap;
		}else{
			List<JobGroup> list=getHibernateTemplate().loadAll(JobGroup.class);
			Map<String, JobGroup> newMap=new HashMap<>();
			for(JobGroup p:list){
				newMap.put(p.getId().toString(),p);
			}
			cacheGroupMap=newMap;
			groupjudge=realTime;
			return cacheGroupMap;
		}
	}
	
	@Override
	public GroupDescriptor createGroup(String user, String groupName,
			String parentGroup, boolean isDirectory) throws ZeusException {
		return groupManager.createGroup(user, groupName, parentGroup, isDirectory);
	}

	@Override
	public JobDescriptor createJob(String user, String jobName,
			String parentGroup, JobRunType jobType) throws ZeusException {
		return jobManager.createJob(user, jobName, parentGroup, jobType);
	}

	@Override
	public void deleteGroup(String user, String groupId) throws ZeusException {
		groupManager.deleteGroup(user, groupId);
	}

	@Override
	public void deleteJob(String user, String jobId) throws ZeusException {
		jobManager.deleteJob(user, jobId);
	}

	@Override
	public List<GroupDescriptor> getChildrenGroup(String groupId) {
		List<GroupDescriptor> list=new ArrayList<>();
		Map<String, JobGroup> map=getCacheGroups();
		for(JobGroup p:map.values()){
			if(p.getParent()!=null && p.getParent().toString().equals(groupId)){
				list.add(PersistenceAndBeanConvert.convert(p));
			}
		}
		return list;
	}

	@Override
	public List<Tuple<JobDescriptor, JobStatus>> getChildrenJob(String groupId) {
		List<Tuple<JobDescriptor, JobStatus>> list=new ArrayList<>();
		Map<String, JobTaskAction> map=getCacheJobs();
		for(JobTaskAction p:map.values()){
			if(p.getGroupId().toString().equals(groupId)){
				list.add(PersistenceAndBeanConvert.convert(p));
			}
		}
		return list;
	}

	@Override
	public GroupBean getDownstreamGroupBean(String groupId) {
		GroupDescriptor group=getGroupDescriptor(groupId);
		GroupBean result=new GroupBean(group);
		return getDownstreamGroupBean(result);
	}

	@Override
	public GroupBean getDownstreamGroupBean(GroupBean parent) {
		if(parent.isDirectory()){
			List<GroupDescriptor> children=getChildrenGroup(parent.getGroupDescriptor().getId());
			for(GroupDescriptor child:children){
				GroupBean childBean=new GroupBean(child);
				getDownstreamGroupBean(childBean);
				childBean.setParentGroupBean(parent);
				parent.getChildrenGroupBeans().add(childBean);
			}
		}else{
			List<Tuple<JobDescriptor, JobStatus>> jobs=getChildrenJob(parent.getGroupDescriptor().getId());
			for(Tuple<JobDescriptor, JobStatus> tuple:jobs){
				JobBean jobBean=new JobBean(tuple.getX(),tuple.getY());
				jobBean.setGroupBean(parent);
				parent.getJobBeans().put(tuple.getX().getJobId(), jobBean);
			}
		}
		
		return parent;
	}

	@Override
	public GroupBean getGlobeGroupBean() {
		return GroupManagerTool.buildGlobeGroupBean(this);
	}

	@Override
	public GroupDescriptor getGroupDescriptor(String groupId) {
		Map<String, JobGroup> map=getCacheGroups();
		return PersistenceAndBeanConvert.convert(map.get(groupId));
	}

	@Override
	public Tuple<JobDescriptor, JobStatus> getJobDescriptor(String jobId) {
		return PersistenceAndBeanConvert.convert(getCacheJobs().get(jobId));
	}

	@Override
	public Tuple<JobDescriptor, JobStatus> getJobActionDescriptor(String jobId) {
		return null;
	}

	@Override
	public Map<String, Tuple<JobDescriptor, JobStatus>> getJobDescriptor(
			Collection<String> jobIds) {
		Map<String, JobTaskAction> map=getCacheJobs();
		Map<String, Tuple<JobDescriptor, JobStatus>> result=new HashMap<>();
		for(String id:jobIds){
			result.put(id,PersistenceAndBeanConvert.convert(map.get(id)));
		}
		return result;
	}

	@Override
	public JobStatus getJobStatus(String jobId) {
		Tuple<JobDescriptor, JobStatus> job=PersistenceAndBeanConvert.convert(getCacheJobs().get(jobId));
		if(job==null){
			return null;
		}
		return job.getY();
	}

	@Override
	public String getRootGroupId() {
		return groupManager.getRootGroupId();
	}

	@Override
	public GroupBean getUpstreamGroupBean(String groupId) {
		return GroupManagerTool.getUpstreamGroupBean(groupId, this);
	}

	@Override
	public JobBean getUpstreamJobBean(String jobId) {
		return GroupManagerTool.getUpstreamJobBean(jobId,jobManager,groupManager);
	}

	@Override
	public void grantGroupOwner(String granter, String uid, String groupId)
			throws ZeusException {
		groupManager.grantGroupOwner(granter, uid, groupId);
	}

	@Override
	public void grantJobOwner(String granter, String uid, String jobId)
			throws ZeusException {
		jobManager.grantJobOwner(granter, uid, jobId);
	}

	@Override
	public void updateGroup(String user, GroupDescriptor group)
			throws ZeusException {
		groupManager.updateGroup(user, group);
	}

	@Override
	public void updateJob(String user, JobDescriptor job) throws ZeusException {
		jobManager.updateJob(user, job);
	}

	@Override
	public void updateJobStatus(JobStatus jobStatus) {
		jobManager.updateJobStatus(jobStatus);
	}
	public void setJobManager(JobManager jobManager) {
		this.jobManager = jobManager;
	}
	@Override
	public void moveJob(String uid, String jobId, String groupId)
			throws ZeusException {
		jobManager.moveJob(uid, jobId, groupId);
	}
	@Override
	public void moveGroup(String uid, String groupId, String newParentGroupId)
			throws ZeusException {
		groupManager.moveGroup(uid, groupId, newParentGroupId);
	}
	@Override
	public List<String> getHosts() throws ZeusException {
		return Collections.emptyList();
	}
	@Override
	public void replaceWorker(ZeusWorker zeusWorker) throws ZeusException {
		// TODO Auto-generated method stub
        throw new ZeusException(new UnsupportedOperationException());
	}
	@Override
	public void removeWorker(String host) throws ZeusException {
		// TODO Auto-generated method stub
        throw new ZeusException(new UnsupportedOperationException());
	}
	
	@Override
	public void saveJobAction(JobTaskAction actionPer) throws ZeusException {
		// TODO Auto-generated method stub
        throw new ZeusException(new UnsupportedOperationException());
	}
	
	@Override
	public List<JobTaskAction> getLastJobAction(String jobId){
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void updateJobActionList(JobDescriptor job) {

	}

	@Override
	public List<String> getDownstreamDependencies(String jobID) {
		return null;
	}

	@Override
	public List<String> getUpstreamDependencies(String jobID) {
		return null;
	}

	@Override
	public List<JobTask> getAllJobs() {
		return null;
	}

	@Override
	public List<Tuple<JobDescriptor, JobStatus>> getJobActionDescriptors(String jobId) {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void removeJob(Long actionId) throws ZeusException {
		// TODO Auto-generated method stub
        throw new ZeusException(new UnsupportedOperationException());
	}
	@Override
	public boolean IsExistedBelowRootGroup(String GroupName) {
		throw new UnsupportedOperationException();
	}

}
