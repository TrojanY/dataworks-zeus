package com.taobao.zeus.web;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.taobao.zeus.model.processor.JobProcessor;
import com.taobao.zeus.model.processor.Processor;
import com.taobao.zeus.store.*;
import com.taobao.zeus.store.mysql.manager.PermissionManager;
import com.taobao.zeus.store.mysql.manager.UserManager;
import com.taobao.zeus.store.mysql.persistence.JobTaskAction;
import com.taobao.zeus.store.mysql.persistence.ZeusUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.taobao.zeus.client.ZeusException;
import com.taobao.zeus.model.GroupDescriptor;
import com.taobao.zeus.model.JobDescriptor;
import com.taobao.zeus.model.JobDescriptor.JobRunType;
import com.taobao.zeus.model.JobStatus;
import com.taobao.zeus.store.mysql.manager.JobManager;
import com.taobao.zeus.store.mysql.persistence.ZeusWorker;
import com.taobao.zeus.util.Tuple;
/**
 * 权限验证，需要的操作权限验证不通过，将抛出异常
 * @author zhoufang
 *
 */
public class PermissionJobManager implements JobManager {

	private JobManager jobManager;
	public void setJobManager(JobManager jobManager) {
		this.jobManager = jobManager;
	}
	@Autowired
	@Qualifier("permissionManager")
	private PermissionManager permissionManager;
	@Autowired
	@Qualifier("userManager")
	private UserManager userManager;
	
	private Boolean isGroupOwner(String uid,GroupBean gb){
		List<String> owners=new ArrayList<>();
		while(gb!=null){
			if(!owners.contains(gb.getGroupDescriptor().getOwner())){
				owners.add(gb.getGroupDescriptor().getOwner());
			}
			gb=gb.getParentGroupBean();
		}
		if(owners.contains(uid)){
			return true;
		}
		return false;
	}
	private Boolean isGroupOwner(String uid,String groupId){
		return isGroupOwner(uid, jobManager.getUpstreamGroupBean(groupId));
	}
	private Boolean isJobOwner(String uid,String jobId){
		JobBean jb= jobManager.getUpstreamJobBean(jobId);
		if(jb.getJobDescriptor().getOwner().equalsIgnoreCase(uid)){
			return true;
		}
		return isGroupOwner(uid, jb.getGroupBean());
	}
	
	public Boolean hasGroupPermission(String uid,String groupId){
		if(isGroupOwner(uid, groupId)){
			return true;
		}
		return permissionManager.hasGroupPermission(uid, groupId);
	}
	public Boolean hasJobPermission(String uid,String jobId){
		if(isJobOwner(uid, jobId)){
			return true;
		}
		return permissionManager.hasJobPermission(uid, jobId);
	}
	@Override
	public GroupDescriptor createGroup(String user, String groupName,
			String parentGroup, boolean isDirectory) throws ZeusException {
		if(hasGroupPermission(user, parentGroup)){
			return jobManager.createGroup(user, groupName, parentGroup, isDirectory);
		}else{
			throw new ZeusException("您无权操作");
		}
	}

	@Override
	public JobDescriptor createJob(String user, String jobName,
			String parentGroup, JobRunType jobType) throws ZeusException {
		if(hasGroupPermission(user, parentGroup)){
			return jobManager.createJob(user, jobName, parentGroup, jobType);
		}else{
			throw new ZeusException("您无权操作");
		}
	}

	@Override
	public void deleteGroup(String user, String groupId) throws ZeusException {
		if(hasGroupPermission(user, groupId)){
			GroupDescriptor gd= jobManager.getGroupDescriptor(groupId);
			if(gd!=null && gd.getOwner().equals(user)){
				jobManager.deleteGroup(user, groupId);
			}
		}else{
			throw new ZeusException("您无权操作");
		}
		
	}

	@Override
	public void deleteJob(String user, String jobId) throws ZeusException {
		if(hasJobPermission(user, jobId)){
			Tuple<JobDescriptor, JobStatus> job= jobManager.getJobDescriptor(jobId);
			if(job!=null){
				jobManager.deleteJob(user, jobId);
			}
		}else{
			throw new ZeusException("没有删除的权限");
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
	public Tuple<JobDescriptor,JobStatus> getJobDescriptor(String jobId) {
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
		if(hasGroupPermission(user, group.getId())){
			GroupDescriptor gd= jobManager.getGroupDescriptor(group.getId());
			if(gd!=null){
				jobManager.updateGroup(user, group);
			}
		}else{
			throw new ZeusException("没有更新的权限");
		}
		
	}

	@Override
	public void updateJob(String user, JobDescriptor job) throws ZeusException {
		if(hasJobPermission(user, job.getId())){
			Tuple<JobDescriptor, JobStatus> old= jobManager.getJobDescriptor(job.getId());
			if(old!=null ){
				List<JobProcessor> hasadd=new ArrayList<>();
				for(Processor p:old.getX().getPreProcessors()){
					if(p instanceof JobProcessor){
						hasadd.add((JobProcessor)p);
					}
				}
				for(Processor p:old.getX().getPostProcessors()){
					if(p instanceof JobProcessor){
						hasadd.add((JobProcessor)p);
					}
				}
				List<JobProcessor> thistime=new ArrayList<>();
				for(Processor p:job.getPreProcessors()){
					if(p instanceof JobProcessor){
						thistime.add((JobProcessor)p);
					}
				}
				for(Processor p:job.getPostProcessors()){
					if(p instanceof JobProcessor){
						thistime.add((JobProcessor)p);
					}
				}
				for(JobProcessor jp:thistime){
					if(jp.getJobId().equals(job.getId())){
						throw new ZeusException("不得将自身设置为自身的处理器");
					}
					boolean exist=false;
					for(JobProcessor jp2:hasadd){
						if(jp2.getId().equalsIgnoreCase(jp.getId())){
							exist=true;
							break;
						}
					}
					if(!exist && !hasJobPermission(user, jp.getJobId())){
						throw new ZeusException("您没有权限将Job："+jp.getJobId() +" 添加到处理单元中");
					}
				}
				jobManager.updateJob(user, job);
			}
		}else{
			throw new ZeusException("没有更新的权限");
		}
		
	}

	@Override
	public Map<String, Tuple<JobDescriptor,JobStatus>> getJobDescriptor(Collection<String> jobIds) {
		return jobManager.getJobDescriptor(jobIds);
	}
	@Override
	public void updateJobStatus(JobStatus jobStatus) {
		throw new UnsupportedOperationException("PermissionJobManager 不支持此操作");
	}
	@Override
	public JobStatus getJobStatus(String jobId) {
		return jobManager.getJobStatus(jobId);
	}
	@Override
	public void grantGroupOwner(String granter, String uid, String groupId) throws ZeusException{
		GroupBean gb= jobManager.getUpstreamGroupBean(groupId);
		List<String> owners=new ArrayList<>();
		while(gb!=null){
			if(!owners.contains(gb.getGroupDescriptor().getOwner())){
				owners.add(gb.getGroupDescriptor().getOwner());
			}
			gb=gb.getParentGroupBean();
		}
		if(owners.contains(granter)){
			jobManager.grantGroupOwner(granter, uid, groupId);
		}else{
			throw new ZeusException("您无权操作");
		}
	}
	@Override
	public void grantJobOwner(String granter, String uid, String jobId) throws ZeusException{
		JobBean jb= jobManager.getUpstreamJobBean(jobId);
		List<String> owners=new ArrayList<>();
		owners.add(jb.getJobDescriptor().getOwner());
		GroupBean gb=jb.getGroupBean();
		while(gb!=null){
			if(!owners.contains(gb.getGroupDescriptor().getOwner())){
				owners.add(gb.getGroupDescriptor().getOwner());
			}
			gb=gb.getParentGroupBean();
		}
		if(owners.contains(granter)){
			jobManager.grantJobOwner(granter, uid, jobId);
		}else{
			throw new ZeusException("您无权操作");
		}
		
	}
	public void addGroupAdmin(String granter,String user, String groupId) throws ZeusException {
		if(isGroupOwner(granter, groupId)){
			permissionManager.addGroupAdmin(user, groupId);
		}else{
			throw new ZeusException("您无权操作");
		}
	}
	public void addJobAdmin(String granter,String user, String jobId) throws ZeusException {
		if(isJobOwner(granter, jobId)){
			permissionManager.addJobAdmin(user, jobId);
		}else{
			throw new ZeusException("您无权操作");
		}
	}
	public void removeGroupAdmin(String granter,String user, String groupId) throws ZeusException {
		if(isGroupOwner(granter, groupId)){
			permissionManager.removeGroupAdmin(user, groupId);
		}else{
			throw new ZeusException("您无权操作");
		}
	}
	public void removeJobAdmin(String granter,String user, String jobId) throws ZeusException {
		if(isJobOwner(granter, jobId)){
			permissionManager.removeJobAdmin(user, jobId);
		}else{
			throw new ZeusException("您无权操作");
		}
	}
	public List<ZeusUser> getGroupAdmins(String groupId) {
		return userManager.findListByUid(permissionManager.getGroupAdmins(groupId));
	}
	public List<ZeusUser> getJobAdmins(String jobId) {
		return userManager.findListByUid(permissionManager.getJobAdmins(jobId));
	}
	public List<Long> getJobACtion(String jobId) {
		return permissionManager.getJobACtion(jobId);
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
		if(!permissionManager.hasGroupPermission(uid, groupId) || !permissionManager.hasJobPermission(uid, jobId)){
			throw new ZeusException("您无权操作");
		}
		jobManager.moveJob(uid, jobId, groupId);
	}
	@Override
	public void moveGroup(String uid, String groupId, String newParentGroupId)
			throws ZeusException {
		if(!permissionManager.hasGroupPermission(uid, groupId) ||
				!permissionManager.hasGroupPermission(uid, newParentGroupId)){
			throw new ZeusException("您无权操作");
		}
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
