package com.taobao.zeus.store.mysql.impl;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.taobao.zeus.store.*;
import com.taobao.zeus.store.mysql.manager.JobManager;
import com.taobao.zeus.store.mysql.manager.PermissionManager;
import com.taobao.zeus.store.mysql.persistence.ZeusPermission;
import org.hibernate.query.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;
import com.taobao.zeus.store.mysql.manager.GroupManager;

import javax.transaction.Transactional;

@SuppressWarnings("unchecked")
@Transactional
public class MysqlPermissionManager extends HibernateDaoSupport implements PermissionManager {
	@Autowired
	@Qualifier("groupManager")
	private GroupManager groupManager;
	
	@Autowired
	@Qualifier("jobManager")
	private JobManager jobManager;
	
	@Override
	public Boolean hasGroupPermission(final String user, final String groupId) {
		if(Super.getSupers().contains(user)){
			//超级管理员
			return true;
		}
		Set<String> groups=new HashSet<>();
		GroupBean gb=groupManager.getUpstreamGroupBean(groupId);
		if(user.equals(gb.getGroupDescriptor().getOwner())){
			//组所有人
			return true;
		}
		while(gb!=null){
			groups.add(gb.getGroupDescriptor().getId());
			gb=gb.getParentGroupBean();
		}
		Set<String> users=new HashSet<>();
		for(String g:groups){
			users.addAll(getGroupAdmins(g));
		}
		return users.contains(user);
	}
	
	public List<String> getGroupAdmins(final String groupId){
		assert getHibernateTemplate() != null;
		return getHibernateTemplate().execute(session -> {
			Query query=session.createQuery("select uid from com.taobao.zeus.store.mysql.persistence.ZeusPermission where type=:groupType and targetId=:groupId");
			query.setParameter("groupType", ZeusPermission.GROUP_TYPE);
			query.setParameter("groupId", Long.valueOf(groupId));
			return query.list();
		});
	}
	public List<String> getJobAdmins(final String jobId){
		assert getHibernateTemplate() != null;
		return getHibernateTemplate().execute(session -> {
			Query query=session.createQuery("select uid from com.taobao.zeus.store.mysql.persistence.ZeusPermission where type=:jobType and targetId=:jobId");
			query.setParameter("jobType", ZeusPermission.JOB_TYPE);
			query.setParameter("jobId", Long.valueOf(jobId));
			return query.list();
		});
	}
	public List<Long> getJobACtion(final String jobId){
		assert getHibernateTemplate() != null;
		return getHibernateTemplate().execute(session -> {

			Query query=session.createQuery("select id from com.taobao.zeus.store.mysql.persistence.JobTaskAction where toJobId=:jobId order by id desc");
			query.setParameter("jobId",jobId);
			return  query.list();
		});
	}
	private ZeusPermission getGroupPermission(final String user, final String groupId){
		assert getHibernateTemplate() != null;
		List<ZeusPermission> list=getHibernateTemplate().execute(session -> {
			Query query=session.createQuery("from com.taobao.zeus.store.mysql.persistence.PermissionPersistence where type=:groupType and uid=:uid and targetId=:groupId");
			query.setParameter("groupType", ZeusPermission.GROUP_TYPE);
			query.setParameter("uid", user);
			query.setParameter("groupId", Long.valueOf(groupId));
			return query.list();
		});
		if(list!=null && !list.isEmpty()){
			return list.get(0);
		}
		return null;
	}
	private ZeusPermission getJobPermission(final String user, final String jobId){
		assert getHibernateTemplate() != null;
		List<ZeusPermission> list=getHibernateTemplate().execute(session -> {
			Query query=session.createQuery("from com.taobao.zeus.store.mysql.persistence.PermissionPersistence where type=:jobType and uid=:uid and targetId=:jobId");
			query.setParameter("jobType", ZeusPermission.JOB_TYPE);
			query.setParameter("uid", user);
			query.setParameter("jobId", Long.valueOf(jobId));
			return query.list();
		});
		if(list!=null && !list.isEmpty()){
			return list.get(0);
		}
		return null;
	}
	@Override
	public void addGroupAdmin(String user,String groupId) {
		boolean has= getGroupPermission(user, groupId) != null;
		if(!has){
			ZeusPermission pp=new ZeusPermission();
			pp.setType(ZeusPermission.GROUP_TYPE);
			pp.setUid(user);
			pp.setTargetId(Long.valueOf(groupId));
			pp.setGmtModified(new Date());
			assert getHibernateTemplate() != null;
			getHibernateTemplate().save(pp);
		}
	}
	@Override
	public void addJobAdmin(String user, String jobId) {
		boolean has= getJobPermission(user, jobId) != null;
		if(!has){
			ZeusPermission pp=new ZeusPermission();
			pp.setType(ZeusPermission.JOB_TYPE);
			pp.setUid(user);
			pp.setTargetId(Long.valueOf(jobId));
			pp.setGmtModified(new Date());
			assert getHibernateTemplate() != null;
			getHibernateTemplate().save(pp);
		}
	}
	@Override
	public Boolean hasJobPermission(String user, String jobId) {
		if(Super.getSupers().contains(user)){
			//超级管理员
			return true;
		}
		Set<String> groups=new HashSet<>();
		JobBean jobBean=jobManager.getUpstreamJobBean(jobId);
		if(user.equals(jobBean.getJobDescriptor().getOwner())){
			//任务所有人
			return true;
		}
		GroupBean gb=jobBean.getGroupBean();
		while(gb!=null){
			groups.add(gb.getGroupDescriptor().getId());
			gb=gb.getParentGroupBean();
		}
		Set<String> users = new HashSet<>(getJobAdmins(jobId));
		for(String g:groups){
			users.addAll(getGroupAdmins(g));
		}
		return users.contains(user)?true:hasGroupPermission(user, jobManager.getJobDescriptor(jobId).getX().getGroupId());
	}
	
	@Override
	public Boolean hasActionPermission(String user, String jobId) {
		if(Super.getSupers().contains(user)){
			//超级管理员
			return true;
		}
		Set<String> groups=new HashSet<>();
		JobBean jobBean= jobManager.getUpstreamJobBean(jobId);
		if(user.equals(jobBean.getJobDescriptor().getOwner())){
			//任务所有人
			return true;
		}
		GroupBean gb=jobBean.getGroupBean();
		while(gb!=null){
			groups.add(gb.getGroupDescriptor().getId());
			gb=gb.getParentGroupBean();
		}
		Set<String> users = new HashSet<>(getJobAdmins(jobBean.getJobDescriptor().getJobId()));
		for(String g:groups){
			users.addAll(getGroupAdmins(g));
		}
		return users.contains(user)?true:hasGroupPermission(user, jobManager.getJobDescriptor(jobId).getX().getGroupId());
	}
	
	@Override
	public void removeGroupAdmin(String user, String groupId) {
		ZeusPermission pp=getGroupPermission(user, groupId);
		if(pp!=null){
			assert getHibernateTemplate() != null;
			getHibernateTemplate().delete(pp);
		}
	}
	@Override
	public void removeJobAdmin(String user, String jobId) {
		ZeusPermission pp=getJobPermission(user, jobId);
		if(pp!=null){
			assert getHibernateTemplate() != null;
			getHibernateTemplate().delete(pp);
		}
	}

}