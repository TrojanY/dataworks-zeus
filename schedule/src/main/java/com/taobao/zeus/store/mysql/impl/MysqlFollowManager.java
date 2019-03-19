package com.taobao.zeus.store.mysql.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import com.taobao.zeus.model.ZeusFollow;
import com.taobao.zeus.store.GroupBean;
import com.taobao.zeus.store.JobBean;
import com.taobao.zeus.store.mysql.manager.FollowManager;
import com.taobao.zeus.store.mysql.manager.GroupManager;
import com.taobao.zeus.store.mysql.manager.JobManager;
import org.hibernate.query.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;

import com.taobao.zeus.store.mysql.persistence.GroupJobFollow;
import com.taobao.zeus.store.mysql.tool.PersistenceAndBeanConvert;

import javax.transaction.Transactional;

@SuppressWarnings("unchecked")
@Transactional
public class MysqlFollowManager extends HibernateDaoSupport implements FollowManager {

	
	@Override
	public List<ZeusFollow> findAllTypeFollows(final String uid) {
		assert getHibernateTemplate() != null;
		List<GroupJobFollow> list= getHibernateTemplate()
				.execute(session -> {
					Query query=session.createQuery("from com.taobao.zeus.store.mysql.persistence.ZeusFollowPersistence where uid=:uid");
					query.setParameter("uid", uid);
					return query.list();
				});
		
		List<ZeusFollow> result=new ArrayList<>();
		if(list!=null){
			for(GroupJobFollow persist:list){
				result.add(PersistenceAndBeanConvert.convert(persist));
			}
		}
		return result;
	}

	@Override
	public List<ZeusFollow> findFollowedGroups(final String uid) {
		assert getHibernateTemplate() != null;
		List<GroupJobFollow> list=getHibernateTemplate()
				.execute(session -> {
			Query query=session.createQuery("from com.taobao.zeus.store.mysql.persistence.ZeusFollowPersistence where type=:jobType and uid=:uid");
			query.setParameter("uid", uid);
			query.setParameter("jobType", ZeusFollow.JobType);
			return query.list();
		});
		List<ZeusFollow> result=new ArrayList<>();
		if(list!=null){
			for(GroupJobFollow persist:list){
				result.add(PersistenceAndBeanConvert.convert(persist));
			}
		}
		return result;
	}

	@Override
	public List<ZeusFollow> findFollowedJobs(final String uid) {
		assert getHibernateTemplate() != null;
		List<GroupJobFollow> list=getHibernateTemplate()
				.execute(session -> {
					Query query=session.createQuery("from com.taobao.zeus.store.mysql.persistence.ZeusFollowPersistence where type=:jobType and uid=:uid");
					query.setParameter("jobType", ZeusFollow.JobType);
					query.setParameter("uid", uid);
					return query.list();
				});
		List<ZeusFollow> result=new ArrayList<>();
		if(list!=null){
			for(GroupJobFollow persist:list){
				result.add(PersistenceAndBeanConvert.convert(persist));
			}
		}
		return result;
	}

	@Override
	public List<ZeusFollow> findJobFollowers(final String jobId) {
		assert getHibernateTemplate() != null;
		List<GroupJobFollow> list=  getHibernateTemplate()
				.execute(session -> {
			Query query=session.createQuery("from com.taobao.zeus.store.mysql.persistence.ZeusFollowPersistence where type=:jobType and targetId=:jobId");
			query.setParameter("jobId", Long.valueOf(jobId));
			query.setParameter("jobType", com.taobao.zeus.model.ZeusFollow.JobType);
			return query.list();
		});
		List<ZeusFollow> result=new ArrayList<>();
		if(list!=null){
			for(GroupJobFollow persist:list){
				result.add(PersistenceAndBeanConvert.convert(persist));
			}
		}
		return result;
	}

	@Override
	public List<ZeusFollow> findGroupFollowers(final List<String> groupIds) {
		assert getHibernateTemplate() != null;
		List<GroupJobFollow> list= getHibernateTemplate()
				.execute(session -> {
					if(groupIds.isEmpty()){
						return Collections.emptyList();
					}
					List<Long> ids=new ArrayList<>();
					for(String group:groupIds){
						ids.add(Long.valueOf(group));
					}
					Query query=session.createQuery("from com.taobao.zeus.store.mysql.persistence.ZeusFollowPersistence where type=:jobType and targetId in (:list)");
					query.setParameterList("list", ids);
					query.setParameter("jobType", com.taobao.zeus.model.ZeusFollow.JobType);
					return query.list();
				});
		List<ZeusFollow> result=new ArrayList<>();
		if(list!=null){
			for(GroupJobFollow persist:list){
				result.add(PersistenceAndBeanConvert.convert(persist));
			}
		}
		return result;
	}

	@Override
	public ZeusFollow addFollow(final String uid, final Integer type, final String targetId) {
		assert getHibernateTemplate() != null;
		List<GroupJobFollow> list=getHibernateTemplate()
				.execute(session -> {
					Query query=session.createQuery("from com.taobao.zeus.store.mysql.persistence.ZeusFollowPersistence where uid=:uid and type=:jobType and targetId=:targetId");
					query.setParameter("uid", uid);
					query.setParameter("jobType", type);
					query.setParameter("targetId", Long.valueOf(targetId));
					return query.list();
				});
		if(list!=null && !list.isEmpty()){
			return PersistenceAndBeanConvert.convert(list.get(0));
		}
		GroupJobFollow persist=new GroupJobFollow();
		persist.setGmtCreate(new Date());
		persist.setGmtModified(new Date());
		persist.setTargetId(Long.valueOf(targetId));
		persist.setType(type);
		persist.setUid(uid);
		persist.setImportant(0);
		getHibernateTemplate().save(persist);
		
		return PersistenceAndBeanConvert.convert(persist);
	}

	@Override
	public void deleteFollow(final String uid, final Integer type, final String targetId) {
		assert getHibernateTemplate() != null;
		List<GroupJobFollow> list=getHibernateTemplate()
				.execute(session -> {
					Query query=session.createQuery("delete com.taobao.zeus.store.mysql.persistence.GroupJobFollow where uid=:uid and type=:jobType and targetId=:targetId");
					query.setParameter("uid", uid);
					query.setParameter("jobType", type);
					query.setParameter("targetId", Long.valueOf(targetId));
					return query.list();
				});
		if(list!=null && !list.isEmpty()){
			for(GroupJobFollow persist:list){
				getHibernateTemplate().delete(persist);
			}
		}
	}

	@Autowired
	@Qualifier("jobManager")
	private JobManager jobManager;
	
	@Override
	public List<String> findActualJobFollowers(String jobId) {
		List<ZeusFollow> jobFollows=findJobFollowers(jobId);
		GroupBean gb= jobManager.getUpstreamGroupBean(jobId);
		
		List<String> groupIds=new ArrayList<>();
		while(gb!=null){
			groupIds.add(gb.getGroupDescriptor().getId());
			gb=gb.getParentGroupBean();
		}
		List<ZeusFollow> groupFollows=findGroupFollowers(groupIds);
		
		List<String> follows=new ArrayList<>();
		//任务创建人自动纳入消息通知人员名单
		follows.add(jobManager.getJobDescriptor(jobId).getX().getOwner());
		for(com.taobao.zeus.model.ZeusFollow zf:jobFollows){
			if(!follows.contains(zf.getUid())){
				follows.add(zf.getUid());
			}
		}
		for(com.taobao.zeus.model.ZeusFollow zf:groupFollows){
			if(!follows.contains(zf.getUid())){
				follows.add(zf.getUid());
			}
		}
		return follows;
	}

	@Override
	public List<ZeusFollow> findAllFollowers(String jobId) {
		List<ZeusFollow> jobFollows=findJobFollowers(jobId);
		GroupBean gb= jobManager.getUpstreamGroupBean(jobId);
		
		List<String> groupIds=new ArrayList<>();
		while(gb!=null){
			groupIds.add(gb.getGroupDescriptor().getId());
			gb=gb.getParentGroupBean();
		}
		List<ZeusFollow> groupFollows=findGroupFollowers(groupIds);
		
		List<ZeusFollow> result = new ArrayList<>();
		result.addAll(jobFollows);
		result.addAll(groupFollows);
		return result;
	}

	public void updateImportantContact(final String targetId,final String uid, int isImportant) {
		assert getHibernateTemplate() != null;
		List<GroupJobFollow> list=getHibernateTemplate()
				.execute(session -> {
			Query query=session.createQuery("from com.taobao.zeus.store.mysql.persistence.ZeusFollowPersistence where uid=:uid and type=:jobType and targetId=:targetId");
			query.setParameter("uid", uid);
			query.setParameter("jobType", com.taobao.zeus.model.ZeusFollow.JobType );
			query.setParameter("targetId", Long.valueOf(targetId));
			return query.list();
		});
		if(list!=null && !list.isEmpty()){
			GroupJobFollow persist=list.get(0);
			persist.setImportant(isImportant);
			persist.setGmtModified(new Date());
			getHibernateTemplate().saveOrUpdate(persist);
		}
	}

	@Override
	public void grantImportantContact(String targetId, String uid) {
		updateImportantContact(targetId, uid, 1);
	}

	@Override
	public void revokeImportantContact(String targetId, String uid) {
		updateImportantContact(targetId, uid, 0);
	}	
}
