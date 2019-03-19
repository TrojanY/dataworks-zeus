package com.taobao.zeus.store.mysql.impl;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.taobao.zeus.store.mysql.persistence.JobTaskHistory;
import org.hibernate.query.Query;
import org.hibernate.query.NativeQuery;
import org.springframework.orm.hibernate5.HibernateCallback;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;

import com.taobao.zeus.store.mysql.manager.JobHistoryManager;
import com.taobao.zeus.store.mysql.tool.PersistenceAndBeanConvert;

import javax.transaction.Transactional;

@SuppressWarnings("unchecked")
@Transactional
public class MysqlJobHistoryManager extends HibernateDaoSupport implements JobHistoryManager{

	@Override
	public void updateJobHistoryLog(final String id, final String log) {
		assert getHibernateTemplate() != null;
		getHibernateTemplate().execute(session -> {
			Query query=session.createQuery("update com.taobao.zeus.store.mysql.persistence.JobTaskHistory set log=:log where id=:id");
			query.setParameter("log", log);
			query.setParameter("id", Long.valueOf(id));
			query.executeUpdate();
			return null;
		});
	}

	@Override
	public void updateJobHistory(com.taobao.zeus.model.JobHistory history) {
		assert getHibernateTemplate() != null;
		JobTaskHistory org=getHibernateTemplate().get(JobTaskHistory.class, Long.valueOf(history.getId()));
		
		JobTaskHistory persist=PersistenceAndBeanConvert.convert(history);
		persist.setGmtModified(new Date());
		assert org != null;
		persist.setGmtCreate(org.getGmtCreate());
		persist.setLog(org.getLog());
		getHibernateTemplate().update(persist);
	}

	@Override
	public com.taobao.zeus.model.JobHistory addJobHistory(com.taobao.zeus.model.JobHistory history) {
		JobTaskHistory persist=PersistenceAndBeanConvert.convert(history);
		persist.setGmtCreate(new Date());
		persist.setGmtModified(new Date());
		assert getHibernateTemplate() != null;
		Long id=(Long)getHibernateTemplate().save(persist);
		history.setId(id.toString()); 
		return history;
	}
	
	@Override
	public List<com.taobao.zeus.model.JobHistory> pagingList(final String jobId, final int start, final int limit) {
		assert getHibernateTemplate() != null;
		return getHibernateTemplate().execute(session -> {
			NativeQuery query=session.createSQLQuery("select id,action_id,job_id,start_time,end_time,execute_host,status,trigger_type,illustrate,operator,properties,statis_end_time,timezone,cycle from zeus_action_history" +
					" where job_id=:jobId or action_id=:actionId order by id desc");
			query.setParameter("jobId", Long.valueOf(jobId));
			query.setParameter("actionId", Long.valueOf(jobId));
			query.setMaxResults(limit);
			query.setFirstResult(start);
			List<Object[]> list=query.list();
			List<com.taobao.zeus.model.JobHistory> result=new ArrayList<>();
			for(Object[] o:list){
				JobTaskHistory p=new JobTaskHistory();
				p.setId(((Number)o[0]).longValue());
				p.setJobId(((Number)o[1]).longValue());
				p.setActionId(((Number)o[2]).longValue());
				p.setStartTime((Date)o[3]);
				p.setEndTime((Date)o[4]);
				p.setExecuteHost((String)o[5]);
				p.setStatus((String)o[6]);
				p.setTriggerType(o[7]==null?null:((Number)o[7]).intValue());
				p.setIllustrate((String)o[8]);
				p.setOperator((String)o[9]);
				p.setProperties((String)o[10]);
				p.setStatisEndTime(o[11]==null?null:(Date)o[11]);
				p.setTimezone((String)o[12]);
				p.setCycle((String)o[13]);
				result.add(PersistenceAndBeanConvert.convert(p));
			}
			return result;
		});
	}

	@Override
	public int pagingTotal(String jobId) {
		assert getHibernateTemplate() != null;
		Number number=(Number) getHibernateTemplate().execute(
				session -> {
					Query query=session.createQuery("select count(*) from com.taobao.zeus.store.mysql.persistence.JobTaskHistory where toJobId=:jobId");
					query.setParameter("jobId",jobId);
					return query.list().iterator().next();
				});
		return number.intValue();
	}

	@Override
	public com.taobao.zeus.model.JobHistory findJobHistory(String id) {
		assert getHibernateTemplate() != null;
		JobTaskHistory persist= getHibernateTemplate().get(JobTaskHistory.class, Long.valueOf(id));
		return PersistenceAndBeanConvert.convert(persist);
	}
 
	@Override
	public Map<String, com.taobao.zeus.model.JobHistory> findLastHistoryByList(final List<String> jobIds) {
		if(jobIds.isEmpty()){
			return Collections.emptyMap();
		}
		final List<Long> ids=(List<Long>) getHibernateTemplate().execute((HibernateCallback) session -> {
			String sql="select max(id) as m_id,action_id  from zeus_action_history where action_id in (:idList) group by action_id desc";
			NativeQuery query=session.createSQLQuery(sql);
			query.setParameterList("idList", jobIds);
			List<Object[]> list= query.list();
			List<Long> ids1 =new ArrayList<>();
			for(Object[] o:list){
				ids1.add(((Number)o[0]).longValue());
			}
			return ids1;
		});
		List<com.taobao.zeus.model.JobHistory> list=(List<com.taobao.zeus.model.JobHistory>) getHibernateTemplate().execute((HibernateCallback) session -> {
			if(ids==null || ids.isEmpty()){
				return Collections.emptyList();
			}
			String sql="select id,action_id,job_id,start_time,end_time,execute_host,status,trigger_type,illustrate,operator,properties from zeus_action_history where id in (:ids)";
			NativeQuery query=session.createSQLQuery(sql);
			query.setParameterList("ids", ids);
			List<Object[]> actionHistorys = query.list();
			List<com.taobao.zeus.model.JobHistory> result=new ArrayList<>();
			for(Object[] o: actionHistorys){
				JobTaskHistory p=new JobTaskHistory();
				p.setId(((Number)o[0]).longValue());
				p.setJobId(((Number)o[1]).longValue());
				p.setActionId(((Number)o[2]).longValue());
				p.setStartTime((Date)o[3]);
				p.setEndTime((Date)o[4]);
				p.setExecuteHost((String)o[5]);
				p.setStatus((String)o[6]);
				p.setTriggerType((Integer)o[7]);
				p.setIllustrate((String)o[8]);
				p.setOperator((String)o[9]);
				p.setProperties((String)o[10]);
				result.add(PersistenceAndBeanConvert.convert(p));
			}
			return result;
		});
		
		
		Map<String, com.taobao.zeus.model.JobHistory> map=new HashMap<>();
		assert list != null;
		for(com.taobao.zeus.model.JobHistory p:list){
			map.put(p.getJobId(),p);
		}
		return map;
	}

	@Override
	public List<com.taobao.zeus.model.JobHistory> findRecentRunningHistory() {
		return (List<com.taobao.zeus.model.JobHistory>) getHibernateTemplate().execute((HibernateCallback) session -> {
			String sql="select id,action_id,job_id,start_time,end_time,execute_host,status,trigger_type,illustrate,operator,properties from zeus_action_history where start_time>:start_time";
			NativeQuery query=session.createSQLQuery(sql);
			Calendar cal=Calendar.getInstance();
			cal.add(Calendar.DAY_OF_YEAR, -1);
			query.setParameter("start_time", cal.getTime());
			List<Object[]> list= query.list();
			List<com.taobao.zeus.model.JobHistory> result=new ArrayList<>();
			for(Object[] o:list){
				JobTaskHistory p=new JobTaskHistory();
				p.setId(((Number)o[0]).longValue());
				p.setJobId(((Number)o[1]).longValue());
				p.setActionId(((Number)o[2]).longValue());
				p.setStartTime((Date)o[3]);
				p.setEndTime((Date)o[4]);
				p.setExecuteHost((String)o[5]);
				p.setStatus((String)o[6]);
				p.setTriggerType((Integer)o[7]);
				p.setIllustrate((String)o[8]);
				p.setOperator((String)o[9]);
				p.setProperties((String)o[10]);
				result.add(PersistenceAndBeanConvert.convert(p));
			}
			return result;
		});
	}

}
