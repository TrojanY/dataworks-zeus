package com.taobao.zeus.store.mysql;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.query.Query;
import org.hibernate.query.NativeQuery;
import org.springframework.orm.hibernate5.HibernateCallback;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;

import com.taobao.zeus.model.JobHistory;
import com.taobao.zeus.store.JobHistoryManager;
import com.taobao.zeus.store.mysql.persistence.JobHistoryPersistence;
import com.taobao.zeus.store.mysql.tool.PersistenceAndBeanConvert;
@SuppressWarnings("unchecked")
public class MysqlJobHistoryManager extends HibernateDaoSupport implements JobHistoryManager{

	@Override
	public void updateJobHistoryLog(final String id, final String log) {
		getHibernateTemplate().execute(session -> {
			Query query=session.createQuery("update com.taobao.zeus.store.mysql.persistence.JobHistoryPersistence set log=? where id=?");
			query.setParameter(0, log);
			query.setParameter(1, Long.valueOf(id));
			query.executeUpdate();
			return null;
		});
	}

	@Override
	public void updateJobHistory(JobHistory history) {
		JobHistoryPersistence org=getHibernateTemplate().get(JobHistoryPersistence.class, Long.valueOf(history.getId()));
		
		JobHistoryPersistence persist=PersistenceAndBeanConvert.convert(history);
		persist.setGmtModified(new Date());
		persist.setGmtCreate(org.getGmtCreate());
		persist.setLog(org.getLog());
		getHibernateTemplate().update(persist);
	}

	@Override
	public JobHistory addJobHistory(JobHistory history) {
		JobHistoryPersistence persist=PersistenceAndBeanConvert.convert(history);
		persist.setGmtCreate(new Date());
		persist.setGmtModified(new Date());
		Long id=(Long)getHibernateTemplate().save(persist);
		history.setId(id.toString()); 
		return history;
	}
	
	@Override
	public List<JobHistory> pagingList(final String jobId,final int start,final int limit) {
		return (List<JobHistory>) getHibernateTemplate().execute((HibernateCallback) session -> {
			NativeQuery query=session.createSQLQuery("select id,action_id,job_id,start_time,end_time,execute_host,status,trigger_type,illustrate,operator,properties,statis_end_time,timezone,cycle from zeus_action_history" +
					" where job_id=? or action_id=? order by id desc");
			query.setParameter(0, Long.valueOf(jobId));
			query.setParameter(1, Long.valueOf(jobId));
			query.setMaxResults(limit);
			query.setFirstResult(start);
			List<Object[]> list=query.list();
			List<JobHistory> result=new ArrayList<>();
			for(Object[] o:list){
				JobHistoryPersistence p=new JobHistoryPersistence();
				p.setId(((Number)o[0]).longValue());
				p.setJobId(((Number)o[1]).longValue());
				p.setToJobId(((Number)o[2]).longValue());
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
		Number number=(Number) getHibernateTemplate().find("select count(*) from com.taobao.zeus.store.mysql.persistence.JobHistoryPersistence where toJobId="+jobId)
		.iterator().next();
		return number.intValue();
	}

	@Override
	public JobHistory findJobHistory(String id) {
		JobHistoryPersistence persist= getHibernateTemplate().get(JobHistoryPersistence.class, Long.valueOf(id));
		return PersistenceAndBeanConvert.convert(persist);
	}
 
	@Override
	public Map<String, JobHistory> findLastHistoryByList(final List<String> jobIds) {
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
		List<JobHistory> list=(List<JobHistory>) getHibernateTemplate().execute((HibernateCallback) session -> {
			if(ids==null || ids.isEmpty()){
				return Collections.emptyList();
			}
			String sql="select id,action_id,job_id,start_time,end_time,execute_host,status,trigger_type,illustrate,operator,properties from zeus_action_history where id in (:ids)";
			NativeQuery query=session.createSQLQuery(sql);
			query.setParameterList("ids", ids);
			List<Object[]> list1 = query.list();
			List<JobHistory> result=new ArrayList<>();
			for(Object[] o: list1){
				JobHistoryPersistence p=new JobHistoryPersistence();
				p.setId(((Number)o[0]).longValue());
				p.setJobId(((Number)o[1]).longValue());
				p.setToJobId(((Number)o[2]).longValue());
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
		
		
		Map<String, JobHistory> map=new HashMap<>();
		for(JobHistory p:list){
			map.put(p.getJobId(),p);
		}
		return map;
	}

	@Override
	public List<JobHistory> findRecentRunningHistory() {
		return (List<JobHistory>) getHibernateTemplate().execute((HibernateCallback) session -> {
			String sql="select id,action_id,job_id,start_time,end_time,execute_host,status,trigger_type,illustrate,operator,properties from zeus_action_history where start_time>?";
			NativeQuery query=session.createSQLQuery(sql);
			Calendar cal=Calendar.getInstance();
			cal.add(Calendar.DAY_OF_YEAR, -1);
			query.setParameter(0, cal.getTime());
			List<Object[]> list= query.list();
			List<JobHistory> result=new ArrayList<>();
			for(Object[] o:list){
				JobHistoryPersistence p=new JobHistoryPersistence();
				p.setId(((Number)o[0]).longValue());
				p.setJobId(((Number)o[1]).longValue());
				p.setToJobId(((Number)o[2]).longValue());
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
