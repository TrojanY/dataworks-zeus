package com.taobao.zeus.store.mysql.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.hibernate.query.Query;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;

import com.taobao.zeus.model.JobStatus.Status;
import com.taobao.zeus.store.mysql.manager.DebugHistoryManager;
import com.taobao.zeus.store.mysql.persistence.JobFileHistory;
import com.taobao.zeus.store.mysql.tool.PersistenceAndBeanConvert;

@SuppressWarnings("unchecked")
public class MysqlDebugHistoryManager extends HibernateDaoSupport implements DebugHistoryManager{

	@Override
	public com.taobao.zeus.model.DebugHistory addDebugHistory(com.taobao.zeus.model.DebugHistory history) {
		JobFileHistory persist=PersistenceAndBeanConvert.convert(history);
        assert getHibernateTemplate() != null;
        getHibernateTemplate().save(persist);
		history.setId(String.valueOf(persist.getId()));
		return history;
	}

	@Override
	public com.taobao.zeus.model.DebugHistory findDebugHistory(String id) {
        assert getHibernateTemplate() != null;
        JobFileHistory persist= getHibernateTemplate().get(JobFileHistory.class, Long.valueOf(id));
        assert persist != null;
        return PersistenceAndBeanConvert.convert(persist);
	}

	@Override
	public List<com.taobao.zeus.model.DebugHistory> pagingList(final String fileId, final int start, final int limit) {
        assert getHibernateTemplate() != null;
        return getHibernateTemplate()
				.execute( session -> {
					Query query=session.createQuery("select id,fileId,startTime,endTime,executeHost,status,script,log,owner from com.taobao.zeus.store.mysql.persistence.JobFileHistory" +
							" where fileId=:fileId order by id desc");
					query.setParameter("fileId", Long.valueOf(fileId));
					query.setMaxResults(limit);
					query.setFirstResult(start);
					List<Object[]> list=query.list();
					List<com.taobao.zeus.model.DebugHistory> result=new ArrayList<>();
					for(Object[] o:list){
						com.taobao.zeus.model.DebugHistory history=new com.taobao.zeus.model.DebugHistory();
						history.setId(String.valueOf(o[0]));
						history.setFileId(String.valueOf(o[1]));
						history.setStartTime((Date) o[2]);
						history.setEndTime((Date) o[3]);
						history.setExecuteHost(String.valueOf(o[4]));
						history.setStatus(o[5]==null?null:Status.parser(String.valueOf(o[5])));
						history.setScript(String.valueOf(o[6]));
						history.setLog(String.valueOf(o[7]));
						history.setOwner(String.valueOf(o[8]));
						result.add(history);
					}
					return result;
				});
	}

	@Override
	public int pagingTotal(String fileId) {
        assert getHibernateTemplate() != null;
        Number number=(Number) getHibernateTemplate()
                .execute(session -> {
                    Query query = session.createQuery("select count(*) from com.taobao.zeus.store.mysql.persistence.JobFileHistory where fileId=:fileId");
                    query.setParameter("fileId",fileId);
                    return query.list().iterator().next();
                });
        assert number != null;
        return number.intValue();
	}

	@Override
	public void updateDebugHistory(com.taobao.zeus.model.DebugHistory history) {
        assert getHibernateTemplate() != null;
        JobFileHistory org=getHibernateTemplate().get(JobFileHistory.class, Long.valueOf(history.getId()));
		
		JobFileHistory persist=PersistenceAndBeanConvert.convert(history);
		persist.setGmtModified(new Date());
        assert org != null;
        persist.setGmtCreate(org.getGmtCreate());
		persist.setLog(org.getLog());
		persist.setOwner(org.getOwner());
		getHibernateTemplate().update(persist);
	}

	@Override
	public void updateDebugHistoryLog(final String id,final  String log) {
        assert getHibernateTemplate() != null;
        getHibernateTemplate().execute(session -> {
		Query query=session.createQuery("update com.taobao.zeus.store.mysql.persistence.JobFileHistory set log=:log where id=:id");
		query.setParameter("log", log);
		query.setParameter("id", Long.valueOf(id));
		query.executeUpdate();
		return null;
	});
	}

}