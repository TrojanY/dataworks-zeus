package com.taobao.zeus.store.mysql;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.hibernate.query.Query;
import org.springframework.orm.hibernate5.HibernateCallback;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;

import com.taobao.zeus.model.DebugHistory;
import com.taobao.zeus.model.JobStatus.Status;
import com.taobao.zeus.store.DebugHistoryManager;
import com.taobao.zeus.store.mysql.persistence.DebugHistoryPersistence;
import com.taobao.zeus.store.mysql.tool.PersistenceAndBeanConvert;

public class MysqlDebugHistoryManager extends HibernateDaoSupport implements DebugHistoryManager{

	@Override
	public DebugHistory addDebugHistory(DebugHistory history) {
		DebugHistoryPersistence persist=PersistenceAndBeanConvert.convert(history);
		getHibernateTemplate().save(persist);
		history.setId(String.valueOf(persist.getId()));
		return history;
	}

	@Override
	public DebugHistory findDebugHistory(String id) {
		DebugHistoryPersistence persist= getHibernateTemplate().get(DebugHistoryPersistence.class, Long.valueOf(id));
		return PersistenceAndBeanConvert.convert(persist);
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<DebugHistory> pagingList(final String fileId, final int start, final int limit) {
		return (List<DebugHistory>) getHibernateTemplate()
				.execute((HibernateCallback) session -> {
					Query query=session.createQuery("select id,fileId,startTime,endTime,executeHost,status,script,log,owner from com.taobao.zeus.store.mysql.persistence.DebugHistoryPersistence" +
							" where fileId=:fileId order by id desc");
					query.setParameter("fileId", Long.valueOf(fileId));
					query.setMaxResults(limit);
					query.setFirstResult(start);
					List<Object[]> list=query.list();
					List<DebugHistory> result=new ArrayList<>();
					for(Object[] o:list){
						DebugHistory history=new DebugHistory();
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
		Number number=(Number) getHibernateTemplate()
                .find("select count(*) from com.taobao.zeus.store.mysql.persistence.DebugHistoryPersistence where fileId="+fileId)
		        .iterator().next();
		return number.intValue();
	}

	@Override
	public void updateDebugHistory(DebugHistory history) {
		DebugHistoryPersistence org=getHibernateTemplate().get(DebugHistoryPersistence.class, Long.valueOf(history.getId()));
		
		DebugHistoryPersistence persist=PersistenceAndBeanConvert.convert(history);
		persist.setGmtModified(new Date());
		persist.setGmtCreate(org.getGmtCreate());
		persist.setLog(org.getLog());
		persist.setOwner(org.getOwner());
		getHibernateTemplate().update(persist);
	}

	@Override
	public void updateDebugHistoryLog(final String id,final  String log) {
		getHibernateTemplate().execute(session -> {
		Query query=session.createQuery("update com.taobao.zeus.store.mysql.persistence.DebugHistoryPersistence set log=:log where id=:id");
		query.setParameter("log", log);
		query.setParameter("id", Long.valueOf(id));
		query.executeUpdate();
		return null;
	});
	}

}