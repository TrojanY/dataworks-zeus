package com.taobao.zeus.store.mysql;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.HibernateException;
import org.hibernate.query.NativeQuery;
import org.hibernate.Session;
import org.springframework.orm.hibernate5.HibernateCallback;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;

@SuppressWarnings("unchecked")
public class MysqlReportManager extends HibernateDaoSupport{

	/**
	 * yyyyMMdd->{success:1,fail:2},{success:1,fail:2}
	 * @param start start date
	 * @param end end date
	 * @return Map<String, Map<String, String>>
	 */
	public Map<String, Map<String, String>> runningJobs(final Date start,final Date end){
		return (Map<String, Map<String, String>>) getHibernateTemplate().execute(new HibernateCallback() {
			private SimpleDateFormat format=new SimpleDateFormat("yyyyMMdd");
			@Override
			public Object doInHibernate(Session session) throws HibernateException{
				Map<String, Map<String, String>> result=new HashMap<>();
				
				String success_sql="select count(distinct h.action_id),h.gmt_create from zeus_action_history h " +
						"left join zeus_action j on h.action_id=j.id " +
						"where h.status='success' and trigger_type=1 and to_days(h.gmt_create) between to_days(:beginday) and to_days(:endday) "+
						"group by to_days(h.gmt_create) order by h.gmt_create desc";
				NativeQuery query=session.createSQLQuery(success_sql);
				query.setParameter("startday", start);
				query.setParameter("endday", end);
				List success_list=query.list();
				for(Object o:success_list){
					Object[] oo=(Object[])o;
					Date date=(Date)oo[1];
					Number success=(Number)oo[0];
					Map<String, String> map=new HashMap<>();
					map.put("success", success.toString());
					result.put(format.format(date), map);
				}
				
				String fail_sql="select count(distinct h.action_id),h.gmt_create from zeus_action_history h " +
					"left join zeus_action j on h.action_id=j.id " +
					"where h.status='failed' and trigger_type=1 and to_days(h.gmt_create) between to_days(:one) and to_days(:two) "+
					"and h.action_id not in (select action_id from zeus_action_history where status='success' and trigger_type=1 and to_days(gmt_create) between to_days(:three) and to_days(:four)) "+
					"group by to_days(h.gmt_create) order by h.gmt_create desc";
				query=session.createSQLQuery(fail_sql);
				query.setParameter("one", start);
				query.setParameter("two", end);
				query.setParameter("three", start);
				query.setParameter("four", end);
				List fail_list=query.list();
				for(Object o:fail_list){
					Object[] oo=(Object[])o;
					Date date=(Date)oo[1];
					Number fail=(Number)oo[0];
					Map<String, String> map = result.computeIfAbsent(format.format(date), k -> new HashMap<>());
					map.put("fail", fail.toString());
				}
				
				return result;
			}
		});
	}

	public List<Map<String, String>> ownerFailJobs(final Date date){
		assert getHibernateTemplate() != null;
		List<Map<String, String>> list=getHibernateTemplate()
				.execute(session -> {
			List<Map<String, String>> result=new ArrayList<>();

			String sql="select count(distinct h.action_id) as cou,j.owner,u.name from zeus_action_history h " +
					"left join zeus_action j on h.action_id=j.id " +
					"left join zeus_user u on j.owner=u.uid " +
					"where h.status='failed' and h.trigger_type=1 " +
					"and to_days(:one)=to_days(h.gmt_create) "+
					"and h.action_id not in (select action_id from zeus_action_history where status='success' and trigger_type=1 and to_days(:two)=to_days(gmt_create)) "+
					"group by j.owner order by cou desc limit 10";
			NativeQuery query=session.createSQLQuery(sql);
			query.setParameter("one", date);
			query.setParameter("two", date);
			List list1 =query.list();
			for(Object o: list1){
				Object[] oo=(Object[])o;
				Number num=(Number)oo[0];
				String uid=(String)oo[1];
				String uname=(String)oo[2];
				Map<String, String> map=new HashMap<>();
				map.put("count", num.toString());
				map.put("uid", uid);
				map.put("uname", uname);
				result.add(map);
			}
			return result;
		});
		
		for(final Map<String, String> map:list){
			getHibernateTemplate().execute(session -> {
				String sql="select distinct h.action_id,j.name from zeus_action_history h " +
						"left join zeus_action j on h.action_id=j.id where h.status='failed' " +
						"and h.trigger_type=1 and to_days(:one) =to_days(h.gmt_create) and j.owner=two "+
						"and h.action_id not in (select action_id from zeus_action_history where status='success' and trigger_type=1 and to_days(three)=to_days(gmt_create))";
				NativeQuery query=session.createSQLQuery(sql);
				query.setParameter("one", date);
				query.setParameter("two", map.get("uid"));
				query.setParameter("three", date);
				List<Object[]> list12 =query.list();
				int count=0;
				for(Object[] rs: list12){
					String jobID = rs[0].toString();
					String jobName = rs[1].toString();
					// 去重
					if(!map.containsKey(jobID)){
						map.put("history"+count++, jobName+"("+jobID+")");
					}
					map.put(jobID, null);
				}
				map.put("count", count+"");
				return null;
			});
		}
		return list;
	}
}
