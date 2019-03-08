package com.taobao.zeus.store.mysql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.query.Query;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;

import com.taobao.zeus.model.HostGroupCache;
import com.taobao.zeus.store.HostGroupManager;
import com.taobao.zeus.store.mysql.persistence.HostGroupPersistence;
import com.taobao.zeus.store.mysql.persistence.HostRelationPersistence;
import com.taobao.zeus.util.Environment;

@SuppressWarnings("unchecked")
public class MysqlHostGroupManager extends HibernateDaoSupport implements HostGroupManager{

	public List<HostRelationPersistence> getAllHostRelations() {
		assert getHibernateTemplate() != null;
		return getHibernateTemplate().execute(session -> {
			Query query = session.createQuery("from com.taobao.zeus.store.mysql.persistence.HostRelationPersistence");
			return query.list();
		});
	}
	
	public List<HostRelationPersistence> getHostRelations(final String hostGroupId) {
		assert getHibernateTemplate() != null;
		return getHibernateTemplate().execute(session -> {
			Query query = session.createQuery("from com.taobao.zeus.store.mysql.persistence.HostRelationPersistence where hostGroupId=" + hostGroupId);
			return query.list();
		});
	}
	

	@Override
	public Map<String,HostGroupCache> getAllHostGroupInfomations() {
		Map<String,HostGroupCache> information = new HashMap<>();
		List<HostGroupPersistence> hostGroups = getAllHostGroup();
		List<HostRelationPersistence> relations = getAllHostRelations();
		for(HostGroupPersistence wg : hostGroups){
			if (wg.getEffective() == 0) {
				continue;
			}
			HostGroupCache info = new HostGroupCache();
			String id = wg.getId().toString();
			info.setId(id);
			info.setCurrentPositon(0);
			info.setName(wg.getName());
			info.setDescription(wg.getDescription());
			List<String> hosts = new ArrayList<>();
			for(HostRelationPersistence r : relations){
				if (wg.getId().equals(r.getHostGroupId())) {
					hosts.add(r.getHost());
				}
			}
			info.setHosts(hosts);
			information.put(id, info);
		}
		return information;
	}
	@Override
	public List<HostGroupPersistence> getAllHostGroup(){
		assert getHibernateTemplate() != null;
		return getHibernateTemplate().execute(session -> {
			Query query = session.createQuery("from com.taobao.zeus.store.mysql.persistence.HostGroupPersistence");
			return query.list();
		});
	}

	@Override
	public HostGroupPersistence getHostGroupName(String hostGroupId) {
		assert getHibernateTemplate() != null;
		return getHibernateTemplate().get(HostGroupPersistence.class, Integer.valueOf(hostGroupId));
	}

	@Override
	public List<String> getPreemptionHost() {
		String id = Environment.getDefaultMasterGroupId();
		List<HostRelationPersistence> hostRelations = getHostRelations(id);
		List<String> result = new ArrayList<>();
		for(HostRelationPersistence hostRelation : hostRelations){
			result.add(hostRelation.getHost());
		}
		return result;
	}

}
