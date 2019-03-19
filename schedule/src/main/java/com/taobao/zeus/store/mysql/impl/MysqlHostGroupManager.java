package com.taobao.zeus.store.mysql.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.query.Query;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;

import com.taobao.zeus.model.HostGroupCache;
import com.taobao.zeus.store.mysql.manager.HostGroupManager;
import com.taobao.zeus.store.mysql.persistence.HostGroup;
import com.taobao.zeus.store.mysql.persistence.HostRelation;
import com.taobao.zeus.util.Environment;

import javax.transaction.Transactional;

@SuppressWarnings("unchecked")
@Transactional
public class MysqlHostGroupManager extends HibernateDaoSupport implements HostGroupManager{

	public List<HostRelation> getAllHostRelations() {
		assert getHibernateTemplate() != null;
		return getHibernateTemplate().execute(session -> {
			Query query = session.createQuery("from com.taobao.zeus.store.mysql.persistence.HostRelationPersistence");
			return query.list();
		});
	}
	
	public List<HostRelation> getHostRelations(final String hostGroupId) {
		assert getHibernateTemplate() != null;
		return getHibernateTemplate().execute(session -> {
			Query query = session.createQuery("from com.taobao.zeus.store.mysql.persistence.HostRelationPersistence where hostGroupId=" + hostGroupId);
			return query.list();
		});
	}
	

	@Override
	public Map<String,HostGroupCache> getAllHostGroupInfomations() {
		Map<String,HostGroupCache> information = new HashMap<>();
		List<HostGroup> hostGroups = getAllHostGroup();
		List<HostRelation> relations = getAllHostRelations();
		for(HostGroup wg : hostGroups){
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
			for(HostRelation r : relations){
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
	public List<HostGroup> getAllHostGroup(){
		assert getHibernateTemplate() != null;
		return getHibernateTemplate().execute(session -> {
			Query query = session.createQuery("from com.taobao.zeus.store.mysql.persistence.HostGroupPersistence");
			return query.list();
		});
	}

	@Override
	public HostGroup getHostGroupName(String hostGroupId) {
		assert getHibernateTemplate() != null;
		return getHibernateTemplate().get(HostGroup.class, Integer.valueOf(hostGroupId));
	}

	@Override
	public List<String> getPreemptionHost() {
		String id = Environment.getDefaultMasterGroupId();
		List<HostRelation> hostRelations = getHostRelations(id);
		List<String> result = new ArrayList<>();
		for(HostRelation hostRelation : hostRelations){
			result.add(hostRelation.getHost());
		}
		return result;
	}

}
