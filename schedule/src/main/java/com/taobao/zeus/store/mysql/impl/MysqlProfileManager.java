package com.taobao.zeus.store.mysql.impl;

import java.util.Date;
import java.util.List;

import org.hibernate.query.Query;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;

import com.taobao.zeus.store.mysql.manager.ProfileManager;
import com.taobao.zeus.store.mysql.persistence.ZeusProfile;
import com.taobao.zeus.store.mysql.tool.PersistenceAndBeanConvert;

import javax.transaction.Transactional;

@SuppressWarnings("unchecked")
@Transactional
public class MysqlProfileManager extends HibernateDaoSupport implements ProfileManager {

	@Override
	public com.taobao.zeus.model.Profile findByUid(final String uid) {
		assert getHibernateTemplate() != null;
		return getHibernateTemplate().execute(session -> {
			Query query=session.createQuery("from com.taobao.zeus.store.mysql.persistence.ZeusProfile where uid=:uid");
			query.setParameter("uid", uid);
			List<ZeusProfile> list=query.list();
			if(!list.isEmpty()){
				return PersistenceAndBeanConvert.convert(list.get(0));
			}
			return null;
		});
	}

	@Override
	public void update(String uid, com.taobao.zeus.model.Profile p){
		com.taobao.zeus.model.Profile old=findByUid(uid);
		if(old==null){
			old=new com.taobao.zeus.model.Profile();
			old.setUid(uid);
			assert getHibernateTemplate() != null;
			getHibernateTemplate().save(PersistenceAndBeanConvert.convert(old));
			old=findByUid(uid);
		}
		p.setUid(old.getUid());
		p.setGmtModified(new Date());
		getHibernateTemplate().update(PersistenceAndBeanConvert.convert(p));
	}
}
