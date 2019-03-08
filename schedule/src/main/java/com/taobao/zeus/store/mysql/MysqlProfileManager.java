package com.taobao.zeus.store.mysql;

import java.util.Date;
import java.util.List;

import org.hibernate.query.Query;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;

import com.taobao.zeus.model.Profile;
import com.taobao.zeus.store.ProfileManager;
import com.taobao.zeus.store.mysql.persistence.ProfilePersistence;
import com.taobao.zeus.store.mysql.tool.PersistenceAndBeanConvert;

@SuppressWarnings("unchecked")
public class MysqlProfileManager extends HibernateDaoSupport implements ProfileManager {

	@Override
	public Profile findByUid(final String uid) {
		assert getHibernateTemplate() != null;
		return getHibernateTemplate().execute(session -> {
			Query query=session.createQuery("from com.taobao.zeus.store.mysql.persistence.ProfilePersistence where uid=:uid");
			query.setParameter("uid", uid);
			List<ProfilePersistence> list=query.list();
			if(!list.isEmpty()){
				return PersistenceAndBeanConvert.convert(list.get(0));
			}
			return null;
		});
	}

	@Override
	public void update(String uid,Profile p){
		Profile old=findByUid(uid);
		if(old==null){
			old=new Profile();
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
