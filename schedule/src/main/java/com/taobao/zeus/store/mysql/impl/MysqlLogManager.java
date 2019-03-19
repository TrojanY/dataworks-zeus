package com.taobao.zeus.store.mysql.impl;

import com.taobao.zeus.store.mysql.persistence.ZeusLog;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;

import com.taobao.zeus.model.LogDescriptor;

import javax.transaction.Transactional;

@SuppressWarnings("unchecked")
@Transactional
public class MysqlLogManager extends HibernateDaoSupport {


	
	public void addLog(LogDescriptor logDescriptor) {
		try {
			ZeusLog zeusLog = new ZeusLog();
			zeusLog.setLogType(logDescriptor.getLogType());
			zeusLog.setUserName(logDescriptor.getUserName());
			zeusLog.setIp(logDescriptor.getIp());
			zeusLog.setUrl(logDescriptor.getUrl());
			zeusLog.setRpc(logDescriptor.getRpc());
			zeusLog.setDelegate(logDescriptor.getDelegate());
			zeusLog.setMethod(logDescriptor.getMethod());
			zeusLog.setDescription(logDescriptor.getDescription());
			assert super.getHibernateTemplate() != null;
			super.getHibernateTemplate().save(zeusLog);
			
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public void asycAddLog(LogDescriptor logDescriptor) {
		try {
			ZeusLog zeusLog = new ZeusLog();
			zeusLog.setLogType(logDescriptor.getLogType());
			zeusLog.setUserName(logDescriptor.getUserName());
			zeusLog.setIp(logDescriptor.getIp());
			zeusLog.setUrl(logDescriptor.getUrl());
			zeusLog.setRpc(logDescriptor.getRpc());
			zeusLog.setDelegate(logDescriptor.getDelegate());
			zeusLog.setMethod(logDescriptor.getMethod());
			zeusLog.setDescription(logDescriptor.getDescription());
			assert super.getHibernateTemplate() != null;
			super.getHibernateTemplate().save(zeusLog);

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
