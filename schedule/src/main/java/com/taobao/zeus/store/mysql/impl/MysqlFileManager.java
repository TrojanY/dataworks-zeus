package com.taobao.zeus.store.mysql.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.hibernate.query.Query;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;

import com.taobao.zeus.model.FileDescriptor;
import com.taobao.zeus.store.mysql.manager.FileManager;
import com.taobao.zeus.store.mysql.persistence.JobFile;
import com.taobao.zeus.store.mysql.tool.PersistenceAndBeanConvert;

import javax.transaction.Transactional;

@Transactional
public class MysqlFileManager extends HibernateDaoSupport implements
		FileManager {

	@Override
	public FileDescriptor addFile(String uid, String parentId, String name,
			boolean folder) {
		JobFile fp = new JobFile();
		fp.setName(name);
		fp.setOwner(uid);
		fp.setParent(Long.valueOf(parentId));
		fp.setType(folder ? JobFile.FOLDER : JobFile.FILE);
		assert getHibernateTemplate() != null;
		getHibernateTemplate().save(fp);
		return PersistenceAndBeanConvert.convert(fp);
	}

	@Override
	public void deleteFile(String fileId) {
		assert getHibernateTemplate() != null;
		JobFile fp = getHibernateTemplate().get(
				JobFile.class, Long.valueOf(fileId));
		assert fp != null;
		getHibernateTemplate().delete(fp);
	}

	@Override
	public FileDescriptor getFile(String id) {
		assert getHibernateTemplate() != null;
		JobFile fp = getHibernateTemplate().get(
				JobFile.class, Long.valueOf(id));
		if (fp != null) {
			return PersistenceAndBeanConvert.convert(fp);
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<FileDescriptor> getSubFiles(final String id) {
		assert getHibernateTemplate() != null;
		return getHibernateTemplate()
				.execute(session -> {
					Query query = session
							.createQuery("from com.taobao.zeus.store.mysql.persistence.JobFile where parent=:parent");
					query.setParameter("parent", Long.valueOf(id));
					List<JobFile> fps = query.list();
					List<FileDescriptor> list1 = new ArrayList<>();
					for (JobFile fp : fps) {
						list1.add(PersistenceAndBeanConvert.convert(fp));
					}
					return list1;
				});
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<FileDescriptor> getUserFiles(final String uid) {
		assert getHibernateTemplate() != null;
		List<JobFile> list = getHibernateTemplate()
				.execute(session -> {
					Query query = session
							.createQuery("from com.taobao.zeus.store.mysql.persistence.JobFile where owner=:owner and parent=null");
					query.setParameter("owner", uid);
					List<JobFile> list1 = query.list();
					if (list1 == null || list1.isEmpty()) {
						if (list1 == null) {
							list1 = new ArrayList<>();
						}
						JobFile personal = new JobFile();
						personal.setName(PERSONAL);
						personal.setOwner(uid);
						personal.setType(JobFile.FOLDER);
						session.save(personal);
						JobFile common = new JobFile();
						common.setName(SHARE);
						common.setOwner(uid);
						common.setType(JobFile.FOLDER);
						session.save(common);

						list1.add(personal);
						list1.add(common);
					}
					return list1;
				});
		List<FileDescriptor> result = new ArrayList<>();
		if (list != null) {
			for (JobFile fp : list) {
				result.add(PersistenceAndBeanConvert.convert(fp));
			}
		}
		return result;
	}

	@Override
	public void update(FileDescriptor fd) {
		fd.setGmtModified(new Date());
		assert getHibernateTemplate() != null;
		getHibernateTemplate().update(PersistenceAndBeanConvert.convert(fd));
	}

}
