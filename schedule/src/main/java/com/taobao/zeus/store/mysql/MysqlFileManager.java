package com.taobao.zeus.store.mysql;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.hibernate.query.Query;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;

import com.taobao.zeus.model.FileDescriptor;
import com.taobao.zeus.store.FileManager;
import com.taobao.zeus.store.mysql.persistence.FilePersistence;
import com.taobao.zeus.store.mysql.tool.PersistenceAndBeanConvert;

public class MysqlFileManager extends HibernateDaoSupport implements
		FileManager {

	@Override
	public FileDescriptor addFile(String uid, String parentId, String name,
			boolean folder) {
		FilePersistence fp = new FilePersistence();
		fp.setName(name);
		fp.setOwner(uid);
		fp.setParent(Long.valueOf(parentId));
		fp.setType(folder ? FilePersistence.FOLDER : FilePersistence.FILE);
		assert getHibernateTemplate() != null;
		getHibernateTemplate().save(fp);
		return PersistenceAndBeanConvert.convert(fp);
	}

	@Override
	public void deleteFile(String fileId) {
		assert getHibernateTemplate() != null;
		FilePersistence fp = getHibernateTemplate().get(
				FilePersistence.class, Long.valueOf(fileId));
		assert fp != null;
		getHibernateTemplate().delete(fp);
	}

	@Override
	public FileDescriptor getFile(String id) {
		assert getHibernateTemplate() != null;
		FilePersistence fp = getHibernateTemplate().get(
				FilePersistence.class, Long.valueOf(id));
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
							.createQuery("from com.taobao.zeus.store.mysql.persistence.FilePersistence where parent=:parent");
					query.setParameter("parent", Long.valueOf(id));
					List<FilePersistence> fps = query.list();
					List<FileDescriptor> list1 = new ArrayList<>();
					for (FilePersistence fp : fps) {
						list1.add(PersistenceAndBeanConvert.convert(fp));
					}
					return list1;
				});
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<FileDescriptor> getUserFiles(final String uid) {
		assert getHibernateTemplate() != null;
		List<FilePersistence> list = getHibernateTemplate()
				.execute(session -> {
					Query query = session
							.createQuery("from com.taobao.zeus.store.mysql.persistence.FilePersistence where owner=:owner and parent=null");
					query.setParameter("owner", uid);
					List<FilePersistence> list1 = query.list();
					if (list1 == null || list1.isEmpty()) {
						if (list1 == null) {
							list1 = new ArrayList<>();
						}
						FilePersistence personal = new FilePersistence();
						personal.setName(PERSONAL);
						personal.setOwner(uid);
						personal.setType(FilePersistence.FOLDER);
						session.save(personal);
						FilePersistence common = new FilePersistence();
						common.setName(SHARE);
						common.setOwner(uid);
						common.setType(FilePersistence.FOLDER);
						session.save(common);

						list1.add(personal);
						list1.add(common);
					}
					return list1;
				});
		List<FileDescriptor> result = new ArrayList<>();
		if (list != null) {
			for (FilePersistence fp : list) {
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
