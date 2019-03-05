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
		getHibernateTemplate().save(fp);
		return PersistenceAndBeanConvert.convert(fp);
	}

	@Override
	public void deleteFile(String fileId) {
		FilePersistence fp = getHibernateTemplate().get(
				FilePersistence.class, Long.valueOf(fileId));
		getHibernateTemplate().delete(fp);
	}

	@Override
	public FileDescriptor getFile(String id) {
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
		List<FileDescriptor> list = getHibernateTemplate()
				.execute(session -> {
					Query query = session
							.createQuery("from com.taobao.zeus.store.mysql.persistence.FilePersistence where parent=?");
					query.setParameter(0, Long.valueOf(id));
					List<FilePersistence> fps = query.list();
					List<FileDescriptor> list1 = new ArrayList<>();
					for (FilePersistence fp : fps) {
						list1.add(PersistenceAndBeanConvert.convert(fp));
					}
					return list1;
				});
		return list;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<FileDescriptor> getUserFiles(final String uid) {
		List<FilePersistence> list = getHibernateTemplate()
				.execute(session -> {
					Query query = session
							.createQuery("from com.taobao.zeus.store.mysql.persistence.FilePersistence where owner=? and parent=null");
					query.setParameter(0, uid);
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
		getHibernateTemplate().update(PersistenceAndBeanConvert.convert(fd));
	}

}
