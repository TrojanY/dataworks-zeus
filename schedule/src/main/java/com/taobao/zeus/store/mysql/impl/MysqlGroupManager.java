package com.taobao.zeus.store.mysql.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.taobao.zeus.model.JobDescriptor;
import com.taobao.zeus.store.GroupBean;
import com.taobao.zeus.store.mysql.tool.GroupManagerTool;
import com.taobao.zeus.store.JobBean;
import com.taobao.zeus.store.mysql.persistence.*;
import com.taobao.zeus.store.mysql.tool.PersistenceAndBeanConvert;
import org.hibernate.query.Query;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;

import com.taobao.zeus.client.ZeusException;
import com.taobao.zeus.model.GroupDescriptor;
import com.taobao.zeus.model.JobStatus;
import com.taobao.zeus.store.mysql.manager.GroupManager;
import com.taobao.zeus.store.mysql.tool.GroupValidate;
import com.taobao.zeus.util.Tuple;

import javax.transaction.Transactional;

@SuppressWarnings("unchecked")
@Transactional
public class MysqlGroupManager extends HibernateDaoSupport implements
		GroupManager {
	@Override
	public void deleteGroup(String user, String groupId) throws ZeusException {
		GroupBean group = getDownstreamGroupBean(groupId);
		if (group.isDirectory()) {
			if (!group.getChildrenGroupBeans().isEmpty()) {
				throw new ZeusException("该组下不为空，无法删除");
			}
			boolean candelete = true;
			for (GroupBean child : group.getChildrenGroupBeans()) {
				if (child.isExisted()) {
					candelete = false;
					break;
				}
			}
			if (!candelete) {
				throw new ZeusException("该组下不为空，无法删除");
			}
		} else {
			if (!group.getJobBeans().isEmpty()) {
				throw new ZeusException("该组下不为空，无法删除");
			}
		}
		assert getHibernateTemplate() != null;
		JobGroup object = getHibernateTemplate().get(JobGroup.class,
				Integer.valueOf(groupId));
		assert object != null;
		object.setExisted(0);
		object.setGmtModified(new Date());
		getHibernateTemplate().update(object);
	}

	@Override
	public GroupBean getDownstreamGroupBean(String groupId) {
		GroupDescriptor group = getGroupDescriptor(groupId);
		GroupBean result = new GroupBean(group);
		return getDownstreamGroupBean(result);
	}

	@Override
	public GroupBean getDownstreamGroupBean(GroupBean parent) {
		if (parent.isDirectory()) {
			List<GroupDescriptor> children = getChildrenGroup(parent
					.getGroupDescriptor().getId());
			for (GroupDescriptor child : children) {
				GroupBean childBean = new GroupBean(child);
				getDownstreamGroupBean(childBean);
				childBean.setParentGroupBean(parent);
				parent.getChildrenGroupBeans().add(childBean);
			}
		} else {
			List<Tuple<JobDescriptor, JobStatus>> jobs = getChildrenJob(parent
					.getGroupDescriptor().getId());
			for (Tuple<JobDescriptor, JobStatus> tuple : jobs) {
				JobBean JobBean = new JobBean(tuple.getX(), tuple.getY());
				JobBean.setGroupBean(parent);
				parent.getJobBeans().put(tuple.getX().getJobId(), JobBean);
			}
		}
		return parent;
	}

	@Override
	public GroupBean getGlobeGroupBean() {
		return GroupManagerTool.buildGlobeGroupBean(this);
	}

	/**
	 * 获取叶子组下所有的Job
	 */
	@Override
	public List<Tuple<JobDescriptor, JobStatus>> getChildrenJob(String groupId) {
		assert getHibernateTemplate() != null;
		List<JobTaskAction> list = getHibernateTemplate().execute(
				session -> {
					Query query = session.createQuery("from com.taobao.zeus.store.mysql.persistence.JobActionPersistence where groupId="
							+ groupId);
					return query.list();
				});
		List<Tuple<JobDescriptor, JobStatus>> result = new ArrayList<>();
		if (list != null) {
			for (JobTaskAction j : list) {
				result.add(PersistenceAndBeanConvert.convert(j));
			}
		}
		return result;
	}

	/**
	 * 获取组的下级组列表
	 */
	@Override
	public List<GroupDescriptor> getChildrenGroup(String groupId) {
		assert getHibernateTemplate() != null;
		List<JobGroup> list = getHibernateTemplate().execute(
				session -> {
					Query query=session.createQuery("from com.taobao.zeus.store.mysql.persistence.GroupPersistence where parent=:groupId");
					query.setParameter("groupId",groupId);
					return query.list();
				});
		List<GroupDescriptor> result = new ArrayList<>();
		if (list != null) {
			for (JobGroup p : list) {
				result.add(PersistenceAndBeanConvert.convert(p));
			}
		}
		return result;
	}

	@Override
	public GroupDescriptor getGroupDescriptor(String groupId) {
		JobGroup persist = getHibernateTemplate()
				.get(JobGroup.class, Integer.valueOf(groupId));
		if (persist != null) {
			return PersistenceAndBeanConvert.convert(persist);
		}
		return null;
	}

	@Override
	public String getRootGroupId() {
		assert getHibernateTemplate() != null;
		return getHibernateTemplate().execute(session -> {
            Query query = session
                    .createQuery("from com.taobao.zeus.store.mysql.persistence.GroupPersistence g order by g.id asc");
            query.setMaxResults(1);
            List<JobGroup> list = query.list();
            if (list == null || list.size() == 0) {
                JobGroup persist = new JobGroup();
                persist.setName("众神之神");
                persist.setOwner(ZeusUser.ADMIN.getUid());
                persist.setDirectory(0);
                session.save(persist);
                if (persist.getId() == null) {
                    return null;
                }
                return String.valueOf(persist.getId());
            }
            return String.valueOf(list.get(0).getId());
        });
	}

	@Override
	public GroupBean getUpstreamGroupBean(String groupId) {
		return GroupManagerTool.getUpstreamGroupBean(groupId, this);
	}

	@Override
	public void updateGroup(String user, GroupDescriptor group)
			throws ZeusException {
		assert getHibernateTemplate() != null;
		JobGroup old = getHibernateTemplate().get(
				JobGroup.class, Integer.valueOf(group.getId()));
		assert old != null;
		updateGroup(user, group, old.getOwner(), old.getParent() == null ? null
				: old.getParent().toString());
	}

	public void updateGroup(String user, GroupDescriptor group, String owner,
			String parent) throws ZeusException {

		if (getHibernateTemplate()== null) throw new ZeusException("take session factory is error");
		JobGroup old = getHibernateTemplate().get(
				JobGroup.class, Integer.valueOf(group.getId()));

		JobGroup persist = PersistenceAndBeanConvert.convert(group);

		persist.setOwner(owner);
		if (parent != null) {
			persist.setParent(Integer.valueOf(parent));
		}

		// 以下属性不允许修改，强制采用老的数据
		if (old== null) throw new ZeusException("None found group descriptor");
		persist.setDirectory(old.getDirectory());
		persist.setGmtCreate(old.getGmtCreate());
		persist.setGmtModified(new Date());
		persist.setExisted(old.getExisted());

		getHibernateTemplate().update(persist);
	}

	@Override
	public GroupDescriptor createGroup(String user, String groupName,
			String parentGroup, boolean isDirectory) throws ZeusException {
		if (parentGroup == null) {
			throw new ZeusException("parent group may not be null");
		}
		GroupDescriptor group = new GroupDescriptor();
		group.setOwner(user);
		group.setName(groupName);
		group.setParent(parentGroup);
		group.setDirectory(isDirectory);

		GroupValidate.valide(group);

		JobGroup persist = PersistenceAndBeanConvert.convert(group);
		persist.setGmtCreate(new Date());
		persist.setGmtModified(new Date());
		persist.setExisted(1);
		assert getHibernateTemplate() != null;
		getHibernateTemplate().save(persist);
		return PersistenceAndBeanConvert.convert(persist);
	}

	@Override
	public void grantGroupOwner(String granter, String uid, String groupId)
			throws ZeusException {
		GroupDescriptor gd = getGroupDescriptor(groupId);
		if (gd != null) {
			updateGroup(granter, gd, uid, gd.getParent());
		}
	}

	@Override
	public void moveGroup(String uid, String groupId, String newParentGroupId)
			throws ZeusException {
		GroupDescriptor gd = getGroupDescriptor(groupId);
		GroupDescriptor parent = getGroupDescriptor(newParentGroupId);
		if (!parent.isDirectory()) {
			throw new ZeusException("非法操作");
		}
		updateGroup(uid, gd, gd.getOwner(), newParentGroupId);
	}

	@Override
	public boolean IsExistedBelowRootGroup(String groupName) {
		String rootId = getRootGroupId();
		assert getHibernateTemplate() != null;
		List<JobGroup> tmps = getHibernateTemplate().execute(
				session -> {
					Query query = session.createQuery("from com.taobao.zeus.store.mysql.persistence.GroupPersistence where existed=1 and parent=" + rootId);
					return query.list();

				});
		assert tmps != null;
		for (JobGroup tmp : tmps) {
			if (tmp.getName().equals(groupName)) {
				return true;
			}
		}
		return false;
	}
}