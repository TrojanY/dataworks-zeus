package com.taobao.zeus.store.mysql.impl;

import java.text.SimpleDateFormat;
import java.util.*;

import com.taobao.zeus.model.processor.DownloadProcessor;
import com.taobao.zeus.store.mysql.manager.GroupManager;
import com.taobao.zeus.store.mysql.manager.JobManager;
import com.taobao.zeus.store.mysql.persistence.JobTask;
import com.taobao.zeus.store.mysql.persistence.JobTaskAction;
import com.taobao.zeus.store.mysql.persistence.JobTaskHistory;
import com.taobao.zeus.store.mysql.tool.GroupManagerTool;
import org.apache.commons.lang.StringUtils;
import org.hibernate.query.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.orm.hibernate5.support.HibernateDaoSupport;

import com.taobao.zeus.client.ZeusException;
import com.taobao.zeus.model.GroupDescriptor;
import com.taobao.zeus.model.JobDescriptor;
import com.taobao.zeus.model.JobDescriptor.JobRunType;
import com.taobao.zeus.model.JobDescriptor.JobScheduleType;
import com.taobao.zeus.model.JobStatus;
import com.taobao.zeus.model.processor.Processor;
import com.taobao.zeus.store.GroupBean;
import com.taobao.zeus.store.JobBean;
import com.taobao.zeus.store.mysql.persistence.ZeusWorker;
import com.taobao.zeus.store.mysql.tool.JobValidate;
import com.taobao.zeus.store.mysql.tool.PersistenceAndBeanConvert;
import com.taobao.zeus.util.Tuple;

import javax.transaction.Transactional;

@SuppressWarnings("unchecked")
@Transactional
public class MysqlJobManager extends HibernateDaoSupport implements JobManager {


	@Autowired
	private JobValidate jobValidate;

	@Autowired
	private GroupManager groupManager;

	@Override
	public JobDescriptor createJob(String user, String jobName,
								   String parentGroup, JobRunType jobType) throws ZeusException {
		GroupDescriptor parent = groupManager.getGroupDescriptor(parentGroup);
		if (parent.isDirectory()) {
			throw new ZeusException("目录组下不得创建Job");
		}
		JobDescriptor job = new JobDescriptor();
		job.setOwner(user);
		job.setName(jobName);
		job.setGroupId(parentGroup);
		job.setJobType(jobType);
		job.setPreProcessors(Arrays.asList((Processor) new DownloadProcessor()));
		JobTask persist = PersistenceAndBeanConvert.convert(job);
		persist.setGmtCreate(new Date());
		persist.setGmtModified(new Date());
		getHibernateTemplate().save(persist);
		return PersistenceAndBeanConvert.convert(persist).getX();
	}

	@Override
	public void deleteJob(String user, String jobId) throws ZeusException {
		GroupBean root = groupManager.getGlobeGroupBean();
		JobBean job = root.getAllSubJobBeans().get(jobId);
		if (!job.getDepender().isEmpty()) {
			List<String> dependers = new ArrayList<>();
			for (JobBean jb : job.getDepender()) {
				dependers.add(jb.getJobDescriptor().getJobId());
			}
			throw new ZeusException("该Job正在被其他Job" + dependers.toString()
					+ "依赖，无法删除");
		}
		assert getHibernateTemplate() != null;
		getHibernateTemplate().delete(
				Objects.requireNonNull(getHibernateTemplate().get(JobTaskAction.class,
						Long.valueOf(jobId))));
	}

	@Override
	public void updateJob(String user, JobDescriptor job) throws ZeusException {
		JobTask orgPersist = getHibernateTemplate()
				.get(JobTask.class, Long.valueOf(job.getJobId()));
		updateJob(user, job, orgPersist.getOwner(), orgPersist.getGroupId()
				.toString());
	}

	public void updateJob(String user, JobDescriptor job, String owner,
						  String groupId) throws ZeusException {
		JobTask orgPersist = getHibernateTemplate()
				.get(JobTask.class, Long.valueOf(job.getJobId()));
		if (job.getScheduleType() == JobScheduleType.Independent) {
			job.setDependencies(new ArrayList<>());
		} else if (job.getScheduleType() == JobScheduleType.Dependent) {
			job.setCronExpression("");
		}
		job.setOwner(owner);
		job.setGroupId(groupId);

		// 以下属性不允许修改，强制采用老的数据
		JobTask persist = PersistenceAndBeanConvert.convert(job);
		persist.setGmtCreate(orgPersist.getGmtCreate());
		persist.setGmtModified(new Date());
		persist.setRunType(orgPersist.getRunType());
		persist.setStatus(orgPersist.getStatus());
		persist.setReadyDependency(orgPersist.getReadyDependency());
		persist.setHost(job.getHost());
		persist.setHostGroupId(Integer.valueOf(job.getHostGroupId()));
		// 如果是用户从界面上更新，开始时间、统计周期等均为空，用原来的值
		if (job.getStartTime() == null || "".equals(job.getStartTime())) {
			persist.setStartTime(orgPersist.getStartTime());
		}
		if (job.getStartTimestamp() == 0) {
			persist.setStartTimestamp(orgPersist.getStartTimestamp());
		}
		if (job.getStatisStartTime() == null
				|| "".equals(job.getStatisStartTime())) {
			persist.setStatisStartTime(orgPersist.getStatisStartTime());
		}
		if (job.getStatisEndTime() == null || "".equals(job.getStatisEndTime())) {
			persist.setStatisEndTime(orgPersist.getStatisEndTime());
		}

		// 如果是周期任务，则许检查依赖周期是否正确
		if (job.getScheduleType().equals(JobScheduleType.CyleJob)
				&& job.getDependencies() != null
				&& job.getDependencies().size() != 0) {
			List<JobDescriptor> list = this.getJobDescriptors(job
					.getDependencies());
			jobValidate.checkCycleJob(job, list);
		}

		if (jobValidate.valide(job)) {
			getHibernateTemplate().update(persist);
		}
	}

	@Override
	public void saveJobAction(JobTaskAction actionPer) throws ZeusException{
		try{
			JobTaskAction action = getHibernateTemplate().get(JobTaskAction.class, actionPer.getId());
			if(action != null){
				if(action.getStatus() == null || !action.getStatus().equalsIgnoreCase("running")){
					actionPer.setHistoryId(action.getHistoryId());
					actionPer.setReadyDependency(action.getReadyDependency());
					actionPer.setStatus(action.getStatus());
				}else{
					actionPer = action;
				}
			}else{
				SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
				SimpleDateFormat df2 = new SimpleDateFormat("HH");
				String currentDateStr = df.format(new Date())+"0000";
				String currentHourStr = df2.format(new Date());
				if (Integer.parseInt(currentHourStr) > 8 && actionPer.getId() < Long.parseLong(currentDateStr)) {
					actionPer.setStatus("failed");
				}
			}
			if(actionPer.getAuto() == 0){
				if(actionPer.getStatus() == null || actionPer.getStatus().equalsIgnoreCase("wait")){
					actionPer.setStatus("failed");
				}
			}
			getHibernateTemplate().saveOrUpdate(actionPer);
		}catch(DataAccessException e){
			throw new ZeusException(e);
		}
	}

//	@Override
//	public void updateJobAction(JobDescriptor jobActionDescriptor) throws ZeusException {
//		JobTaskAction actionPer = PersistenceAndBeanConvert.convert(jobActionDescriptor);
//		saveJobAction(actionPer);
//	}

	@Override
	public Tuple<JobDescriptor, JobStatus> getJobDescriptor(String jobId) {
		JobTask persist = getJobPersistence(jobId);
		if (persist == null) {
			return null;
		}
		Tuple<JobDescriptor, JobStatus> t = PersistenceAndBeanConvert
				.convert(persist);
		JobDescriptor jd = t.getX();
		// 如果是周期任务，并且依赖不为空，则需要封装周期任务的依赖
		if (jd.getScheduleType() == JobScheduleType.CyleJob
				&& jd.getDependencies() != null) {
			JobTask jt;
			for (String jobID : jd.getDependencies()) {
				if (StringUtils.isNotEmpty(jobID)) {
					jt = getJobPersistence(jobID);
					if(jt!=null){
						jd.getDepdCycleJob().put(jobID, jt.getCycle());
					}
				}
			}

		}
		return t;
	}

	@Override
	public Tuple<JobDescriptor, JobStatus> getJobActionDescriptor(String actionId) {
		JobTaskAction persist = getJobActionPersistence(actionId);
		if (persist == null) {
			return null;
		}
		Tuple<JobDescriptor, JobStatus> t = PersistenceAndBeanConvert
				.convert(persist);
		JobDescriptor jd = t.getX();
		// 如果是周期任务，并且依赖不为空，则需要封装周期任务的依赖
		if (jd.getScheduleType() == JobScheduleType.CyleJob
				&& jd.getDependencies() != null) {
			JobTaskAction jp;
			for (String jobID : jd.getDependencies()) {
				if (StringUtils.isNotEmpty(jobID)) {
					jp = getJobActionPersistence(jobID);
					if(jp!=null){
						jd.getDepdCycleJob().put(jobID, jp.getCycle());
					}
				}
			}

		}
		return t;
	}

	@Override
	public GroupBean getUpstreamGroupBean(String jobId){
		Tuple<JobDescriptor,JobStatus> tuple=getJobDescriptor(jobId);
		return new JobBean(tuple.getX(),tuple.getY()).getGroupBean();
	}

	@Override
	public JobBean getUpstreamJobBean(String jobId) {
		return GroupManagerTool.getUpstreamJobBean(jobId, this, groupManager);
	}

	private JobTask getJobPersistence(String jobId) {
		JobTask persist = getHibernateTemplate().get(
				JobTask.class, Long.valueOf(jobId));
		if (persist == null) {
			return null;
		}
		return persist;
	}
	private JobTaskAction getJobActionPersistence(String actionId) {
		JobTaskAction persist = getHibernateTemplate().get(
				JobTaskAction.class, Long.valueOf(actionId));
		if (persist == null) {
			return null;
		}
		return persist;
	}
	private JobTaskHistory getJobHistoryPersistence(String historyId) {
		JobTaskHistory persist = getHibernateTemplate().get(
				JobTaskHistory.class, Long.valueOf(historyId));
		if (persist == null) {
			return null;
		}
		return persist;
	}

	@Override
	public Map<String, Tuple<JobDescriptor, JobStatus>> getJobDescriptor(
			final Collection<String> jobIds) {
		List<Tuple<JobDescriptor, JobStatus>> list = getHibernateTemplate()
				.execute(session -> {
					if (jobIds.isEmpty()) {
						return Collections.emptyList();
					}
					List<Long> ids = new ArrayList<>();
					for (String i : jobIds) {
						ids.add(Long.valueOf(i));
					}
					Query query = session
							.createQuery("from com.taobao.zeus.store.mysql.persistence.JobPersistence where id in (:list)");
					query.setParameterList("list", ids);
					List<JobTask> list1 = query.list();
					List<Tuple<JobDescriptor, JobStatus>> result = new ArrayList<>();
					if (list1 != null && !list1.isEmpty()) {
						for (JobTask persist : list1) {
							result.add(PersistenceAndBeanConvert
									.convert(persist));
						}
					}
					return result;
				});

		Map<String, Tuple<JobDescriptor, JobStatus>> map = new HashMap<>();
		for (Tuple<JobDescriptor, JobStatus> jd : list) {
			map.put(jd.getX().getJobId(), jd);
		}
		return map;
	}

	public List<JobDescriptor> getJobDescriptors(final Collection<String> jobIds) {
		List<JobDescriptor> list = getHibernateTemplate()
				.execute(session -> {
					if (jobIds.isEmpty()) {
						return Collections.emptyList();
					}
					List<Long> ids = new ArrayList<>();
					for (String i : jobIds) {
						if (StringUtils.isNotEmpty(i)) {
							ids.add(Long.valueOf(i));
						}
					}
					if (ids.isEmpty()) {
						return Collections.emptyList();
					}
					Query query = session
							.createQuery("from com.taobao.zeus.store.mysql.persistence.JobActionPersistence where id in (:list)");
					query.setParameterList("list", ids);
					List<JobTask> list1 = query.list();
					List<JobDescriptor> result = new ArrayList<>();
					if (list1 != null && !list1.isEmpty()) {
						for (JobTask persist : list1) {
							result.add(PersistenceAndBeanConvert.convert(
									persist).getX());
						}
					}
					return result;
				});
		return list;
	}

	@Override
	public void updateJobStatus(JobStatus jobStatus) {
		JobTaskAction persistence = getJobActionPersistence(jobStatus.getJobId());
		persistence.setGmtModified(new Date());

		// 只修改状态 和 依赖 2个字段
		JobTaskAction temp = PersistenceAndBeanConvert.convert(jobStatus);
		persistence.setStatus(temp.getStatus());
		persistence.setReadyDependency(temp.getReadyDependency());
		persistence.setHistoryId(temp.getHistoryId());

		getHibernateTemplate().update(persistence);
	}

	@Override
	public JobStatus getJobStatus(String jobId) {
		Tuple<JobDescriptor, JobStatus> tuple = getJobDescriptor(jobId);
		if (tuple == null) {
			return null;
		}
		return tuple.getY();
	}

	@Override
	public void grantJobOwner(String granter, String uid, String jobId)
			throws ZeusException {
		Tuple<JobDescriptor, JobStatus> job = getJobDescriptor(jobId);
		if (job != null) {
			job.getX().setOwner(uid);
			updateJob(granter, job.getX(), uid, job.getX().getGroupId());
		}
	}

	@Override
	public void moveJob(String uid, String jobId, String groupId)
			throws ZeusException {
		JobDescriptor jd = getJobDescriptor(jobId).getX();
		GroupDescriptor gd = groupManager.getGroupDescriptor(groupId);
		if (gd.isDirectory()) {
			throw new ZeusException("非法操作");
		}
		updateJob(uid, jd, jd.getOwner(), groupId);
	}

	@Override
	public List<String> getHosts() throws ZeusException {
		return getHibernateTemplate().execute(
				 session -> {
					Query query = session
							.createQuery("select host from com.taobao.zeus.store.mysql.persistence.ZeusWorker");
					return query.list();
				});
	}

	@Override
	public void replaceWorker(ZeusWorker zeusWorker) throws ZeusException {
		try {
			getHibernateTemplate().saveOrUpdate(zeusWorker);
		} catch (DataAccessException e) {
			throw new ZeusException(e);
		}

	}

	@Override
	public void removeWorker(String host) throws ZeusException {
		try {
			getHibernateTemplate().delete(
					getHibernateTemplate().get(ZeusWorker.class, host));
		} catch (DataAccessException e) {
			throw new ZeusException(e);
		}

	}

	
	@Override
	public List<JobTaskAction> getLastJobAction(String jobId) {
		assert getHibernateTemplate() != null;
		List<JobTaskAction> list = getHibernateTemplate().execute(
				session -> {
					Query query = session.createQuery("from com.taobao.zeus.store.mysql.persistence.JobTaskAction where jobId='" + jobId + "' order by id desc");
					return query.list();
				});
		return list;
	}

	@Override
	public List<Tuple<JobDescriptor, JobStatus>> getJobActionDescriptors(String jobId) {
		assert getHibernateTemplate() != null;
		List<JobTaskAction> list = getHibernateTemplate().execute(
				session -> {
					Query query = session.createQuery("from com.taobao.zeus.store.mysql.persistence.JobTaskAction where jobId='"+jobId+"' order by id desc");
					return query.list();
				});
		List<Tuple<JobDescriptor, JobStatus>> lst = new ArrayList<>();
		assert list != null;
		for(JobTaskAction persist : list){
			lst.add(PersistenceAndBeanConvert.convert(persist));
		}
		return lst;
	}


	@Override
	public void removeJob(Long actionId) throws ZeusException{
		try{
			assert getHibernateTemplate() != null;
			JobTaskAction action = getHibernateTemplate().get(JobTaskAction.class, actionId);
			if(action != null){
				getHibernateTemplate().delete(action);
			}
		}catch(DataAccessException e){
			throw new ZeusException(e);
		}
	}

	@Override
	public List<String> getDownstreamDependencies(String jobID) {
		return null;
	}

	@Override
	public List<String> getUpstreamDependencies(String jobID) {
		return null;
	}

	@Override
	public void updateJobActionList(JobDescriptor job) {

	}

	@Override
	public List<JobTask> getAllJobs() {
		return null;
	}


}