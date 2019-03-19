package com.taobao.zeus.store.mysql.manager;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.taobao.zeus.client.ZeusException;
import com.taobao.zeus.model.JobDescriptor;
import com.taobao.zeus.model.JobDescriptor.JobRunType;
import com.taobao.zeus.model.JobStatus;
import com.taobao.zeus.store.GroupBean;
import com.taobao.zeus.store.JobBean;
import com.taobao.zeus.store.mysql.persistence.JobTask;
import com.taobao.zeus.store.mysql.persistence.JobTaskAction;
import com.taobao.zeus.store.mysql.persistence.ZeusWorker;
import com.taobao.zeus.util.Tuple;


public interface JobManager {

	JobDescriptor createJob(String user, String jobName, String parentGroup, JobRunType jobType) throws ZeusException;
	/**
	 * 删除一个Job
	 * 1.该job没有被其他job依赖
	 * 删除操作完成后，全量重新加载配置
	 */
	void deleteJob(String user,String jobId) throws ZeusException;
	/**
	 * 更新Job
	 */
	void updateJob(String user,JobDescriptor job) throws ZeusException;
	/**
	 * 保存jobAction
	 */
	void saveJobAction(JobTaskAction action) throws ZeusException;
	/**
	 * 保存jobAction
	 * @throws ZeusException ZeusException
	 */
	//void updateJobAction(JobDescriptor actionDescriptor) throws ZeusException;
	/**
	 * 移除jobAction
	 */
	void removeJob(Long actionId) throws ZeusException;
	/**
	 * 获取最近的jobAction
	 */
	List<JobTaskAction> getLastJobAction(String jobId);
	/**
	 * 根据界面job信息批量更新anction列表
	 */
	void updateJobActionList(JobDescriptor job);
	/**
	 * 得到所有下游依赖于jobID的JobID列表
	 */
	List<String> getDownstreamDependencies(String jobID);
	/**
	 * 得到所有上游需要依赖的jobID列表
	 */
	List<String> getUpstreamDependencies(String jobID);

	List<JobTask> getAllJobs();

	/**
	 * 根据jobid查询job的记录信息
	 */
	Tuple<JobDescriptor,JobStatus> getJobDescriptor(String jobId);
	/**
	 * 根据actionid查询action的记录信息
	 */
	Tuple<JobDescriptor, JobStatus> getJobActionDescriptor(String jobId);
	/**
	 * 批量查询Job信息
	 */
	Map<String, Tuple<JobDescriptor, JobStatus>> getJobDescriptor(Collection<String> jobIds);
	/**
	 * 获取jobAction列表
	 */
	List<Tuple<JobDescriptor, JobStatus>> getJobActionDescriptors(String jobId);

	/**
	 * 根据JobId查询Job信息
	 * 向上查询所有的组信息
	 */
	GroupBean getUpstreamGroupBean(String jobId);

	JobBean getUpstreamJobBean(String jobId);

	/**
	 * 查询Job状态
	 */
	JobStatus getJobStatus(String jobId);
	/**
	 * 更新Job状态
	 */
	void updateJobStatus(JobStatus jobStatus);
	
	void grantJobOwner(String granter,String uid,String jobId)throws ZeusException;
	
	void moveJob(String uid,String jobId,String groupId) throws ZeusException;

	/**
	 * 获取worker列表
	 */
	List<String> getHosts() throws ZeusException;
	/**
	 * 保存或者更新worker，如果存在则更新
	 */
	void replaceWorker(ZeusWorker zeusWorker) throws ZeusException;
	
	/**
	 * 删除过期worker
	 */
	void removeWorker(String host) throws ZeusException;
}
