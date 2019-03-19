package com.taobao.zeus.web.platform.shared.rpc;

import java.util.Date;
import java.util.List;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;
import com.sencha.gxt.data.shared.loader.PagingLoadConfig;
import com.sencha.gxt.data.shared.loader.PagingLoadResult;
import com.taobao.zeus.web.platform.client.module.jobdisplay.job.JobHistoryModel;
import com.taobao.zeus.web.platform.client.module.jobmanager.JobModel;
import com.taobao.zeus.web.platform.client.module.jobmanager.JobModelAction;
import com.taobao.zeus.web.platform.client.util.GwtException;
import com.taobao.zeus.web.platform.client.util.HostGroupModel;
import com.taobao.zeus.web.platform.client.util.ZUser;
import com.taobao.zeus.web.platform.client.util.ZUserContactTuple;

/**
 *
 * @author zhoufang
 */
@RemoteServiceRelativePath("job.rpc")
public interface JobService extends RemoteService {
	/**
	 * 创建一个Job任务
	 * @param jobName 任务名称
	 * @param parentGroupId 组ID
	 * @param jobType 任务类型
	 * @return 任务内存模型
	 * @throws GwtException GwtException
	 */
	JobModel createJob(String jobName,String parentGroupId,String jobType) throws GwtException;


	/**
	 * 获取任务的上游任务
	 * @param jobId
	 * @return
	 * @throws GwtException GwtException
	 */
	JobModel getUpstreamJob(String jobId) throws GwtException;
	
	JobModel updateJob(JobModel jobModel) throws GwtException;
	/**
	 * 开关
	 * @param auto auto
	 * @throws  GwtException
	 */
	List<Long> switchAuto(String jobId,Boolean auto) throws GwtException;
	/**
	 * 运行程序
	 * 1:手动运行
	 * 2:手动恢复
	 */
	void run(String jobId,int type) throws GwtException;
	/**
	 * 取消一个正在执行的任务
	 */
	void cancel(String historyId) throws GwtException;
	/**
	 * 分页查询Job任务的历史日志
	 */
	PagingLoadResult<JobHistoryModel> jobHistoryPaging(String jobId,PagingLoadConfig config);
	/**
	 * 获取Job任务的详细日志
	 */
	JobHistoryModel getJobHistory(String id);
	/**
	 * 获取Job的运行状态
	 */
	JobModel getJobStatus(String jobId);
	/**
	 * 获取组下的所有任务任务状态
	 */
	PagingLoadResult<JobModelAction> getSubJobStatus(String groupId,PagingLoadConfig config,Date startDate, Date endDate);
	/**
	 * 获取组下正在运行的自动job
	 */
//	@Deprecated
//	List<JobHistoryModel> getRunningJobs(String groupId);
	
	List<JobHistoryModel> getAutoRunning(String groupId);
	/**
	 * 获取正在运行的手动任务
	 */
//	@Deprecated
//	List<JobHistoryModel> getManualRunningJobs(String groupId);
	
	List<JobHistoryModel> getManualRunning(String groupId);
	/**
	 * 删除Job任务
	 */
	void deleteJob(String jobId) throws GwtException;
	
	void addJobAdmin(String jobId,String uid)throws GwtException;
	
	void removeJobAdmin(String jobId,String uid)throws GwtException;
	
	List<ZUser> getJobAdmins(String jobId);
	
	void transferOwner(String jobId,String uid) throws GwtException;
	/**
	 * 移动Job
	 * 将job移动到新的group下
	 */
	void move(String jobId,String newGroupId) throws GwtException;
	/**
	 * 同步任务脚本
	 * 给开发中心使用，方便开发中心直接同步脚本到调度中心
	 */
	void syncScript(String jobId,String script) throws GwtException;
	/**
	 * 获得该JOB ID下面的的所有ACTIONDI
	 * 给开发中心使用，方便开发中心直接同步脚本到调度中心
	 */
	List<Long> getJobACtion(String id);
	
	void grantImportantContact(String jobId, String uid)  throws GwtException;
	
	void revokeImportantContact(String jobId, String uid)  throws GwtException;
	
	List<ZUserContactTuple> getAllContactList(String jobId);
	
	List<String> getJobDependencies(String jobId) throws GwtException;
	
	PagingLoadResult<HostGroupModel> getHostGroup(PagingLoadConfig config);

	void syncScriptAndHostGroupId(String jobId, String script, String hostGroupId) throws GwtException;
	
	String getHostGroupNameById(String hostGroupId);
}
