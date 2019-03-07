package com.taobao.zeus.client.operate;

import java.util.Map;

import com.taobao.zeus.model.JobHistory;

public interface ZeusHelperService {
	/**
	 * 获取Job所有的配置项(包含继承下来的配置项)
	 * @param jobId 任务ID
	 * @return map
	 */
	Map<String, String> getJobAllConfigurations(String jobId);
	/**
	 * 获取Job的一次运行记录
	 * @param historyId 历史任务ID
	 * @return jobHistory
	 */
	JobHistory getJobHistory(String historyId);
	
}
