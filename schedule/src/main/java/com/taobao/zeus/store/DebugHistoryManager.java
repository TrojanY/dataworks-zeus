package com.taobao.zeus.store;

import java.util.List;

import com.taobao.zeus.model.DebugHistory;

public interface DebugHistoryManager {

	DebugHistory addDebugHistory(DebugHistory history);
	
	DebugHistory findDebugHistory(String id);

	List<DebugHistory> pagingList(final String fileId,final int start,final int limit);
	int pagingTotal(String jobId);
	
	void updateDebugHistoryLog(String id,String log);
	/**
	 * 更新JobLogHistory，但是不包括log字段
	 * @param history
	 */
	void updateDebugHistory(DebugHistory history);
	
}
