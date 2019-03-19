package com.taobao.zeus.broadcast.notify;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.taobao.zeus.store.mysql.manager.JobManager;
import com.taobao.zeus.store.mysql.manager.JobHistoryManager;

public abstract class AbstractJobResultNotify implements JobResultNotify{
	@Autowired
	protected JobHistoryManager jobHistoryManager;
	@Autowired
	@Qualifier("jobManager")
	protected JobManager jobManager;

}