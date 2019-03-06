package com.taobao.zeus.client.crud;

import com.taobao.zeus.client.ZeusException;
import com.taobao.zeus.model.JobDescriptor;
import com.taobao.zeus.model.JobDescriptor.JobRunType;

public interface ZeusJobService_Old {

	JobDescriptor createJob(String uid,String jobName,String parentGroup,JobRunType jobType) throws ZeusException;
	
	void updateJob(String uid,JobDescriptor jobDescriptor) throws ZeusException;
	
	void deleteJob(String uid,String jobId) throws ZeusException;
	
	JobDescriptor getJobDescriptor(String jobId);
	
}
