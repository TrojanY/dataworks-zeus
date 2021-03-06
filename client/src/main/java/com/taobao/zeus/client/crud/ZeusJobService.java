package com.taobao.zeus.client.crud;

import com.taobao.zeus.client.ZeusException;
import com.taobao.zeus.model.JobDescriptor;

public interface ZeusJobService {

	JobDescriptor createJob(String uid,String jobName,String parentGroup,JobDescriptor.JobRunType jobType) throws ZeusException;
	
	void updateJob(String uid,JobDescriptor jobDescriptor) throws ZeusException;
	
	void deleteJob(String uid,String jobId) throws ZeusException;
	
	JobDescriptor getJobDescriptor(String jobId);
	
}
