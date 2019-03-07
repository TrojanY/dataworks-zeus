package com.taobao.zeus.client.crud;

import com.taobao.zeus.client.ZeusException;
import com.taobao.zeus.model.JobDescriptorOld;

public interface ZeusJobService_Old {

	JobDescriptorOld createJob(String uid,String jobName,String parentGroup,JobDescriptorOld.JobRunTypeOld jobType) throws ZeusException;
	
	void updateJob(String uid,JobDescriptorOld jobDescriptor) throws ZeusException;
	
	void deleteJob(String uid,String jobId) throws ZeusException;

	JobDescriptorOld getJobDescriptor(String jobId);
	
}
