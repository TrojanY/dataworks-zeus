package com.taobao.zeus.store.mysql.manager;

import java.util.List;
import java.util.Map;

import com.taobao.zeus.model.HostGroupCache;
import com.taobao.zeus.store.mysql.persistence.HostGroup;

public interface HostGroupManager{
	public HostGroup getHostGroupName(String hostGroupId);
	
	public Map<String,HostGroupCache> getAllHostGroupInfomations();
	
	public List<HostGroup> getAllHostGroup();
	
	public List<String> getPreemptionHost();
}
