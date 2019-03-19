package com.taobao.zeus.store.mysql.manager;

import java.util.List;

import com.taobao.zeus.client.ZeusException;
import com.taobao.zeus.model.GroupDescriptor;
import com.taobao.zeus.model.JobDescriptor;
import com.taobao.zeus.model.JobStatus;
import com.taobao.zeus.store.GroupBean;
import com.taobao.zeus.util.Tuple;


public interface GroupManager {
	/**
	 * 获取根节点的组ID
	 */
	String getRootGroupId();
	/**
	 * 获取根节点Group
	 * 包含完整的树结构信息
	 */
	GroupBean getGlobeGroupBean();
	/**
	 * 根据组ID查询组信息
	 * 向上查询该组上的所有组信息
	 */
	GroupBean getUpstreamGroupBean(String groupId);
	/**
	 * 根据组ID查询组信息
	 * 向下查询该组下的所有组信息以及Job信息
	 */
	GroupBean getDownstreamGroupBean(String groupId);

	GroupBean getDownstreamGroupBean(GroupBean parent);
	/**
	 * 根据groupId查询该组的记录
	 */
	GroupDescriptor getGroupDescriptor(String groupId);
	/**
	 * 获取组下的组
	 */
	List<GroupDescriptor> getChildrenGroup(String groupId);
	/**
	 * 获取组下的job
	 */
	List<Tuple<JobDescriptor,JobStatus>> getChildrenJob(String groupId);
	/**
	 * 创建一个group
	 */
	GroupDescriptor createGroup(String user,String groupName,String parentGroup,boolean isDirectory) throws ZeusException;
	/**
	 * 删除组，成功删除需要的条件：
	 * 1.操作人是该组的创建者
	 * 2.该组下的任务没有被其他组依赖
	 */
	void deleteGroup(String user,String groupId) throws ZeusException;
	/**
	 * 更新Group
	 */
	void updateGroup(String user,GroupDescriptor group) throws ZeusException;

	void grantGroupOwner(String granter,String uid,String groupId)throws ZeusException;

	void moveGroup(String uid,String groupId,String newParentGroupId) throws ZeusException;

	boolean IsExistedBelowRootGroup(String GroupName);

}
