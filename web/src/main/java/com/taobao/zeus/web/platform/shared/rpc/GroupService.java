package com.taobao.zeus.web.platform.shared.rpc;

import java.io.IOException;
import java.util.List;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;
import com.taobao.zeus.web.platform.client.module.jobmanager.GroupModel;
import com.taobao.zeus.web.platform.client.util.GwtException;
import com.taobao.zeus.web.platform.client.util.ZUser;

@RemoteServiceRelativePath("group.rpc")
public interface GroupService extends RemoteService {
	/**
	 * 创建一个分组
	 * @param groupName 组名称
	 * @param parentGroupId 父组ID
	 * @param isDirectory 是否文件夹
	 * @return 组ID
	 * @throws GwtException GwtException
	 */
	String createGroup(String groupName,String parentGroupId,boolean isDirectory) throws GwtException;

	/**
	 * 根据名称获取相应的组
	 * @param groupId 组名称或ID
	 * @return 组内存模型
	 * @throws GwtException GwtException
	 */
	GroupModel getGroup(String groupId) throws GwtException;

	/**
	 * 根据名称获取组的父组
	 * @param groupId 组名称或ID
	 * @return 父组内存模型
	 * @throws GwtException GwtException
	 */
	GroupModel getUpstreamGroup(String groupId) throws GwtException;

	/**
	 * 删除分组
	 * @param groupId 组名称或ID
	 * @throws GwtException GwtException
	 */
	void deleteGroup(String groupId) throws GwtException;

	/**
	 * 更新组信息
	 * @param group 组名称或ID
	 * @throws GwtException GwtException
	 */
	void updateGroup(GroupModel group) throws GwtException;

	/**
	 * 根据名称获取组的管理着
	 * @param groupId 组名称或ID
	 * @return 管理着列表
	 */
	List<ZUser> getGroupAdmins(String groupId);

	/**
	 * 添加组的管理着
	 * @param groupId 组名称或ID
	 * @param uid uid
	 * @throws GwtException GwtException
	 */
	void addGroupAdmin(String groupId,String uid) throws GwtException;

	/**
	 * 删除组的管理者
	 * @param groupId 组名称或ID
	 * @param uid uid
	 * @throws GwtException GwtException
	 */
	void removeGroupAdmin(String groupId,String uid) throws GwtException;

	/**
	 * 更换组的维护者
	 * @param groupId 组名称或ID
	 * @param uid uid
	 * @throws GwtException GwtException
	 */
	void transferOwner(String groupId,String uid) throws GwtException;
	/**
	 * 移动组
	 * @param groupId 组名称或ID
	 * @param newParentGroupId 父组ID
	 * @throws GwtException GwtException
	 */
	void move(String groupId,String newParentGroupId) throws GwtException;
}
