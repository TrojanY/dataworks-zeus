package com.taobao.zeus.store.mysql.manager;

import java.util.List;

import com.taobao.zeus.model.ZeusFollow;


public interface FollowManager {
	/**
	 * 新增一个关注
	 */
	ZeusFollow addFollow(String uid,Integer type,String targetId) ;
	/**
	 * 删除一个关注
	 */
	void deleteFollow(String uid,Integer type,String targetId);
	/**
	 * 查询用户的所有关注
	 */
	List<ZeusFollow> findAllTypeFollows(String uid);
	/**
	 * 查询关注的所有Group
	 */
	List<ZeusFollow> findFollowedGroups(String uid);
	/**
	 * 查询关注的所有Job
	 */
	List<ZeusFollow> findFollowedJobs(String uid);
	/**
	 * 查询关注该Job的人员名单
	 */
	List<ZeusFollow> findJobFollowers(String jobId);
	/**
	 * 查询关注该group的人员名单
	 */
	List<ZeusFollow> findGroupFollowers(List<String> groupIds);
	/**
	 * 查询实际关注该job的人
	 * 综合考虑了job自身被关注的人，以及上层group被关注的人
	 */
	List<String> findActualJobFollowers(String jobId);
	/**
	 * 查询所有关注该job的人
	 * 综合考虑了job自身被关注的人，以及上层group被关注的人
	 */
	List<ZeusFollow> findAllFollowers(String jobId);
	/**
	 * 添加zeusfollow重要联系人
	 */
	void grantImportantContact(String jobId, String uid);

	void revokeImportantContact(String jobId, String uid);
}
