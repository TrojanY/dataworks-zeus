package com.taobao.zeus.store.mysql.manager;

import java.util.List;

import com.taobao.zeus.store.mysql.persistence.ZeusUser;


public interface UserManager{
	
	List<ZeusUser> getAllUsers();
	
	ZeusUser findByUid(String uid);
	
	List<ZeusUser> findListByUid(List<String> uids);
	
	ZeusUser addOrUpdateUser(ZeusUser user);

	List<ZeusUser> findListByUidByOrder(List<String> uids);
	
	//2015-02-04 add--------
	ZeusUser findByUidFilter(String uid);
	
	List<ZeusUser> findAllUsers(String sortField, String sortOrder);
	
	List<ZeusUser> findListByFilter(String filter, String sortField, String sortOrder);
	
	void update(ZeusUser user);
}
