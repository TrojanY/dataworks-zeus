package com.taobao.zeus.store.mysql.manager;

import com.taobao.zeus.model.Profile;

public interface ProfileManager {

	Profile findByUid(String uid);
	
	void update(String uid,Profile p) throws Exception;
}
