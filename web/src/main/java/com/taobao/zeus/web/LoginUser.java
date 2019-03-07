package com.taobao.zeus.web;

import com.taobao.zeus.store.mysql.persistence.ZeusUser;

public class LoginUser {
	public static ThreadLocal<ZeusUser> user=new ThreadLocal<>();
	
	public static ZeusUser getUser(){
		return user.get();
	}

	public static void setUser(ZeusUser user) {
		LoginUser.user.set(user);
	}
	
}
