package com.taobao.zeus.store.mysql.manager;

import java.util.List;

import com.taobao.zeus.model.FileDescriptor;

public interface FileManager {
	String PERSONAL="个人文档";
	String SHARE="共享文档";
	/**
	 * 添加文件/文件夹
	 */
	FileDescriptor addFile(String uid,String parentId,String name,boolean folder);
		
	/**
	 * 删除文件/文件夹
	 */
	void deleteFile(String fileId);
	/**
	 * 后台查询File最新内容
	 */
	FileDescriptor getFile(String id);
	
	void update(FileDescriptor fd);
	
	List<FileDescriptor> getSubFiles(String id);
	
	List<FileDescriptor> getUserFiles(String uid);
}
