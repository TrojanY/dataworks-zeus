package com.taobao.zeus.web.platform.client.module.filemanager;

import java.util.List;

import com.google.gwt.user.client.ui.IsWidget;
import com.sencha.gxt.data.shared.TreeStore;

public interface FileManagerView extends IsWidget{

	void collapse();

	void editName(FileModel childFileModel);

	void expand();

	FileModel getSelectedItem();

	List<FileModel> getSelectedItems();

	void selectFileModel(FileModel fileModel);
	  
	TreeStore<FileModel> getMyTreeStore();
	  
	void setMyActivity();
	  
	void setSharedActivity();
}
