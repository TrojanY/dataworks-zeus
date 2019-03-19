package com.taobao.zeus.web.platform.shared.rpc;

import java.util.List;

import com.google.gwt.user.client.rpc.RemoteService;
import com.google.gwt.user.client.rpc.RemoteServiceRelativePath;
import com.sencha.gxt.data.shared.loader.FilterPagingLoadConfig;
import com.sencha.gxt.data.shared.loader.PagingLoadResult;
import com.taobao.zeus.web.platform.client.module.tablemanager.model.PartitionModel;
import com.taobao.zeus.web.platform.client.module.tablemanager.model.TableModel;
import com.taobao.zeus.web.platform.client.module.tablemanager.TablePreviewModel;
import com.taobao.zeus.web.platform.client.util.GwtException;

/**
 * hive表管理服务
 * 
 * @author gufei.wzy 2012-9-17
 */
@RemoteServiceRelativePath("table.rpc")
public interface TableManagerService extends RemoteService {
	
	

	/**
	 * 获取一张hive表的model
	 * 
	 * @param tableName 表名
	 * @return 表模型
	 */
	TableModel getTableModel(String dataBaseName, String tableName);

	PagingLoadResult<TableModel> getPagingTables(
			FilterPagingLoadConfig loadConfigString, String uid, String dbName) throws GwtException;

	/**
	 * 获取预览数据
	 * @param  model model
	 * @return TablePreviewModel
	 * @throws GwtException GwtException
	 */
	TablePreviewModel getPreviewData(PartitionModel model) throws GwtException;

	List<PartitionModel> getPartitions(TableModel t) throws GwtException;

	PartitionModel fillPartitionSize(PartitionModel p);


}
