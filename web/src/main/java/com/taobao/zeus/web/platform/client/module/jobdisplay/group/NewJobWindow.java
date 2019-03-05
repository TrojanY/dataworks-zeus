package com.taobao.zeus.web.platform.client.module.jobdisplay.group;

import java.util.HashMap;
import java.util.Map;

import com.sencha.gxt.cell.core.client.form.ComboBoxCell.TriggerAction;
import com.sencha.gxt.data.shared.LabelProvider;
import com.sencha.gxt.data.shared.ListStore;
import com.sencha.gxt.data.shared.ModelKeyProvider;
import com.sencha.gxt.widget.core.client.Window;
import com.sencha.gxt.widget.core.client.button.TextButton;
import com.sencha.gxt.widget.core.client.container.VerticalLayoutContainer;
import com.sencha.gxt.widget.core.client.container.VerticalLayoutContainer.VerticalLayoutData;
import com.sencha.gxt.widget.core.client.event.SelectEvent;
import com.sencha.gxt.widget.core.client.event.SelectEvent.SelectHandler;
import com.sencha.gxt.widget.core.client.form.ComboBox;
import com.sencha.gxt.widget.core.client.form.FieldLabel;
import com.sencha.gxt.widget.core.client.form.FormPanel;
import com.sencha.gxt.widget.core.client.form.TextField;
import com.taobao.zeus.web.platform.client.module.jobmanager.GroupModel;
import com.taobao.zeus.web.platform.client.module.jobmanager.JobModel;
import com.taobao.zeus.web.platform.client.module.jobmanager.TreeKeyProviderTool;
import com.taobao.zeus.web.platform.client.module.jobmanager.event.TreeNodeChangeEvent;
import com.taobao.zeus.web.platform.client.util.PlatformBus;
import com.taobao.zeus.web.platform.client.util.RPCS;
import com.taobao.zeus.web.platform.client.util.async.AbstractAsyncCallback;

public class NewJobWindow extends Window{

	private FormPanel formPanel;
	
	private GroupModel groupModel;
	
	private TextButton save=new TextButton("保存", new SelectHandler() {
		@Override
		public void onSelect(SelectEvent event) {
			if(formPanel.isValid()){
				RPCS.getJobService().createJob(name.getValue(), 
						groupModel.getId(), jobType.getValue().get("name"), new AbstractAsyncCallback<JobModel>() {
							@Override
							public void onSuccess(JobModel result) {
								TreeNodeChangeEvent event=new TreeNodeChangeEvent();
								event.setNeedSelectProviderKey(TreeKeyProviderTool.genJobProviderKey(result.getId()));
								bus.fireEvent(event);
								NewJobWindow.this.hide();
							}
				});
			}
		}
	});
	private PlatformBus bus;
	private ComboBox<Map<String, String>> jobType;
	private TextField name;
	public NewJobWindow(PlatformBus bus,GroupModel groupModel){
		this.bus=bus;
		this.groupModel=groupModel;
		setModal(true);
		setHeight(150);
		setWidth(350);
		
		setHeading(groupModel.getName()+" 下新建任务");
		
		formPanel=new FormPanel();
		VerticalLayoutContainer p=new VerticalLayoutContainer();
		formPanel.add(p);
		
		name=new TextField();
		name.setAllowBlank(false);
		p.add(new FieldLabel(name, "任务名称"),new VerticalLayoutData(1,-1));
		
		ListStore<Map<String, String>> jobTypeStore=new ListStore<>(item -> item.get("name"));
		Map<String, String> javamain=new HashMap<>();
		javamain.put("name", JobModel.MapReduce);
		jobTypeStore.add(javamain);
		Map<String, String> shell=new HashMap<>();
		shell.put("name", JobModel.SHELL);
		jobTypeStore.add(shell);
		Map<String, String> hive=new HashMap<>();
		hive.put("name", JobModel.HIVE);
		jobTypeStore.add(hive);
		jobType=new ComboBox<>(jobTypeStore, item -> item.get("name"));
		jobType.setAllowBlank(false);
		jobType.setTypeAhead(true);
		jobType.setTriggerAction(TriggerAction.ALL);
		jobType.setEditable(false);
		
		
		jobType.setStore(jobTypeStore);
		
		p.add(new FieldLabel(jobType, "任务类型"),new VerticalLayoutData(1, -1));
		
		add(formPanel);
		addButton(save);
	}
}
