package com.taobao.zeus.web.platform.client.module.jobdisplay.job;

import java.util.Date;

import com.google.gwt.user.client.ui.Frame;
import com.sencha.gxt.widget.core.client.FramedPanel;
import com.sencha.gxt.widget.core.client.Window;
import com.sencha.gxt.widget.core.client.container.MarginData;
import com.sencha.gxt.widget.core.client.container.BoxLayoutContainer.BoxLayoutPack;

public class DefaultPanel extends Window {
	public DefaultPanel(final String id){
		setModal(true);
		setSize("800", "800");
//		setBorders(hidden);
//		setBodyStyle("border:none");
		setHeading("datax配置工具");
		
		FramedPanel fp=new FramedPanel();
		fp.setHeaderVisible(false);
		fp.setButtonAlign(BoxLayoutPack.CENTER);
		
		Date d = new Date();
		Frame frame = new Frame("lingoes.html?="+d.getTime());
		frame.setStylePrimaryName("lingoes");
		frame.getElement().getStyle().setProperty("border", "none");
		fp.add(frame);
		
		add(fp,new MarginData(5));
	}

}