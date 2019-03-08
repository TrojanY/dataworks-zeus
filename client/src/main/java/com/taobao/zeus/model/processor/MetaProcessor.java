package com.taobao.zeus.model.processor;
/**
 * 已经废弃，Meta消息将会默认发送，无需配置
 * @author zhoufang
 *
 */
@Deprecated
public class MetaProcessor implements Processor {

	private static final long serialVersionUID = 1L;

	@Override
	public String getConfig() {
		return "";
	}

	@Override
	public String getId() {
		return "meta";
	}

	@Override
	public void parse(String config) {
		
	}

}
