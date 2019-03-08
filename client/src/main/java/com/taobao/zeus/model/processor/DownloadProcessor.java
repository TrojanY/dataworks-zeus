package com.taobao.zeus.model.processor;
/**
 * 下载资源文件
 * @author zhoufang
 *
 */
public class DownloadProcessor implements Processor {

	private static final long serialVersionUID = 1L;

	@Override
	public String getConfig() {
		return "";
	}

	@Override
	public String getId() {
		return "download";
	}

	@Override
	public void parse(String configs) {
		
	}
}
