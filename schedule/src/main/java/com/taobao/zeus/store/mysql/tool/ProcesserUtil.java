package com.taobao.zeus.store.mysql.tool;

import com.taobao.zeus.model.processor.*;
import net.sf.json.JSONObject;

import com.taobao.zeus.model.processor.Processor;

@SuppressWarnings("deprecation")
public class ProcesserUtil {

	public static Processor parse(JSONObject o){
		Processor result=null;
		String id=o.getString("id");
		if("download".equals(id)){
			result= new DownloadProcessor();
		}else if("zookeeper".equalsIgnoreCase(id)){
			result=new ZooKeeperProcessor();
			result.parse(o.getString("config"));
		}else if("mail".equalsIgnoreCase(id)){
			result=new MailProcessor();
			result.parse(o.getString("config"));
		}else if("meta".equalsIgnoreCase(id)){
			result=new MetaProcessor();
			result.parse(o.getString("config"));
		}else if("wangwang".equalsIgnoreCase(id)){
			result=new WangWangProcessor();
			result.parse(o.getString("config"));
		}else if("OutputCheck".equalsIgnoreCase(id)){
			result=new OutputCheckProcessor();
			result.parse(o.getString("config"));
		}else if("OutputClean".equalsIgnoreCase(id)){
			result=new OutputCleanProcessor();
			result.parse(o.getString("config"));
		}else if("JobProcessor".equalsIgnoreCase(id)){
			result=new JobProcessor();
			result.parse(o.getString("config"));
		}else if("hive".equalsIgnoreCase(id)){
			result = new HiveProcessor();
			result.parse(o.getString("config"));
		}
		
		return result;
	}
	
}
