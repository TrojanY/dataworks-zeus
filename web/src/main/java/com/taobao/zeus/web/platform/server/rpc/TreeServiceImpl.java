package com.taobao.zeus.web.platform.server.rpc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.taobao.zeus.model.JobDescriptor;
import com.taobao.zeus.store.GroupBean;
import com.taobao.zeus.store.JobBean;
import com.taobao.zeus.store.mysql.manager.FollowManager;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.taobao.zeus.model.JobHistory;
import com.taobao.zeus.model.ZeusFollow;
import com.taobao.zeus.store.mysql.manager.JobHistoryManager;
import com.taobao.zeus.store.mysql.impl.ReadOnlyGroupManager;
import com.taobao.zeus.web.LoginUser;
import com.taobao.zeus.web.platform.client.module.jobmanager.GroupJobTreeModel;
import com.taobao.zeus.web.platform.shared.rpc.TreeService;

public class TreeServiceImpl implements TreeService{
	@Autowired
	private ReadOnlyGroupManager readOnlyGroupManager;
	@Autowired
	private FollowManager followManager;
	@Autowired
	private JobHistoryManager jobHistoryManager;
	
	private static Logger log=LoggerFactory.getLogger(TreeServiceImpl.class);
	@Override
	public GroupJobTreeModel getMyTreeData() {
		String uid=LoginUser.getUser().getUid();
		GroupBean rootGroup=readOnlyGroupManager.getGlobeGroupBeanForTreeDisplay(true);
		Map<String, JobBean> allJobs=rootGroup.getAllSubJobBeans();
		for(String key:allJobs.keySet()){
			JobBean bean=allJobs.get(key);
			//不是owner，删除
			if(!bean.getJobDescriptor().getOwner().equals(uid)){
				bean.getGroupBean().getJobBeans().remove(key);
			}
		}
		
		Map<String, GroupBean> allGroups=rootGroup.getAllSubGroupBeans();
		List<GroupBean> leafGroups=new ArrayList<>();
		for(GroupBean bean:allGroups.values()){
			if(!bean.isDirectory() || bean.getChildrenGroupBeans().isEmpty()){
				leafGroups.add(bean);
			}
		}
		for(GroupBean bean:leafGroups){
			recursionRemove(bean, uid);
		}
		
		return getTreeData(rootGroup);
	}
	private void recursionRemove(GroupBean bean,String uid){
		if(!bean.isDirectory()){
			if(!bean.getGroupDescriptor().getOwner().equals(uid) && bean.getJobBeans().isEmpty()){
				GroupBean parent=bean.getParentGroupBean();
				if(parent!=null){
					parent.getChildrenGroupBeans().remove(bean);
					recursionRemove(parent, uid);
				}
			}
		}else{
			if(!bean.getGroupDescriptor().getOwner().equals(uid) && bean.getChildrenGroupBeans().isEmpty()){
				GroupBean parent=bean.getParentGroupBean();
				if(parent!=null){
					parent.getChildrenGroupBeans().remove(bean);
					recursionRemove(parent, uid);
				}
			}
		}
	}
	@Override
	public GroupJobTreeModel getTreeData() {
		GroupBean globe;
		globe=readOnlyGroupManager.getGlobeGroupBeanForTreeDisplay(false);
		return getTreeData(globe);
	}
	private GroupJobTreeModel getTreeData(GroupBean rootGroup) {
		
		String uid=LoginUser.getUser().getUid();
		List<ZeusFollow> list=followManager.findAllTypeFollows(uid);
		Map<String, Boolean> groupFollow=new HashMap<>();
		Map<String, Boolean> jobFollow=new HashMap<>();
		for(ZeusFollow f:list){
			if(ZeusFollow.GroupType.equals(f.getType())){
				groupFollow.put(f.getTargetId(), true);
			}else if(ZeusFollow.JobType.equals(f.getType())){
				jobFollow.put(f.getTargetId(), true);
			}
		}
		
		GroupJobTreeModel root=new GroupJobTreeModel();
		root.setName(rootGroup.getGroupDescriptor().getName());
		root.setId(rootGroup.getGroupDescriptor().getId());
		root.setGroup(true);
		root.setDirectory(true);
		root.setJob(false);
		root.setOwner(rootGroup.getGroupDescriptor().getOwner());
		Boolean isFollow=groupFollow.get(rootGroup.getGroupDescriptor().getId());
		root.setFollow(isFollow==null?false:isFollow);
		
		
		
		setGroup(root,rootGroup.getChildrenGroupBeans(),groupFollow,jobFollow);
		
		
		return root;
	}
	
	private void setGroup(GroupJobTreeModel parent,List<GroupBean> children,Map<String, Boolean> groupFollow,Map<String, Boolean> jobFollow){
		children.sort((o1, o2) -> o1.getGroupDescriptor().getName().compareToIgnoreCase(o2.getGroupDescriptor().getName()));
		for(GroupBean g:children){
			if(g.isExisted()){
				GroupJobTreeModel group=new GroupJobTreeModel();
				group.setName(g.getGroupDescriptor().getName());
				group.setId(g.getGroupDescriptor().getId());
				group.setGroup(true);
				group.setJob(false);
				group.setOwner(g.getGroupDescriptor().getOwner());
				group.setDirectory(g.isDirectory());
				Boolean follow=groupFollow.get(g.getGroupDescriptor().getId());
				group.setFollow(follow != null && (follow));
				parent.getChildren().add(group);
				if(g.isDirectory()){
					setGroup(group,g.getChildrenGroupBeans(),groupFollow,jobFollow);
				}else{
					List<JobBean> list = new ArrayList<>(g.getJobBeans().values());
					list.sort(Comparator.comparing(o -> o.getJobDescriptor().getName()));
					for(JobBean jb:list){
						GroupJobTreeModel job=new GroupJobTreeModel();
						job.setId(jb.getJobDescriptor().getJobId());
						job.setGroup(false);
						job.setDirectory(false);
						job.setName(jb.getJobDescriptor().getName());
						job.setJob(true);
						Boolean jFollow=jobFollow.get(job.getId());
						job.setFollow(jFollow != null && (jFollow));
						group.getChildren().add(job);
					}
				}
			}
		}
	}

	@Override
	public void follow(int type, String targetId) {
		followManager.addFollow(LoginUser.getUser().getUid(), type, targetId);
	}

	@Override
	public void unfollow(int type, String targetId) {
		followManager.deleteFollow(LoginUser.getUser().getUid(), type, targetId);
	}

	@Override
	public GroupJobTreeModel getDependeeTree(String jobId) {
		GroupBean globe=readOnlyGroupManager.getGlobeGroupBean();
		JobBean jb=globe.getAllSubJobBeans().get(jobId);
		if(jb!=null){
			GroupJobTreeModel root=new GroupJobTreeModel();
			root.setName(jb.getJobDescriptor().getName());
			root.setId(jb.getJobDescriptor().getJobId());
			root.setGroup(false);
			root.setDirectory(!jb.getDependee().isEmpty());
			root.setJob(true);
			root.setOwner(jb.getJobDescriptor().getOwner());
			
			setJob(root,jb.getDependee(),true);
			return root;
		}
		return null;
	}
	private void setJob(GroupJobTreeModel parent,Collection<JobBean> children,boolean dependee){
		for(JobBean g:children){
			GroupJobTreeModel job=new GroupJobTreeModel();
			job.setName(g.getJobDescriptor().getName());
			job.setId(g.getJobDescriptor().getJobId());
			job.setGroup(false);
			job.setJob(true);
			job.setOwner(g.getJobDescriptor().getOwner());
			Boolean dir;
			Collection<JobBean> childs;
			if(dependee){
				dir= !g.getDependee().isEmpty();
				childs=g.getDependee();
			}else{
				dir= !g.getDepender().isEmpty();
				childs=g.getDepender();
			}
			job.setDirectory(dir);
			parent.getChildren().add(job);
			if(!childs.isEmpty()){
				setJob(job,childs,dependee);
			}
		}
	}

	@Override
	public GroupJobTreeModel getDependerTree(String jobId) {
		GroupBean globe=readOnlyGroupManager.getGlobeGroupBean();
		JobBean jb=globe.getAllSubJobBeans().get(jobId);
		if(jb!=null){
			GroupJobTreeModel root=new GroupJobTreeModel();
			root.setName(jb.getJobDescriptor().getName());
			root.setId(jb.getJobDescriptor().getJobId());
			root.setGroup(false);
			root.setDirectory(!jb.getDepender().isEmpty());
			root.setJob(true);
			root.setOwner(jb.getJobDescriptor().getOwner());
			
			setJob(root,jb.getDepender(),false);
			return root;
		}
		return null;
	}

	@Override
	public String getDependeeTreeJson(String jobId) {
		try {
			return getJsonData(getDependeeTree(jobId)).toString();
		} catch (Exception e) {
			log.error("getDependeeTreeJson",e);
			throw new RuntimeException(e.getMessage());
		}
	}
	
	private JSONObject getJsonData(GroupJobTreeModel result) throws JSONException{
		JSONObject jsonData = new JSONObject();
		//JSONObject data = new JSONObject();
		jsonData.put("id", result.getId()+"-"+new Random().nextInt(1000));
		jsonData.put("name", result.getName());
		Map<String, JobHistory> map=jobHistoryManager.findLastHistoryByList(Arrays.asList(result.getId()));
		JobHistory his=map.get(result.getId());
		JSONObject data=new JSONObject();
		if(his==null){
			data.put("jobId",result.getId());
			data.put("historyId", "");
			data.put("lastStatus", "");
			data.put("lastRuntime", 0);
		}else{
			data.put("jobId",result.getId());
			data.put("historyId", his.getId());
			data.put("lastStatus", his.getStatus()==null?"":his.getStatus().toString());
			data.put("lastRuntime", his.getStartTime()==null?null:his.getStartTime().getTime());
		}
		jsonData.put("data", data);
		if(result.getChildren().size()>0)
			jsonData.put("children", getChildren(result));
		else
			jsonData.put("children", Collections.EMPTY_LIST);
		return jsonData;
	}
	private ArrayList<JSONObject> getChildren(GroupJobTreeModel result) throws JSONException{
		ArrayList<JSONObject> children = new ArrayList<>();
		for(GroupJobTreeModel child : result.getChildren()){
			children.add(getJsonData(child));
		}
		return children;
	}
	@Override
	public String getDependerTreeJson(String jobId) {
		try {
			return getJsonData(getDependerTree(jobId)).toString();
		} catch (Exception e) {
			log.error("getDependerTreeJson",e);
			throw new RuntimeException(e.getMessage());
		}
	}
	@Override
	public GroupJobTreeModel getTreeDataOfOtherDependentJob(String jobId) {
		GroupBean rootGroup = readOnlyGroupManager
				.getGlobeGroupBeanForTreeDisplay(true);
		Map<String, JobBean> allJobs = rootGroup.getAllSubJobBeans();
		for (String key : allJobs.keySet()) {
			JobBean bean = allJobs.get(key);
			if (!JobDescriptor.JobScheduleType.Dependent.equals(bean.getJobDescriptor().getScheduleType()) ) {
				bean.getGroupBean().getJobBeans().remove(key);
			}
			if (bean.getJobDescriptor().getJobId().equals(jobId)) {
				bean.getGroupBean().getJobBeans().remove(key);
			}
		}
		return getTreeData(rootGroup);
	}
	
}