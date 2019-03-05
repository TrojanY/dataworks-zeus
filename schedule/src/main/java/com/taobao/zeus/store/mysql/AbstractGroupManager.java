package com.taobao.zeus.store.mysql;

import com.taobao.zeus.model.GroupDescriptor;
import com.taobao.zeus.model.JobDescriptor;
import com.taobao.zeus.model.JobStatus;
import com.taobao.zeus.store.GroupBean;
import com.taobao.zeus.store.GroupManager;
import com.taobao.zeus.store.JobBean;
import com.taobao.zeus.util.Tuple;

import java.util.List;

public abstract class AbstractGroupManager implements GroupManager {
    @Override
    public GroupBean getDownstreamGroupBean(GroupBean parent) {
        if (parent.isDirectory()) {
            List<GroupDescriptor> children = getChildrenGroup(parent
                    .getGroupDescriptor().getId());
            for (GroupDescriptor child : children) {
                GroupBean childBean = new GroupBean(child);
                getDownstreamGroupBean(childBean);
                childBean.setParentGroupBean(parent);
                parent.getChildrenGroupBeans().add(childBean);
            }
        } else {
            List<Tuple<JobDescriptor, JobStatus>> jobs = getChildrenJob(parent
                    .getGroupDescriptor().getId());
            for (Tuple<JobDescriptor, JobStatus> tuple : jobs) {
                JobBean jobBean = new JobBean(tuple.getX(), tuple.getY());
                jobBean.setGroupBean(parent);
                parent.getJobBeans().put(tuple.getX().getId(), jobBean);
            }
        }

        return parent;
    }
}
