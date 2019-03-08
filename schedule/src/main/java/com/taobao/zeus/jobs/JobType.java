package com.taobao.zeus.jobs;

public enum JobType{
    HiveJob("HiveJob"), MapReduceJob("MapReduceJob");
    private String type;
    JobType(String type){this.type = type;}
    public String value(){return type;}
}
