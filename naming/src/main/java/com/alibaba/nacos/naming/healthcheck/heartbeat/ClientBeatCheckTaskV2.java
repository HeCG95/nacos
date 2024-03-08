/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.healthcheck.heartbeat;

import com.alibaba.nacos.common.task.AbstractExecuteTask;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.core.v2.client.impl.IpPortBasedClient;
import com.alibaba.nacos.naming.core.v2.pojo.HealthCheckInstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.healthcheck.NacosHealthCheckTask;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.sys.utils.ApplicationUtils;

import java.util.Collection;

/**
 * Client beat check task of service for version 2.x.
 * 处理心跳相关的状态
 * @author nkorange
 */
public class ClientBeatCheckTaskV2 extends AbstractExecuteTask implements BeatCheckTask, NacosHealthCheckTask {
    
    private final IpPortBasedClient client;
    
    private final String taskId;
    
    private final InstanceBeatCheckTaskInterceptorChain interceptorChain;// 拦截器链：拦截器列表对将要进行的任务进行拦截处理
    
    public ClientBeatCheckTaskV2(IpPortBasedClient client) {
        this.client = client;
        this.taskId = client.getResponsibleId();
        this.interceptorChain = InstanceBeatCheckTaskInterceptorChain.getInstance();
    }
    
    public GlobalConfig getGlobalConfig() {
        return ApplicationUtils.getBean(GlobalConfig.class);
    }
    
    @Override
    public String taskKey() {
        return KeyBuilder.buildServiceMetaKey(client.getClientId(), String.valueOf(client.isEphemeral()));
    }
    
    @Override
    public String getTaskId() {
        return taskId;
    }
    
    @Override
    public void doHealthCheck() {
        try {
            Collection<Service> services = client.getAllPublishedService();// 获取当前客户端下所有的服务
            for (Service each : services) {
                HealthCheckInstancePublishInfo instance = (HealthCheckInstancePublishInfo) client
                        .getInstancePublishInfo(each);// 获取Service对应的InstancePublishInfo
                interceptorChain.doInterceptor(new InstanceBeatCheckTask(client, each, instance));// 创建一个InstanceBeatCheckTask[task 内部维护了一个Checker列表，用于添加额外的检查器]，并交由拦截器链处理
            }
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("Exception while processing client beat time out.", e);
        }
    }
    
    @Override
    public void run() {
        doHealthCheck();
    }
    
    @Override
    public void passIntercept() {
        doHealthCheck();
    }
    
    @Override
    public void afterIntercept() {
    }
}
