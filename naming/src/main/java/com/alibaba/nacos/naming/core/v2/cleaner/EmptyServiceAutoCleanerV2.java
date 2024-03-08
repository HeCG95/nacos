/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.naming.core.v2.cleaner;

import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.naming.core.v2.ServiceManager;
import com.alibaba.nacos.naming.core.v2.event.metadata.MetadataEvent;
import com.alibaba.nacos.naming.core.v2.index.ClientServiceIndexesManager;
import com.alibaba.nacos.naming.core.v2.index.ServiceStorage;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Empty service auto cleaner for v2.x.
 * 空服务自动清理器
 * @author xiweng.yy
 */
@Component
public class EmptyServiceAutoCleanerV2 extends AbstractNamingCleaner {
    
    private static final String EMPTY_SERVICE = "emptyService";
    
    private final ClientServiceIndexesManager clientServiceIndexesManager;// Client和Service索引管理
    
    private final ServiceStorage serviceStorage;// Service仓库
    
    public EmptyServiceAutoCleanerV2(ClientServiceIndexesManager clientServiceIndexesManager,
            ServiceStorage serviceStorage) {
        this.clientServiceIndexesManager = clientServiceIndexesManager;
        this.serviceStorage = serviceStorage;// 延迟30秒执行，每60秒清空一次空服务
        GlobalExecutor.scheduleExpiredClientCleaner(this, TimeUnit.SECONDS.toMillis(30),
                GlobalConfig.getEmptyServiceCleanInterval(), TimeUnit.MILLISECONDS);
        
    }
    
    @Override
    public String getType() {
        return EMPTY_SERVICE;
    }
    
    @Override
    public void doClean() {// 获取ServiceManager
        ServiceManager serviceManager = ServiceManager.getInstance();
        // Parallel flow opening threshold 并行处理开启阈值，当服务数量超过100的时候就使用多线程处理
        int parallelSize = 100;
        
        for (String each : serviceManager.getAllNamespaces()) {// 处理多个Namespace下的Service
            Set<Service> services = serviceManager.getSingletons(each);
            Stream<Service> stream = services.size() > parallelSize ? services.parallelStream() : services.stream();// 根据当前Namespace下的Service数量决定是否采用多线程处理
            stream.forEach(this::cleanEmptyService);// 对每个Service执行cleanEmptyService
        }
    }
    
    private void cleanEmptyService(Service service) {
        Collection<String> registeredService = clientServiceIndexesManager.getAllClientsRegisteredService(service);// 获取当前Service下所有的clientId
        if (registeredService.isEmpty() && isTimeExpired(service)) {// 若当前服务下的客户端为空，或者当前服务距离最后一次更新时间超过60秒
            Loggers.SRV_LOG.warn("namespace : {}, [{}] services are automatically cleaned", service.getNamespace(),
                    service.getGroupedServiceName());
            clientServiceIndexesManager.removePublisherIndexesByEmptyService(service);// 移除Service和Client关联信息
            ServiceManager.getInstance().removeSingleton(service);// 移除指定Namespace下的Service服务
            serviceStorage.removeData(service);// 移除Service的详细信息
            NotifyCenter.publishEvent(new MetadataEvent.ServiceMetadataEvent(service, true));// 发布Service过期事件
        }
    }
    
    private boolean isTimeExpired(Service service) {
        long currentTimeMillis = System.currentTimeMillis();
        return currentTimeMillis - service.getLastUpdatedTime() >= GlobalConfig.getEmptyServiceExpiredTime();
    }
}
