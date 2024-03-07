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

package com.alibaba.nacos.common.task.engine;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.task.AbstractExecuteTask;
import com.alibaba.nacos.common.task.NacosTaskProcessor;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;

import java.util.Collection;

/**
 * Nacos execute task execute engine.
 * 根据CPU的核数计算线程数，然后获取对应的worker执行任务，在addTask方法中的获取分派worker，最后执行任务worker.process(task);执行任务的时候我们自然而然可以想到里面是一个线程，然后死循环从阻塞队列中获取任务执行任务。
 * @author xiweng.yy
 */
public class NacosExecuteTaskExecuteEngine extends AbstractNacosTaskExecuteEngine<AbstractExecuteTask> {
    
    private final TaskExecuteWorker[] executeWorkers;
    
    public NacosExecuteTaskExecuteEngine(String name, Logger logger) {// 根据计算机的CPU合数计算线程数，大于等于CPU核数*threadMultiple的最小的pow(2,n)的正整数
        this(name, logger, ThreadUtils.getSuitableThreadCount(1));
    }
    
    public NacosExecuteTaskExecuteEngine(String name, Logger logger, int dispatchWorkerCount) {
        super(logger);
        executeWorkers = new TaskExecuteWorker[dispatchWorkerCount];
        for (int mod = 0; mod < dispatchWorkerCount; ++mod) {
            executeWorkers[mod] = new TaskExecuteWorker(name, mod, dispatchWorkerCount, getEngineLog());
        }
    }
    
    @Override
    public int size() {
        int result = 0;
        for (TaskExecuteWorker each : executeWorkers) {
            result += each.pendingTaskCount();
        }
        return result;
    }
    
    @Override
    public boolean isEmpty() {
        return 0 == size();
    }
    
    @Override
    public void addTask(Object tag, AbstractExecuteTask task) {
        NacosTaskProcessor processor = getProcessor(tag);
        if (null != processor) {// 如果有自定义的处理器的话，则使用自定义处理器执行任务
            processor.process(task);
            return;
        }
        TaskExecuteWorker worker = getWorker(tag);// 获取执行任务的worker
        worker.process(task);// 执行任务
    }
    
    private TaskExecuteWorker getWorker(Object tag) {// 根据tag判断哪一个worker来执行任务
        int idx = (tag.hashCode() & Integer.MAX_VALUE) % workersCount();// 保证得到的idx为0~workersCount的数
        return executeWorkers[idx];
    }
    
    private int workersCount() {
        return executeWorkers.length;
    }
    
    @Override
    public AbstractExecuteTask removeTask(Object key) {
        throw new UnsupportedOperationException("ExecuteTaskEngine do not support remove task");
    }
    
    @Override
    public Collection<Object> getAllTaskKeys() {
        throw new UnsupportedOperationException("ExecuteTaskEngine do not support get all task keys");
    }
    
    @Override
    public void shutdown() throws NacosException {
        for (TaskExecuteWorker each : executeWorkers) {
            each.shutdown();
        }
    }
    
    /**
     * Get workers status.
     *
     * @return workers status string
     */
    public String workersStatus() {
        StringBuilder sb = new StringBuilder();
        for (TaskExecuteWorker worker : executeWorkers) {
            sb.append(worker.status()).append('\n');
        }
        return sb.toString();
    }
}
