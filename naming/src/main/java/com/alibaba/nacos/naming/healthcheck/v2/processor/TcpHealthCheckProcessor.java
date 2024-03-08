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

package com.alibaba.nacos.naming.healthcheck.v2.processor;

import com.alibaba.nacos.api.naming.pojo.healthcheck.HealthCheckType;
import com.alibaba.nacos.naming.core.v2.metadata.ClusterMetadata;
import com.alibaba.nacos.naming.core.v2.pojo.HealthCheckInstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.healthcheck.v2.HealthCheckTaskV2;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import com.alibaba.nacos.sys.env.EnvUtil;
import org.springframework.stereotype.Component;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.naming.misc.Loggers.SRV_LOG;

/**
 * TCP health check processor for v2.x.
 * TCP请求方式的健康检查处理器，内部使用NIO实现网络通信
 * <p>Current health check logic is same as v1.x. TODO refactor health check for v2.x.
 *
 * @author xiweng.yy
 */
@Component
public class TcpHealthCheckProcessor implements HealthCheckProcessorV2, Runnable {
    
    public static final String TYPE = HealthCheckType.TCP.name();// 当前的Processor可处理的Task类型为TCP
    
    public static final int CONNECT_TIMEOUT_MS = 500;// 连接超时时长
    
    /**
     * this value has been carefully tuned, do not modify unless you're confident.
     */
    private static final int NIO_THREAD_COUNT = EnvUtil.getAvailableProcessors(0.5);// NIO线程数量
    
    /**
     * because some hosts doesn't support keep-alive connections, disabled temporarily.
     */
    private static final long TCP_KEEP_ALIVE_MILLIS = 0;
    
    private final HealthCheckCommonV2 healthCheckCommon;// v2版本健康检查通用方法集合
    
    private final SwitchDomain switchDomain;
    
    private final Map<String, BeatKey> keyMap = new ConcurrentHashMap<>();
    
    private final BlockingQueue<Beat> taskQueue = new LinkedBlockingQueue<>();// Tcp心跳任务阻塞队列，用于实现生产者消费者模式
    
    private final Selector selector;// NIO多路复用器，用于管理多个线程的网络连接，此处就是检查多个心跳的连接
    
    public TcpHealthCheckProcessor(HealthCheckCommonV2 healthCheckCommon, SwitchDomain switchDomain) {
        this.healthCheckCommon = healthCheckCommon;
        this.switchDomain = switchDomain;
        try {
            selector = Selector.open();// 创建Selector
            GlobalExecutor.submitTcpCheck(this);// 使用线程执行器执行当前类，也就是将当前类作为消费者启动，run方法内部的循环将会持续进行，不断消费数据
        } catch (Exception e) {
            throw new IllegalStateException("Error while initializing SuperSense(TM).");
        }
    }
    // 作为Processor的时候，它提供process方法来对task进行处理，处理的结果就是将其放入消费队列
    @Override// 作为一个线程运行的时候，它作为消费者，不断从队列中获取任务来执行
    public void process(HealthCheckTaskV2 task, Service service, ClusterMetadata metadata) {
        HealthCheckInstancePublishInfo instance = (HealthCheckInstancePublishInfo) task.getClient()
                .getInstancePublishInfo(service);// 获取Instance的检查信息
        if (null == instance) {
            return;
        }
        // TODO handle marked(white list) logic like v1.x.
        if (!instance.tryStartCheck()) {
            SRV_LOG.warn("[HEALTH-CHECK-V2] tcp check started before last one finished, service: {} : {} : {}:{}",
                    service.getGroupedServiceName(), instance.getCluster(), instance.getIp(), instance.getPort());
            healthCheckCommon
                    .reEvaluateCheckRT(task.getCheckRtNormalized() * 2, task, switchDomain.getTcpHealthParams());
            return;
        }// 处理任务时，将其放入队列内部，此处相当于生产者，每调用一次process都会将其放入队列
        taskQueue.add(new Beat(task, service, metadata, instance));
        MetricsMonitor.getTcpHealthCheckMonitor().incrementAndGet();
    }
    
    @Override
    public String getType() {
        return TYPE;
    }
    // 处理TCP健康检查任务
    private void processTask() throws Exception {
        Collection<Callable<Void>> tasks = new LinkedList<>();// 任务处理器集合，为每一个Beat创建一个TaskProcessor
        do {// 疯狂从队列获取心跳信息
            Beat beat = taskQueue.poll(CONNECT_TIMEOUT_MS / 2, TimeUnit.MILLISECONDS);// 从队列获取元素，超时时间为250毫秒
            if (beat == null) {// 若数据为空，继续执行下次循环
                return;
            }
            // 添加任务到集合中，后续一次性处理
            tasks.add(new TaskProcessor(beat));
        } while (taskQueue.size() > 0 && tasks.size() < NIO_THREAD_COUNT * 64);// 循环条件：1. 队列内有数据 2. 已获取的task小于CPU核数的0.5倍 * 64（例如8核CPU的话就是 8 * 0.5 * 64 = 256）
        // 一次性调用所有task
        for (Future<?> f : GlobalExecutor.invokeAllTcpSuperSenseTask(tasks)) {
            f.get();
        }
    }
    
    @Override
    public void run() {
        while (true) {
            try {
                processTask();
                // 使用非阻塞方法获取已准备好进行I/O的channel数量集
                int readyCount = selector.selectNow();
                if (readyCount <= 0) {
                    continue;
                }
                // 处理 SelectionKey
                Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    SelectionKey key = iter.next();
                    iter.remove();
                    
                    GlobalExecutor.executeTcpSuperSense(new PostProcessor(key));
                }
            } catch (Throwable e) {
                SRV_LOG.error("[HEALTH-CHECK-V2] error while processing NIO task", e);
            }
        }
    }
    
    public class PostProcessor implements Runnable {
        
        SelectionKey key;
        
        public PostProcessor(SelectionKey key) {
            this.key = key;
        }
        
        @Override
        public void run() {
            Beat beat = (Beat) key.attachment();
            SocketChannel channel = (SocketChannel) key.channel();
            try {
                if (!beat.isHealthy()) {
                    //invalid beat means this server is no longer responsible for the current service
                    key.cancel();
                    key.channel().close();
                    
                    beat.finishCheck();
                    return;
                }
                
                if (key.isValid() && key.isConnectable()) {
                    //connected
                    channel.finishConnect();
                    beat.finishCheck(true, false, System.currentTimeMillis() - beat.getTask().getStartTime(),
                            "tcp:ok+");
                }
                
                if (key.isValid() && key.isReadable()) {
                    //disconnected
                    ByteBuffer buffer = ByteBuffer.allocate(128);
                    if (channel.read(buffer) == -1) {
                        key.cancel();
                        key.channel().close();
                    } else {
                        // not terminate request, ignore
                        SRV_LOG.warn(
                                "Tcp check ok, but the connected server responses some msg. Connection won't be closed.");
                    }
                }
            } catch (ConnectException e) {
                // unable to connect, possibly port not opened
                beat.finishCheck(false, true, switchDomain.getTcpHealthParams().getMax(),
                        "tcp:unable2connect:" + e.getMessage());
            } catch (Exception e) {
                beat.finishCheck(false, false, switchDomain.getTcpHealthParams().getMax(),
                        "tcp:error:" + e.getMessage());
                
                try {
                    key.cancel();
                    key.channel().close();
                } catch (Exception ignore) {
                }
            }
        }
    }
    
    private class Beat {
        
        private final HealthCheckTaskV2 task;
        
        private final Service service;
        
        private final ClusterMetadata metadata;
        
        private final HealthCheckInstancePublishInfo instance;
        
        long startTime = System.currentTimeMillis();
        
        public Beat(HealthCheckTaskV2 task, Service service, ClusterMetadata metadata,
                HealthCheckInstancePublishInfo instance) {
            this.task = task;
            this.service = service;
            this.metadata = metadata;
            this.instance = instance;
        }
        
        public void setStartTime(long time) {
            startTime = time;
        }
        
        public long getStartTime() {
            return startTime;
        }
        
        public HealthCheckTaskV2 getTask() {
            return task;
        }
        
        public Service getService() {
            return service;
        }
        
        public ClusterMetadata getMetadata() {
            return metadata;
        }
        
        public HealthCheckInstancePublishInfo getInstance() {
            return instance;
        }
        
        public boolean isHealthy() {
            return System.currentTimeMillis() - startTime < TimeUnit.SECONDS.toMillis(30L);
        }
        
        /**
         * finish check only, no ip state will be changed.
         */
        public void finishCheck() {
            instance.finishCheck();
        }
        
        public void finishCheck(boolean success, boolean now, long rt, String msg) {
            if (success) {
                healthCheckCommon.checkOk(task, service, msg);
            } else {
                if (now) {
                    healthCheckCommon.checkFailNow(task, service, msg);
                } else {
                    healthCheckCommon.checkFail(task, service, msg);
                }
                
                keyMap.remove(toString());
            }
            
            healthCheckCommon.reEvaluateCheckRT(rt, task, switchDomain.getTcpHealthParams());
        }
        
        @Override
        public String toString() {
            return service.getGroupedServiceName() + ":" + instance.getCluster() + ":" + instance.getIp() + ":"
                    + instance.getPort();
        }
        
        @Override
        public int hashCode() {
            return toString().hashCode();
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Beat)) {
                return false;
            }
            
            return this.toString().equals(obj.toString());
        }
    }
    
    private static class BeatKey {
        
        public SelectionKey key;
        
        public long birthTime;
        
        public BeatKey(SelectionKey key) {
            this.key = key;
            this.birthTime = System.currentTimeMillis();
        }
    }
    
    private static class TimeOutTask implements Runnable {
        
        SelectionKey key;
        
        public TimeOutTask(SelectionKey key) {
            this.key = key;
        }
        
        @Override
        public void run() {
            if (key != null && key.isValid()) {
                SocketChannel channel = (SocketChannel) key.channel();
                Beat beat = (Beat) key.attachment();
                
                if (channel.isConnected()) {
                    return;
                }
                
                try {
                    channel.finishConnect();
                } catch (Exception ignore) {
                }
                
                try {
                    beat.finishCheck(false, false, beat.getTask().getCheckRtNormalized() * 2, "tcp:timeout");
                    key.cancel();
                    key.channel().close();
                } catch (Exception ignore) {
                }
            }
        }
    }
    
    private class TaskProcessor implements Callable<Void> {
        
        private static final int MAX_WAIT_TIME_MILLISECONDS = 500;
        
        Beat beat;
        
        public TaskProcessor(Beat beat) {
            this.beat = beat;
        }
        
        @Override
        public Void call() {
            long waited = System.currentTimeMillis() - beat.getStartTime();
            if (waited > MAX_WAIT_TIME_MILLISECONDS) {
                Loggers.SRV_LOG.warn("beat task waited too long: " + waited + "ms");
            }
            
            SocketChannel channel = null;
            try {
                HealthCheckInstancePublishInfo instance = beat.getInstance();
                
                BeatKey beatKey = keyMap.get(beat.toString());
                if (beatKey != null && beatKey.key.isValid()) {
                    if (System.currentTimeMillis() - beatKey.birthTime < TCP_KEEP_ALIVE_MILLIS) {
                        instance.finishCheck();
                        return null;
                    }
                    
                    beatKey.key.cancel();
                    beatKey.key.channel().close();
                }
                
                channel = SocketChannel.open();
                channel.configureBlocking(false);
                // only by setting this can we make the socket close event asynchronous
                channel.socket().setSoLinger(false, -1);
                channel.socket().setReuseAddress(true);
                channel.socket().setKeepAlive(true);
                channel.socket().setTcpNoDelay(true);
                
                ClusterMetadata cluster = beat.getMetadata();
                int port = cluster.isUseInstancePortForCheck() ? instance.getPort() : cluster.getHealthyCheckPort();
                channel.connect(new InetSocketAddress(instance.getIp(), port));
                
                SelectionKey key = channel.register(selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
                key.attach(beat);
                keyMap.put(beat.toString(), new BeatKey(key));
                
                beat.setStartTime(System.currentTimeMillis());
                
                GlobalExecutor
                        .scheduleTcpSuperSenseTask(new TimeOutTask(key), CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                beat.finishCheck(false, false, switchDomain.getTcpHealthParams().getMax(),
                        "tcp:error:" + e.getMessage());
                
                if (channel != null) {
                    try {
                        channel.close();
                    } catch (Exception ignore) {
                    }
                }
            }
            
            return null;
        }
    }
}
