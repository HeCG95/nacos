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

package com.alibaba.nacos.common.notify;

import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.alibaba.nacos.common.notify.NotifyCenter.ringBufferSize;

/**
 * The default event publisher implementation.
 * 单事件发布者：一个发布者实例只能处理一种类型的事件
 * <p>Internally, use {@link ArrayBlockingQueue <Event/>} as a message staging queue.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 * @author zongtanghu
 */
public class DefaultPublisher extends Thread implements EventPublisher {
    
    protected static final Logger LOGGER = LoggerFactory.getLogger(NotifyCenter.class);
    
    private volatile boolean initialized = false;// 发布者是否初始化完毕
    
    private volatile boolean shutdown = false;// 是否关闭了发布者
    
    private Class<? extends Event> eventType;// 事件的类型
    
    protected final ConcurrentHashSet<Subscriber> subscribers = new ConcurrentHashSet<>();// 订阅者列表
    
    private int queueMaxSize = -1;// 队列最大容量
    
    private BlockingQueue<Event> queue;// 队列类型
    
    protected volatile Long lastEventSequence = -1L;// 最后一个事件的序列号
    
    private static final AtomicReferenceFieldUpdater<DefaultPublisher, Long> UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(DefaultPublisher.class, Long.class, "lastEventSequence");// 事件序列号更新对象，用于更新原子属性lastEventSequence
    
    @Override
    public void init(Class<? extends Event> type, int bufferSize) {
        setDaemon(true);// 设置为了守护线程，意味着它将持续运行（它需要持续监控内部的事件队列）
        setName("nacos.publisher-" + type.getName());// 设置当前线程的名称以事件类型为区分
        this.eventType = type;// 传入的type属性为当前发布者需要处理的事件类型
        this.queueMaxSize = bufferSize;
        this.queue = new ArrayBlockingQueue<>(bufferSize);// 有界阻塞队列，元素先进先出。并且使用非公平模式提升性能，意味着等待消费的订阅者执行顺序将得不到保障（业务需求没有这种顺序性要求）
        start();// 它将会以多个线程的形式存在，每个线程代表一种事件类型的发布者,后面初始化了队列的长度。最后调用启动方法完成当前线程的启动
    }
    
    public ConcurrentHashSet<Subscriber> getSubscribers() {
        return subscribers;
    }
    
    @Override
    public synchronized void start() {
        if (!initialized) {
            // start just called once
            super.start();
            if (queueMaxSize == -1) {
                queueMaxSize = ringBufferSize;
            }
            initialized = true;// 设置初始化状态为true
        }
    }
    
    @Override
    public long currentEventSize() {
        return queue.size();
    }
    
    @Override
    public void run() {
        openEventHandler();
    }
    
    void openEventHandler() {
        try {
            
            // This variable is defined to resolve the problem which message overstock in the queue.
            int waitTimes = 60;// 在首次启动的时候会等待1分钟，然后再进行消息消费
            // To ensure that messages are not lost, enable EventHandler when
            // waiting for the first Subscriber to register
            while (!shutdown && !hasSubscriber() && waitTimes > 0) {// 线程终止条件判断
                ThreadUtils.sleep(1000L);// 线程休眠1秒
                waitTimes--;// 等待次数减1
            }

            while (!shutdown) {// 线程终止条件判断
                final Event event = queue.take();// 从队列取出事件
                receiveEvent(event);// 接收事件
                UPDATER.compareAndSet(this, lastEventSequence, Math.max(lastEventSequence, event.sequence()));// 更新事件序列号
            }
        } catch (Throwable ex) {
            LOGGER.error("Event listener exception : ", ex);
        }
    }
    
    private boolean hasSubscriber() {
        return CollectionUtils.isNotEmpty(subscribers);
    }
    
    @Override
    public void addSubscriber(Subscriber subscriber) {
        subscribers.add(subscriber);
    }
    
    @Override
    public void removeSubscriber(Subscriber subscriber) {
        subscribers.remove(subscriber);
    }
    
    @Override
    public boolean publish(Event event) {// 外部调用发布事件：开放给外部调用者，接收统一通知中心的事件并放入队列中的
        checkIsStart();
        boolean success = this.queue.offer(event);
        if (!success) {
            LOGGER.warn("Unable to plug in due to interruption, synchronize sending time, event : {}", event);
            receiveEvent(event);// 直接同步发送事件给订阅者，不经过队列
            return true;
        }
        return true;// 放入队列成功的时候直接返回
    }
    
    void checkIsStart() {
        if (!initialized) {
            throw new IllegalStateException("Publisher does not start");
        }
    }
    
    @Override
    public void shutdown() {
        this.shutdown = true;
        this.queue.clear();
    }
    
    public boolean isInitialized() {
        return initialized;
    }
    
    /**
     * Receive and notifySubscriber to process the event.
     * 接收通知中心发过来的事件，发布给订阅者
     * @param event {@link Event}.
     */
    void receiveEvent(Event event) {
        final long currentEventSequence = event.sequence();// 获取当前事件的序列号，它是自增的
        
        if (!hasSubscriber()) {
            LOGGER.warn("[NotifyCenter] the {} is lost, because there is no subscriber.", event);
            return;
        }
        
        // Notification single event listener
        for (Subscriber subscriber : subscribers) {// 通知所有订阅了该事件的订阅者 - 在死循环中不断从ArrayBlockingQueue中获取数据来循环通知每一个订阅者，也就是调用订阅者的onEvent()方法
            if (!subscriber.scopeMatches(event)) {
                continue;
            }
            // 判断订阅者是否忽略事件过期，判断当前事件是否被处理过（lastEventSequence初始化的值为-1，而Event的sequence初始化的值为0）
            // Whether to ignore expiration events
            if (subscriber.ignoreExpireEvent() && lastEventSequence > currentEventSequence) {
                LOGGER.debug("[NotifyCenter] the {} is unacceptable to this subscriber, because had expire",
                        event.getClass());
                continue;
            }
            
            // Because unifying smartSubscriber and subscriber, so here need to think of compatibility.
            // Remove original judge part of codes.
            notifySubscriber(subscriber, event);
        }
    }
    
    @Override
    public void notifySubscriber(final Subscriber subscriber, final Event event) {
        
        LOGGER.debug("[NotifyCenter] the {} will received by {}", event, subscriber);
        
        final Runnable job = () -> subscriber.onEvent(event);// 为每个订阅者创建一个Runnable对象
        final Executor executor = subscriber.executor();// 使用订阅者的线程执行器
        
        if (executor != null) {
            executor.execute(job);
        } else {// 若订阅者没有自己的执行器，则直接执行run方法启动订阅者消费线程
            try {
                job.run();
            } catch (Throwable e) {
                LOGGER.error("Event callback exception: ", e);
            }
        }
    }
}
