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

package com.alibaba.nacos.naming.healthcheck;

import com.alibaba.nacos.naming.interceptor.Interceptable;

/**
 * Nacos health check task.
 * 用于健康检查的Task，定义了健康检查的基本方法
 * @author xiweng.yy
 */
public interface NacosHealthCheckTask extends Interceptable, Runnable {// 可以被拦截器处理，被线程执行器调度
    
    /**
     * Get task id.
     *
     * @return task id.
     */
    String getTaskId();
    
    /**
     * Do health check.
     */
    void doHealthCheck();
}
