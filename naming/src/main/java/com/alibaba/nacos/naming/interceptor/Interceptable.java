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

package com.alibaba.nacos.naming.interceptor;

/**
 * Interceptable Interface.
 * 用于拦截将要执行的任务，对任务进行一些验证处理
 * @author xiweng.yy
 */
public interface Interceptable {
    
    /** 若没有拦截器拦截此对象，此方法会被调用
     * If no {@link NacosNamingInterceptor} intercept this object, this method will be called to execute.
     */
    void passIntercept();
    
    /** 若此对象被拦截器拦截，此方法会被调用
     * If one {@link NacosNamingInterceptor} intercept this object, this method will be called.
     */
    void afterIntercept();
}
