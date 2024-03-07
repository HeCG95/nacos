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
 * Nacos naming interceptor.
 * 限定了它的实现必须是处理 Interceptable 类型。它最主要的功能就是定义了拦截机制
 * @author xiweng.yy
 */
public interface NacosNamingInterceptor<T extends Interceptable> {
    
    /**
     * Judge whether the input type is intercepted by this Interceptor.
     * 判断输入的参数是否是当前拦截器可处理的类型
     * <p>This method only should judge the object type whether need be do intercept. Not the intercept logic.
     *
     * @param type type
     * @return true if the input type is intercepted by this Interceptor, otherwise false
     */
    boolean isInterceptType(Class<?> type);
    
    /**
     * Do intercept operation.
     * 拦截后的操作
     * <p>This method is the actual intercept operation.
     *
     * @param object need intercepted object
     * @return true if object is intercepted, otherwise false
     */
    boolean intercept(T object);
    
    /**
     * The order of interceptor. The lower the number, the earlier the execution.
     * 拦截器的优先级，数字越低，优先级越高
     * @return the order number of interceptor
     */
    int order();
}
