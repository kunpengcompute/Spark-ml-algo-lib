/*
 * Copyright (C) 2021-2022. Huawei Technologies Co., Ltd.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * */
/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.ml.feature.df.intf;

import java.io.Serializable;

/**
 * 语种设定信息。对象构造时，通过读取语种配置，获取需要执行分词的语种列表
 */
public class LanguagesConf implements Serializable {

    /**
     * 读取语种配置，获取需要执行分词的语种列表，构造对象。
     * @param langFile 配置文件名称
     */
    public LanguagesConf(String langFile) {
    }

    /**
     * 获取已配置的语种列表
     * @return 语种简称列表
     */
    public String[] getShortNameLanguages() {
        return null;
    }
}
