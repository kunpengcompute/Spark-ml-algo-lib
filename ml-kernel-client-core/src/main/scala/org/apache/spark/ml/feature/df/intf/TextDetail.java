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

/**
 * 文本信息，包括文本内容和所属语种
 */
public class TextDetail {
    private String[] content;
    private String shortName;

    TextDetail(String[] content, String shortName) {
        this.content = content;
        this.shortName = shortName;
    }

    /**
     * 获取文本内容
     * @return
     */
    public String[] getContent() {
        return content;
    }

    /**
     * 获取文本的语种
     * @return
     */
    public String getShortName() {
        return shortName;
    }
}
