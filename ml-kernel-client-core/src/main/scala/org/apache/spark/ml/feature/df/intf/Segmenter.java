/*
 * This file to You under the Apache License, Version 2.0;
 * you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.spark.ml.feature.df.intf;

import java.util.List;

/**
 * 统一分词器
 */
public class Segmenter {
    private static Segmenter singleton;

    /**
     * 【单例模式】获取静态的分词器对象
     * @param langConf 语种设定
     * @return 分词器对象
     */
    public static synchronized Segmenter getInstance(LanguagesConf langConf) {
        if (singleton == null) {
            singleton = new Segmenter();
        }
        return singleton;
    }

    /**
     * 调用分词接口，获取词语列表，将用于IDF值统计
     * @param content 文本内容
     * @param shortName 语种简称
     * @return 词语列表
     */
    public List<String> getWords(String[] content, String shortName) {
        return null;
    }
}
