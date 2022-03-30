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
