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
