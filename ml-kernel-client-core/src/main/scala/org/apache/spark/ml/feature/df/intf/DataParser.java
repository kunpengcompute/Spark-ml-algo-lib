package org.apache.spark.ml.feature.df.intf;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import java.nio.ByteBuffer;

/**
 * 文本数据解析器
 */
public class DataParser {
    private static final int MAX_CONTENT_LENGTH = 50000;

    /**
     * 解析HDFS上的数据，获取文本内容(content)和语种(languageShortName)
     * @param inBuffer 输入的ByteBuffer
     * @param outBuffer 输出的ByteBuffer
     * @param value HDFS上的原始数据
     * @param langConf 语种设定
     * @return TextDetail类的对象，包含文本内容(content)和语种(languageShortName)
     */
    public static TextDetail decode(ByteBuffer inBuffer, ByteBuffer outBuffer, ImmutableBytesWritable value, LanguagesConf langConf) {
        return null;
    }
}
