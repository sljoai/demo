package com.song.cn.hadoop.hive.udf;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 字符串脱敏
 */
public class StringMask extends UDF {

    private static Logger LOGGER = LoggerFactory.getLogger(StringMask.class);

    private static List<String> sensitiveWordList = new ArrayList<>();

    static {
        try {
            List<String> lineList = FileUtils.readLines(new File("/sensitive_word"), StandardCharsets.UTF_8);
            sensitiveWordList.addAll(sensitiveWordList);
        } catch (IOException e) {
            LOGGER.error("failed to import sensitive word txt!");
        }
    }

    public String evaluate(String ori, String... sensitiveWords) {
        // 输入字符串判空
        if (StringUtils.isEmpty(ori)) {
            return "";
        }
        String result = ori;
        // 字符串脱敏
        for (String sensitiveWord : sensitiveWordList) {
            result = StringUtils.replace(result, sensitiveWord, "*");
        }
        return result;
    }
}
