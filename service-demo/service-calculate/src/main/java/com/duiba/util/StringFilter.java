package com.duiba.util;

import org.apache.commons.lang3.CharEncoding;
import org.apache.commons.lang3.CharUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class StringFilter extends UDF {

    public String evaluate(String str) {
        StringBuffer out = new StringBuffer();
        if (str == null || ("".equals(str)))
            return "";
        char[] chars = str.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if ((chars[i] >= 19968 && chars[i] <= 40869) //中日朝兼容形式的unicode编码范围： U+4E00——U+9FA5
                    || (chars[i] >= 11904 && chars[i] <= 42191)//中日朝兼容形式扩展
                    || (chars[i] >= 63744 && chars[i] <= 64255)//中日朝兼容形式扩展
                    || (chars[i] >= 65072 && chars[i] <= 65103)//中日朝兼容形式扩展
                    || (chars[i] >= 65280 && chars[i] <= 65519)//全角ASCII、全角中英文标点、半宽片假名、半宽平假名、半宽韩文字母的unicode编码范围：U+FF00——U+FFEF
                    || (chars[i] >= 32 && chars[i] <= 126)//半角字符的unicode编码范围：U+0020-U+007e
                    || (chars[i] >= 12289 && chars[i] <= 12319)//全角字符的unicode编码范围：U+3000——U+301F
            ) {
                out.append(chars[i]);
            }
        }
        String result = out.toString().trim();
        result = result.replaceAll("\\?", "").replaceAll("\\*", "").replaceAll("<|>", "").replaceAll("\\|", "").replaceAll("/", "");
        return result;
    }
}
