package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeyWordUtil {
    public static List<String> splitKeyWord(String keyword) throws IOException {
        //创建集合用于存放最终结果数据
        ArrayList<String> resultList = new ArrayList<>();

        //创建IK分词对象  ik_max_word   ik_smart
        StringReader reader = new StringReader(keyword);
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        //获取切分出来的单词
        Lexeme next = ikSegmenter.next();

        while (next != null) {
            String line = next.getLexemeText();
            resultList.add(line);
            next = ikSegmenter.next();
        }

        //返回集合
        return resultList;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(splitKeyWord("尚硅谷大数据项目之Flink实时数仓"));
    }
}
