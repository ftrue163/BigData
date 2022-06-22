package com.atguigu.app.fun;

import com.atguigu.utils.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {
    public void eval(String str) {
        try {
            List<String> splitKeyWord = KeyWordUtil.splitKeyWord(str);

            for (String word : splitKeyWord) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            e.printStackTrace();
            collect(Row.of(str));
        }
    }
}
