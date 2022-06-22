package com.atguigu.hive.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

public class ExplodeJSONArray extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        //1 约束函数传入的参数个数
        if (argOIs.getAllStructFieldRefs().size() != 1) {
            throw new UDFArgumentLengthException("explode_json_array函数的参数个数只能为1...");
        }

        //2 约束函数传入的参数类型
        String typeName = argOIs.getAllStructFieldRefs().get(0).getFieldObjectInspector().getTypeName();
        if (!"string".equals(typeName)) {
            throw new UDFArgumentTypeException(0, "explode_json_array函数的第一个参数的类型必须为String...");
        }

        //3 约束函数返回值的类型
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldNames.add("aaa");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);

    }

    @Override
    public void process(Object[] args) throws HiveException {
        //1 获取到传入的json数据字符串
        String jsonArrStr = args[0].toString();

        //2 将jonsArrStr变成真正的jonsArr
        JSONArray jsonArray = new JSONArray(jsonArrStr);

        //3 遍历jsonArray,取出里面一个个的json
        for (int i = 0; i < jsonArray.length(); i++) {
            String jsonStr = jsonArray.getString(0);

            //写出jsonStr
            //因为初始化方法里面限定了返回值类型是struct结构体
            //所以在这个地方不能直接输出jsonStr,需要用个字符串数组包装下
            String[] result = new String[1];
            result[0] = jsonStr;

            forward(result);
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
