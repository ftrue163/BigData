package com.atguigu.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 我们需计算一个要给定基本数据类型的长度
 */

public class MyUDF extends GenericUDF {
    /**
     * 判断传进来的参数的类型和长度
     * 约定返回的数据类型
     *
     * @param arguments  输入参数类型的鉴别器对象
     * @return  返回值类型的鉴别器对象
     * @throws UDFArgumentException  UDF参数异常
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 判断输入参数的个数
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("please give me only one arg");
        }

        // 判断输入参数的类型
        if (!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            throw new UDFArgumentTypeException(1, "i need primitive type arg");
        }

        // 指定UDF函数的返回值类型的鉴别器对象
        // 比如如果函数返回值需要为int类型，则返回int类型的鉴别器对象
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 函数的逻辑处理
     *
     * @param arguments  输入的参数
     * @return  函数的返回值
     * @throws HiveException  hive异常
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object o = arguments[0].get();
        if (o == null) {
            return 0;
        }

        return o.toString().length();
    }

    // 注释说明UDF函数
    @Override
    public String getDisplayString(String[] children) {
        return "计算给定字符串的长度的UDF函数";
    }
}
