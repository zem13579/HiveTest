package com.zem.test.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFMethodResolver;

public class UDFInCache extends UDF {

    @Override
    public UDFMethodResolver getResolver() {
        return super.getResolver();
    }
}
