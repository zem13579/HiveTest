package com.zem.test;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class GenericUDFNvl extends GenericUDF {
    private ObjectInspector[] argumentOIs;
    private GenericUDFUtils.ReturnObjectInspectorResolver resolver;
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        argumentOIs = objectInspectors;
        if(argumentOIs.length != 2){
            throw new UDFArgumentLengthException("The operator 'Nvl' accept 2 arguments");
        }

        resolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
        if((!resolver.update(argumentOIs[0]) && resolver.update(argumentOIs[1]))){
            throw new UDFArgumentTypeException(2, "both arg should be the same type");
        }

        return resolver.get();
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object retval = resolver.convertIfNecessary(deferredObjects[0].get(), argumentOIs[0]);
        if(retval == null){
            System.out.println("retval is null");
            resolver.convertIfNecessary(deferredObjects[1].get(),argumentOIs[1]);
        }
        return retval;
    }

    public String getDisplayString(String[] strings) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder
                .append("if")
                .append(strings[0])
                .append("is null")
                .append("returns")
                .append(strings[1]);
        return stringBuilder.toString();
    }
}
