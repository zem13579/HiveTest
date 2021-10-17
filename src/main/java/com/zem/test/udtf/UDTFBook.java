package com.zem.test.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.ArrayList;


public class UDTFBook extends GenericUDTF {
    private String sent;
    Object[] forwardObj = null;

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        //参数个数检测

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fiedlsOIs = new ArrayList<ObjectInspector>();

        fieldNames.add("isbn");
        fiedlsOIs.add(PrimitiveObjectInspectorFactory
                .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING));

        fieldNames.add("title");
        fiedlsOIs.add(PrimitiveObjectInspectorFactory
                .getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING));

        fieldNames.add("authors");
        fiedlsOIs.add(ObjectInspectorFactory
                .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                        PrimitiveObjectInspector.PrimitiveCategory.STRING
                )));
        forwardObj = new Object[3];
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fiedlsOIs);
    }

    public void process(Object[] args) throws HiveException {
        sent = new String(((LazyString) args[0]).getWritableObject().getBytes());
        String[] parts =  sent.split("\\|");
        forwardObj[0] = parts[0];
        forwardObj[1] = parts[1];
        forwardObj[2] = parts[2].split(",");
        this.forward(forwardObj);
    }

    public void close() throws HiveException {

    }
}
