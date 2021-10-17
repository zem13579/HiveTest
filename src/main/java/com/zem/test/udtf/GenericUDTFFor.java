package com.zem.test.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableConstantIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;


public class GenericUDTFFor extends GenericUDTF {

    WritableConstantIntObjectInspector start;
    WritableConstantIntObjectInspector end;
    WritableConstantIntObjectInspector inc;

    Object[] forwardObj = null;

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<? extends StructField> fieldRefs = argOIs.getAllStructFieldRefs();
        //该函数参数个数只允许2，或者3
        if(!(fieldRefs.size() == 2 | fieldRefs.size() ==3)){
            throw new UDFArgumentTypeException(fieldRefs.size()-1,"the For function need 2 or 3 args ！");
        }
        start = (WritableConstantIntObjectInspector) fieldRefs.get(0).getFieldObjectInspector();
        end = (WritableConstantIntObjectInspector) fieldRefs.get(1).getFieldObjectInspector();
        if(fieldRefs.size() == 3){
            inc = (WritableConstantIntObjectInspector) fieldRefs.get(2).getFieldObjectInspector();
        }else {
            inc = (WritableConstantIntObjectInspector)PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo,new IntWritable(1));
        }
        this.forwardObj = new Object[1];
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIS = new ArrayList<ObjectInspector>();

        fieldNames.add("col0");
        fieldOIS.add(
                PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT)
        );
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIS);

    }

    public void process(Object[] args) throws HiveException {
        for (int i = start.getWritableConstantValue().get(); i < end.getWritableConstantValue().get(); i = i+inc.getWritableConstantValue().get()) {
            this.forwardObj[0] = i;
            forward(forwardObj);
        }
    }

    public void close() throws HiveException {

    }
}
