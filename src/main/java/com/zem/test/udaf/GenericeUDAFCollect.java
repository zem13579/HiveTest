package com.zem.test.udaf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@Description
        (
                name = "collect",
                value= "_FUNC_(X) - Returns a list of objects "
                +"caution will easily OOM on large data sets"
        )
public class GenericeUDAFCollect extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(GenericeUDAFCollect.class.getName());

    public GenericeUDAFCollect(){}

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if(info.length != 1){
            throw new UDFArgumentTypeException(info.length-1,"only one arg is expected");
        }
        if(info[0].getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentTypeException(0,"only primitive type arguments are accept but "
                    +info[0].getCategory()+"was passed as parameter 1");
        }
        return new GenericUDAFMkListEvalutor();
    }

    public static class GenericUDAFMkListEvalutor extends GenericUDAFEvaluator{

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            return super.init(m, parameters);
        }

        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return null;
        }

        public void reset(AggregationBuffer agg) throws HiveException {

        }

        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {

        }

        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return null;
        }

        public void merge(AggregationBuffer agg, Object partial) throws HiveException {

        }

        public Object terminate(AggregationBuffer agg) throws HiveException {
            return null;
        }
    }
}
