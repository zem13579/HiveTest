package com.zem.test.udaf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.List;

@Description
        (
                name = "collect",
                value = "_FUNC_(X) - Returns a list of objects "
                        + "caution will easily OOM on large data sets"
        )
public class GenericeUDAFCollect extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(GenericeUDAFCollect.class.getName());

    public GenericeUDAFCollect() {
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length != 1) {
            throw new UDFArgumentTypeException(info.length - 1, "only one arg is expected");
        }
        if (info[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "only primitive type arguments are accept but "
                    + info[0].getCategory() + "was passed as parameter 1");
        }
        return new GenericUDAFMkListEvalutor();
    }

    public static class GenericUDAFMkListEvalutor extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector inputIO;
        private StandardListObjectInspector loi;
        private StandardListObjectInspector internalMergeOI;


        /**
         * 初始化
         * @param m
         * @param parameters
         * @return
         * @throws HiveException
         */
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if (m == Mode.PARTIAL1) {
                inputIO = (PrimitiveObjectInspector) parameters[0];
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputIO)
                );
            } else {
                if (!(parameters[0] instanceof StandardListObjectInspector)) {
                    inputIO = (PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
                    return (StandardListObjectInspector) ObjectInspectorFactory.getStandardListObjectInspector(inputIO);
                } else {
                    internalMergeOI = (StandardListObjectInspector) parameters[0];
                    inputIO = (PrimitiveObjectInspector) internalMergeOI.getListElementObjectInspector();
                    loi = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
                    return loi;
                }
            }
        }

        static class MkArrayAggregationBuffer implements AggregationBuffer {
            List<Object> container;
        }

        /**
         * 返回一个用于存储中间对象聚合结果的对象
         * @return
         * @throws HiveException
         */
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MkArrayAggregationBuffer ret = new MkArrayAggregationBuffer();
            reset(ret);
            return ret;
        }

        public void reset(AggregationBuffer agg) throws HiveException {
            ((MkArrayAggregationBuffer) agg).container = new ArrayList<Object>();
        }

        /**
         * 将一行新数据载入聚合缓存汇中
         * @param agg
         * @param parameters
         * @throws HiveException
         */
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            Object p = parameters[0];
            if (p != null) {
                MkArrayAggregationBuffer myagg  = (MkArrayAggregationBuffer) agg;
                putIntoList(p,myagg);
            }
        }

        private void putIntoList(Object p,MkArrayAggregationBuffer myagg){
            Object Pcopy = ObjectInspectorUtils.copyToStandardObject(p, this.inputIO);
            myagg.container.add(Pcopy);
        }

        /**
         * 以一种可持久化的方式返回内容
         * @param agg
         * @return
         * @throws HiveException
         */
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
            ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());
            ret.addAll(myagg.container);
            return ret;
        }


        /**
         * 将terminatePartial 返回中间部分聚合结果到当前聚合中
         * @param agg
         * @param partial
         * @throws HiveException
         */
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
            ArrayList<Object> partialResult  = (ArrayList<Object>)internalMergeOI.getList(partial);
            for(Object i : partialResult){
                putIntoList(i,myagg);
            }
        }

        /**
         * 返回最终聚合结果
         * @param agg
         * @return
         * @throws HiveException
         */
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
            ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());
            ret.addAll(myagg.container);
            return ret;
        }
    }

}
