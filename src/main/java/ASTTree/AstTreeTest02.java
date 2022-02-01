package ASTTree;

import jdk.nashorn.internal.runtime.RewriteException;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;

public class AstTreeTest02 {
    public static void main(String[] args) {
    }
//    public String rewrite(String sourceQry) throws RewriteException {
//        String result = sourceQry;
//        ASTNode tree = null;
//        try {
//            ParseDriver pd = new ParseDriver();
//            tree = pd.parse(sourceQry, null, true);
//            tree = ParseUtils.findRootNonNullToken(tree);
//            this.rwCtx = new RewriteContext(sourceQry, tree, null);
//            rewrite(tree);
//            result = toSQL();
//        } catch (ParseException e) {
//            LOG.error("Could not parse the query {} ", sourceQry, e);
//            throw new RewriteException("Could not parse query : " , e);
//        }
//        return result;
//    }
}
