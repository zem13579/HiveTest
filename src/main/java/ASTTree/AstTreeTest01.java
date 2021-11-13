package ASTTree;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;

public class AstTreeTest01 {
    public static void main(String[] args) {
        ParseDriver pd =new ParseDriver();
        String sql="select t2.a,b,c from tab as t1 left join music t2 where age =222";
        ASTNode tree=null;
        try {
            tree=pd.parse(sql);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        System.out.println(tree.getType());
        System.out.println("AstNode:"+tree.dump());
    }
}
