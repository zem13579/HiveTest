package ASTTree;

import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;

public class AstTreeTest01 {
    public static void main(String[] args) {
        ParseDriver pd =new ParseDriver();
        String sql="with customer_total_return as\n" +
                "         (select sr_customer_sk as ctr_customer_sk\n" +
                "               , sr_store_sk    as ctr_store_sk\n" +
                "               , sum(SR_FEE)    as ctr_total_return\n" +
                "          from store_returns\n" +
                "             , date_dim\n" +
                "          where sr_returned_date_sk = d_date_sk\n" +
                "            and d_year = 2000\n" +
                "          group by sr_customer_sk\n" +
                "                 , sr_store_sk)\n" +
                "select c_customer_id\n" +
                "from customer_total_return ctr1\n" +
                "   , store\n" +
                "   , customer\n" +
                "where ctr1.ctr_total_return > (select avg(ctr_total_return) * 1.2\n" +
                "                               from customer_total_return ctr2\n" +
                "                               where ctr1.ctr_store_sk = ctr2.ctr_store_sk)\n" +
                "  and s_store_sk = ctr1.ctr_store_sk\n" +
                "  and s_state = 'NM'\n" +
                "  and ctr1.ctr_customer_sk = c_customer_sk\n" +
                "order by c_customer_id\n" +
                "limit 100";
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
