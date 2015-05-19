package org.wso2.carbon.analytics.test;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.File;
import java.io.IOException;

/**
 * Created by niranda on 5/18/15.
 */
public class TestRdbmsSpark {
    public static void main(String[] args) {
        String sparkHome = "/home/niranda/software/spark-1.3.0-bin-hadoop1";
        System.setProperty("wso2_custom_conf_dir", sparkHome + File.separator + "conf");
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkSQL rdbms test");

        JavaSparkContext ctx = new JavaSparkContext(conf);
        addJars(new File(sparkHome + File.separator + "analytics"), ctx);
        SQLContext sqlCtx = new SQLContext(ctx);

        //registering the analytics table
        System.out.println("Running query");
        sqlCtx.sql(
                "CREATE TEMPORARY TABLE LOG USING org.wso2.carbon.analytics.spark.core.util.AnalyticsRelationProvider " +
                "OPTIONS (" +
                "tenantId \"-1234\", " +
                "tableName \"LOG\", " +
                "schema \"server_name STRING, ip STRING, tenant INTEGER, sequence LONG, summary STRING\"" +
                ")");

        //registering the summary table in rdbms
        //NOTE: this should be available in the rdbms database when it's registering
//        sqlCtx.sql("CREATE TEMPORARY TABLE LOG123 USING org.apache.spark.sql.jdbc.wso2.Wso2JdbcDS " +
//                   "OPTIONS (" +
//                   "url \"jdbc:h2:/home/niranda/Desktop/db/test_db2\", " +
//                   "dbtable \"LOG123\", " +
//                   "driver \"org.h2.Driver\"" +
//                   ")");

        //access data in the analytics tables
        DataFrame dataFrame = sqlCtx.sql("select * from LOG");
        dataFrame.show();

        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void addJars(final File folder, JavaSparkContext sc) {
        for (final File fileEntry : folder.listFiles()) {
            sc.addJar(fileEntry.getPath());
        }
    }
}