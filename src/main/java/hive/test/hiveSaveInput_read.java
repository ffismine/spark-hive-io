package hive.test;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;


/**
 * 官方文档
 * https://spark.apache.org/docs/2.4.5/sql-data-sources-hive-tables.html
 *
 * spark操作hive的blog
 * https://blog.csdn.net/weixin_43093501/article/details/95023669
 *
 * 报错 Failed to locate the winutils binary
 * https://blog.csdn.net/hdsghtj/article/details/83956267
 *
 * Spark SQL操作外部hive数据库
 * https://blog.csdn.net/xiexianyou666/article/details/106295919
 */



public class hiveSaveInput_read {
    public static void main(String[] args) {
        // $example on:spark_hive$
        // warehouseLocation points to the default location for managed databases and tables
        // System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.6.0");


        SparkConf sparkConf = new SparkConf()
                .setAppName("readHiveTest")
                .setMaster("local[*]");


        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();

        /*  事先在hive建好表，用spark-sql或者命令行
         * Gopa	21	45000
         * Manisha	30	45000
         * Masthanvali	22	10000
         * Krian	27	15000
         * Kranth	42	30000
         */
        spark.sql("show databases").show();
        spark.sql("select * from default.esalary").show();


    }
}
