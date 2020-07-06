package hive.test;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class hiveLoadOutput_write {
    static final StructType SCHEMA = FIELD_SCHEMA();
    private static final Encoder<Row> ENC = RowEncoder.apply(SCHEMA);

    private static StructType FIELD_SCHEMA() {
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("salary", DataTypes.StringType, true));
        return new StructType(fields.toArray(new StructField[fields.size()]));
    }


    /**
     * https://www.baidu.com/link?url=WYuDOxN99S_Eg40diHu7EMz9Bo1riPLNQ-zhBss_iX7q1g2SJd7j3f3JDYK8UKnf&wd=&eqid=d9ef0cba000864c9000000045efef8c5
     */
    public static void main(String[] args) {

        List<Row> list = new ArrayList<Row>();
        list.add(RowFactory.create("Tom","22","5000"));
        list.add(RowFactory.create("Sally","20","6000"));


        SparkConf sparkConf = new SparkConf()
                .setAppName("readHiveTest")
                .setMaster("local[*]");


        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();
        Dataset<Row> dataset = spark.createDataset(list, ENC);


     //   dataset.createOrReplaceTempView("TmpData");

        dataset.write().mode(SaveMode.Append)
                .format("json")  //
                //.partitionBy("")
                .insertInto("default.esalary");



    }
}
