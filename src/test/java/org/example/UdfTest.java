package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UdfTest {
    private SparkSession sparkSession;

    @BeforeEach
    void before(){
        sparkSession = SparkSession.builder()
                .appName("HiveUDFExample")
                .master("local") // 或者指定你的Spark集群的Master节点
                .enableHiveSupport()
                .getOrCreate();
    }

    @AfterEach
    void after(){
        //关闭sc
        sparkSession.stop();
    }

    @Test
    void test(){

        // 注册 Hive UDF
        sparkSession.sql("CREATE TEMPORARY FUNCTION toUpperCase AS 'org.example.udfs.Upcase'");

        // 使用注册的 Hive UDF 进行查询
        Dataset<Row> results = sparkSession.sql("SELECT toUpperCase('abc') as Up");

        // 显示结果
        results.show();
    }

}
