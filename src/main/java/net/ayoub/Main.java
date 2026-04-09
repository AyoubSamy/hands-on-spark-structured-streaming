package net.ayoub;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("TP6-SparkStructredStreaming")
                .getOrCreate();

        sparkSession.sparkContext().setLogLevel("ERROR");

        // defining the schema for the files to stream in hdfs ( csv files )

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.StringType,false, Metadata.empty()),
                new StructField("timestamp", DataTypes.TimestampType,false,Metadata.empty()),
                new StructField("capteur", DataTypes.StringType,false,Metadata.empty()),
                new StructField("valeur", DataTypes.DoubleType,false,Metadata.empty()),
                new StructField("unite", DataTypes.StringType,false,Metadata.empty()),
        });


        Dataset<Row> inputDf = sparkSession
                .readStream()
                .option("header", "true")
                .option("sep", ",")
                .schema(schema)
                .csv("hdfs://namenode:8020/streaming");


        StreamingQuery query = inputDf.writeStream()
                .outputMode("append")
                .format("console")
                .start();

        query.awaitTermination();
    }
}