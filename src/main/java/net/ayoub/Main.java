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

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.count;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        // Setup the reading stream :
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


        Dataset<Row> inputDF = sparkSession
                .readStream()
                .option("header", "true")
                .option("sep", ",")
                .schema(schema)
                .csv("hdfs://namenode:8020/streaming/capteurs");

        //traitement :
        //-->Calcule en streaming la moyenne des valeurs par capteur, la valeur minimale par capteur, la
        //--> valeur maximale par capteur et le nombre de mesures par capteur.

        Dataset<Row> statsParCapteur = inputDF.groupBy("capteur")
                .agg(avg("valeur").alias("moyenne"),
                        min("valeur").alias("ValeurMin"),
                        max("valeur").alias("ValeurMax"),
                        count("valeur").alias("nombretotal")
                );


        // adding the statis mesures to the foulder checkpoints for fault-tolerance

        StreamingQuery queryStats = statsParCapteur.writeStream()
                .outputMode("complete")
                .format("console")
                .option("truncate", "false")
                .option("numRows",  "50")
                .option("checkpointLocation",
                        "hdfs://namenode:8020/streaming/checkpoints/capteurs/stats")
                .queryName("stats_par_capteur")
                .start();

        // Identifier les mesures anormales dépassant un seuil donné

        Dataset<Row> mesuresAnormales = inputDF.filter(
                col("capteur").startsWith("CAPTEUR_TEMP").and(col("valeur").gt(1000))
                        .or(col("capteur").startsWith("CAPTEUR_HUM") .and(col("valeur").gt(500)))
                        .or(col("capteur").startsWith("CAPTEUR_PRES").and(col("valeur").lt(4)))
                        .or(col("capteur").startsWith("CAPTEUR_LUM") .and(col("valeur").gt(80)))
                        .or(col("capteur").startsWith("CAPTEUR_CO2") .and(col("valeur").gt(300)))
        );

        StreamingQuery queryAnomalies = mesuresAnormales
                .select(
                        col("id"),
                        col("timestamp"),
                        col("capteur"),
                        col("valeur"),
                        col("unite")
                )
                .writeStream()
                .outputMode("append")
                .format("console")
                .option("truncate", "false")
                .option("checkpointLocation",
                        "hdfs://namenode:8020/streaming/checkpoints/capteurs/anomalies")
                .queryName("mesures_anormales")
                .start();


        //executer et attendre la fin des deux queries
        sparkSession.streams().awaitAnyTermination();

    }
}