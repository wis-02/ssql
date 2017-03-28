package org.hua;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Recommendation {

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-2.6.4\\");
        boolean isLocal = false;
        if (args.length == 0) {
            isLocal = true;
        } else if (args.length < 2) { 
            System.out.println("Usage: Example input-path output-path");
            System.exit(0);
        }

        SparkSession spark;
        String inputPath, outputPath;        
        if (isLocal) {
            spark = SparkSession.builder().master("local").appName("Java Spark SQL example")
                    .getOrCreate();
            inputPath = "src/main/resources";
            outputPath= "output";
        } else {
            spark = SparkSession.builder().appName("Java Spark SQL example")
                    .getOrCreate();
            inputPath = args[0];
            outputPath= args[1];
        }

        // load
        Dataset<Row> movies = spark.read().option("header", "true").csv(inputPath+"/movies.csv");
        Dataset<Row> links = spark.read().option("header", "true").csv(inputPath+"/links.csv");
        Dataset<Row> ratings = spark.read().option("header", "true").csv(inputPath+"/ratings.csv");
        Dataset<Row> tags = spark.read().option("header", "true").csv(inputPath+"/tags.csv");

        // schema
        //
        // ratings.csv   userId,movieId,rating,timestamp
        // movies.csv    movieId,title,genres
        // links.csv     movieId,imdbId,tmdbId
        // tags.csv      userId,movieId,tag,timestamp
        // print schema
        movies.printSchema();
        links.printSchema();
        ratings.printSchema();
        tags.printSchema();
        
        // register as temporary views and run sql
        movies.createOrReplaceTempView("movies");
        links.createOrReplaceTempView("links");
        ratings.createOrReplaceTempView("ratings");
        tags.createOrReplaceTempView("tags");

        // number of users, movies and ratings
        spark.sql(
                "SELECT COUNT(distinct userId) as nusers, " + 
                "       COUNT(distinct movieId) as nmovies, " + 
                "       COUNT(*) as nbratings FROM ratings")
                .show();

        // global recommendation, movies with the best average rating
        Dataset<Row> res = spark.sql(
                "select movies.movieId, avgrating, nbratings from movies, " +
                "      (select round(avg(rating),1) as avgrating, " + 
                "              count(userId) as nbratings, movieId " + 
                "       from ratings " + 
                "       group by movieId " + 
                "       order by avgrating desc limit 10) as avgratingbymovie " +
                "where movies.movieId = avgratingbymovie.movieId " + 
                "order by avgrating desc"
        );

        // show execution plan
        res.explain();
        
        // show some of the results
        res.show();
        
        res.write().format("json").save(outputPath+"/recommendation");

        spark.close();

    }
}
