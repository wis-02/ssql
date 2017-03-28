
package org.hua;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Example {

    public static void main(String[] args) throws Exception {
       //System.setProperty("hadoop.home.dir", "C:\\hadoop\\hadoop-2.6.4\\");
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
        
        // print some data
        movies.show();
        links.show();
        ratings.show();
        tags.show();

        // get all comedies
        Dataset<Row> allComedies = movies.filter(movies.col("genres").like("%Comedy%"));
        allComedies.show();
       // allComedies.write().format("json").save(outputPath+"/all-comedies");

        // TODO: count all comedies that a user rates at least 3.0
        //       (join ratings with movies, filter by rating, groupby userid and
        //        aggregate count)
        Dataset<Row> goodComediesPerUser = movies.join(ratings, movies.col("movieId").equalTo(ratings.col("movieId"))).filter("rating > 3.0").groupBy("userId").sum().coalesce(5);
                /*???*//*???*/
        
        goodComediesPerUser.show();
        //goodComediesPerUser.write();
        goodComediesPerUser.write().format("json").save(outputPath+"/good-comedies-per-user");

        spark.close();

    }
}
