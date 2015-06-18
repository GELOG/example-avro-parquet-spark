/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//https://github.com/AndreSchumacher/avro-parquet-spark-example
package avrotest


//import com.google.common.io.Files
import org.apache.avro.Schema
//import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.IndexedRecord
//import org.apache.avro.specific.SpecificDatumWriter
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
//import org.apache.spark.sql.catalyst.util.getTempFilePath
import parquet.avro.AvroParquetWriter



// our own classes generated from user.avdl by Avro tools

import java.io.File

import org.bdgenomics.formats.avro.{Variant, Genotype}

object ParquetAvroSparkExample {

  private var sqc: SQLContext = _

  def main(args: Array[String]) {
    /*
    // Create a Spark Configuration for local cluster mode
    val conf = new SparkConf(true)
      .setMaster("local")
      .setAppName("ParquetAvroExample")

    // Create a Spark Context and wrap it inside a SQLContext
    sqc = new SQLContext(new SparkContext(conf))

    // Prepare some input data
    val avroFiles = (getTempFilePath("users", ".avro"), getTempFilePath("messages", ".avro"))
    val parquetFiles = (
      new Path(Files.createTempDir().toString, "users.parquet"),
      new Path(Files.createTempDir().toString, "messages.parquet"))

    // Generate some input (100 users, 1000 messages) and write then as Avro files to the local
    // file system
    writeAvroFile(avroFiles._1, createUser, 100)
    writeAvroFile(avroFiles._2, createMessage(100)_, 1000)
    // Now convert the Avro file to a Parquet file (we could have generated one right away)
    convertAvroToParquetAvroFile(
      new Path(avroFiles._1.toString),
      new Path(parquetFiles._1.toString),
      User.getClassSchema,
      sqc.sparkContext.hadoopConfiguration)
    convertAvroToParquetAvroFile(
      new Path(avroFiles._2.toString),
      new Path(parquetFiles._2.toString),
      Message.getClassSchema,
      sqc.sparkContext.hadoopConfiguration)

    // Import the Parquet files we just generated and register them as tables
    sqc.parquetFile(parquetFiles._1.getParent.toString)
      .registerAsTable("UserTable")
    sqc.parquetFile(parquetFiles._2.getParent.toString)
      .registerAsTable("MessageTable")



    println("The age of User3:")
    println(findAgeOfUser("User3", sqc))
    println("The favorite color of User4:")
    println(findFavoriteColorOfUser("User4", sqc))
    println("Favorite color distribution:")
    val result = findColorDistribution(sqc)
    for (color <- result.keys) {
      println(s"color: $color count: ${result.apply(color)}")
    }
    findNumberOfMessagesSent(sqc).foreach {
      case (sender, messages) => println(s"$sender sent $messages messages")
    }
    findMutualMessageExchanges(sqc).foreach {
      case (user_a, user_b) => println(s"$user_a and $user_b mutually exchanged messages")
    }
    println("Count words in messages:")
    countWordsInMessages(sqc).toTraversable.foreach {
      case (word, count) => println(s"word: $word count: $count")
    }

    // Stop the SparkContext
    sqc.sparkContext.stop()

    // What follows is an example of how to use Avro objects inside Spark directly. For that we
    // need to register a few Kryo serializers. Note: this is only required if we would like to
    // use User objects inside Spark's MapReduce operations. For Spark SQL this is not required
    // and in fact it seems to mess up the Parquet Row serialization(?).
    setKryoProperties(conf)
    val sc = new SparkContext(conf)

    def myMapFunc(user: User): String = user.toString

    println("Let's load the User file as a RDD[User], call toString() on each and collect the result")
    val userRDD: RDD[User] = readParquetRDD[User](sc, parquetFiles._1.toString)
    userRDD.map(myMapFunc).collect().foreach(println(_))
    sc.stop()
    */

    //Generates the genotypes

    /*
    val datumWriter = new SpecificDatumWriter[T](
      prototype.getClass.asInstanceOf[java.lang.Class[T]])
    val dataFileWriter = new DataFileWriter[T](datumWriter)

    dataFileWriter.create(prototype.getSchema, file)
    for(i <- 1 to count) {
      dataFileWriter.append(factoryMethod(i))
    }
    dataFileWriter.close()
     */




    //ETS :: Generates dommy data for test purposes
    val NUMBER_OF_GENOTYPE = 20000000;
    val DATA_PATH = "data/"
    var fileName = "genotypes.parquet"

     val conf = new SparkConf(true).setMaster("local[10]").setAppName("ParquetAvroExample")

     // Create a Spark Context and wrap it inside a SQLContext
     sqc = new SQLContext(new SparkContext(conf))

    //createParquetFile(NUMBER_OF_GENOTYPE, Genotype.getClassSchema, new Path(DATA_PATH, fileName), sqc.sparkContext.hadoopConfiguration)

    //load and parse the file
    //sqc.parquetFile(DATA_PATH).registerAsTable("GenotypeTable")
    val dataFrame:DataFrame = sqc.read.parquet(DATA_PATH + fileName)//https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrame
    //dataFrame.registerTempTable("GenotypeTable")

    println("")
    println("******************************************************************");
    println("******************************************************************");
    //df3.printSchema()
    //println(sqc.sql("SELECT sampleDescription FROM GenotypeTable WHERE sampleId = 20").collect().apply(0).getString(0))
    //println(sqc.sql("SELECT variant[end] FROM GenotypeTable WHERE sampleId = 20").collect().apply(0).toString())
    dataFrame.select("variant.start", "variant.end").filter("start >= 20").filter("start <= 30").show()



    //aaa.foreach(fff(Row))

    println("******************************************************************");
    println("******************************************************************");
    println("")

    sqc.sparkContext.stop()


    /*val conf = new SparkConf(true).setMaster("local").setAppName("ParquetAvroExample")

    // Create a Spark Context and wrap it inside a SQLContext
    val sqlContext = new SQLContext(new SparkContext(conf))

    val df = sqlContext.read.load(DATA_PATH + fileName)*/
  }

  /**
   *
   */
  def deleteIfExist(path:String, fileName:String)
  {
    val fileTemp = new File(path, fileName);
    if (fileTemp.exists())
    {
      fileTemp.delete();
    }
  }


  /**
   *
   */
  def createParquetFile(numberOfGenotype: Int, schema: Schema, output: Path, conf: Configuration): Unit = {
    //Prepare the parquet file(s)...
    //We must make sure that the parquet file(s) are deleted because the following script doesn't replace the file.
    deleteIfExist(output.getParent().toString(), "genotypes.parquet");
    deleteIfExist(output.getParent().toString(), ".DS_Store");//mac computers create temporary files... and make the following code crash for nothing (spaks seems to take the first file it find)...

    val parquetWriter = new AvroParquetWriter[IndexedRecord](output, schema)

    for (i <- 0 until numberOfGenotype by 1)
    {
      parquetWriter.write(createGenotype(i))
    }

    parquetWriter.close()
  }

  /**
   *
   */
  def createGenotype(idx: Int): Genotype = {
    return Genotype.newBuilder()
      .setSampleId(idx.toString())
      .setSampleDescription("mySampleDescription" + idx.toString())
      .setVariant(Variant.newBuilder()
                  .setStart(idx.toLong)
                  .setEnd(idx.toLong + 10)
                  .setReferenceAllele("myReferenceAllele" + idx.toString())
                  .build())
      .build()
  }
}