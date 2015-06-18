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


import avrotest.avro.{Message, User}
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import parquet.avro.AvroParquetWriter



// our own classes generated from user.avdl by Avro tools

import java.io.File

import org.bdgenomics.formats.avro.{Variant, Genotype}

object ParquetAvroSparkExample {

  //This is the path where Parquet files will be created
  private val DATA_PATH = "data/"

  private var conf:SparkConf = _
  private var sqc:SQLContext = _

  def main(args: Array[String])
  {
    //Initialize Spark; this variable is always required
    conf = new SparkConf(true).setAppName("ParquetAvroExample").setMaster("local[2]")
    //Create a Spark Context and wrap it inside a SQLContext
    sqc = new SQLContext(new SparkContext(conf))

    //genotype_test()
    userAndMessages_test()


    //Close Spark to free up the memory
    sqc.sparkContext.stop()
  }

  /**
   *
   */
  def genotype_test(): Unit =
  {
    //This is the number of Genotype class that will be generated by the createParquetFile function.
    val NUMBER_OF_GENOTYPE:Int = 200;

    //This is the file name of the file that will contains the Genotypes objects
    val parquetFilePath:Path = new Path(DATA_PATH, "genotypes.parquet")

    //This call generates some Genotype classes and saves it in a Parquet file.  You can comment this line if you don't
    //want to re-generate the Parquet file everytime you test this program.
    createGenotypesParquetFile(NUMBER_OF_GENOTYPE, parquetFilePath)

    //This function query the Genotype parquet file
    queryGenotypeFile(parquetFilePath);
  }

   /**
   * This function generates a Genotype object for testing purpose
   *
   * @param idx allow us to identify the genotypes
   * @return a Genotype object
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

  /**
   * This function first make sure that the parquet file its about to create does not exist on the hard disk.
   * Then, it creates an AvroParquetWriter object that will be able to understand the Avro objects and writes them into a Parquet file
   * Then, it loops for the number of Genotype we want to create (see constant in the main function), create a Genotype object and write it in the Parquet File
   * The last line of this function close the Parquet file writer which commit and save the file on the disk.
   *
   * @param numberOfGenotype
   */
  def createGenotypesParquetFile(numberOfGenotype: Int, parquetFilePath: Path): Unit =
  {
    //We must make sure that the parquet file(s) are deleted because the following script doesn't replace the file.
    deleteIfExist(parquetFilePath.getParent().toString(), parquetFilePath.getName());
    //deleteIfExist(output.getParent().toString(), ".DS_Store");//mac computers create temporary files... and make the following code crash for nothing (spaks seems to take the first file it find)...

    val parquetWriter = new AvroParquetWriter[IndexedRecord](parquetFilePath, Genotype.getClassSchema())

    for (i <- 0 until numberOfGenotype by 1)
    {
      parquetWriter.write(createGenotype(i))
    }

    parquetWriter.close()
  }

  /**
   *
   * @param idx
   * @return
   */
  def createMessage(idx:Int, maxUsers: Int): avrotest.avro.Message =
  {
    val r = scala.util.Random
    val senderId:Int = r.nextInt(maxUsers)
    val recipientId:Int = r.nextInt(maxUsers)//We allow a user to send an email to himself... why not... I do it, you do it too...

    return Message.newBuilder()
                  .setID(idx.toLong)
                  .setSender(senderId)
                  .setRecipient(recipientId)
                  .setContent("The message #" + idx.toString())
                  .build()
  }

  /**
   *
   * @param numberOfUser
   * @param numberOfMessages
   * @param parquetFilePath
   */
  def createMessagesParquetFile(numberOfUser: Int, numberOfMessages: Int, parquetFilePath: Path): Unit =
  {
    //We must make sure that the parquet file(s) are deleted because the following script doesn't replace the file.
    deleteIfExist(parquetFilePath.getParent().toString(), parquetFilePath.getName());

    val parquetWriter = new AvroParquetWriter[IndexedRecord](parquetFilePath, Message.getClassSchema())

    for (i <- 0 until numberOfMessages by 1)
    {
      parquetWriter.write(createMessage(i, numberOfUser))
    }

    parquetWriter.close()
  }

  /**
   *
   * @param idx
   * @return
   */
  def createUser(idx: Int): User =
  {
    val r = scala.util.Random
    val age:Int = 18 + r.nextInt(100-18)//we want users between 18 and 100 years old
    val color:String = if(idx % 5 == 0) "purple" else if(idx % 3 == 0) "blue" else if(idx % 2 == 0) "orange" else "red"

    return User.newBuilder()
                .setId(idx)
                .setName("UserName" + idx.toString())
                .setAge(age)
                .setFavoriteColor(color)
                .build()
  }

  /**
   *
   * @param numberOfUser
   * @param parquetFilePath
   */
  def createUsersParquetFile(numberOfUser: Int, parquetFilePath: Path): Unit =
  {
    //We must make sure that the parquet file(s) are deleted because the following script doesn't replace the file.
    deleteIfExist(parquetFilePath.getParent().toString(), parquetFilePath.getName());

    val parquetWriter = new AvroParquetWriter[IndexedRecord](parquetFilePath, User.getClassSchema())

    for (i <- 0 until numberOfUser by 1)
    {
      parquetWriter.write(createUser(i))
    }

    parquetWriter.close()
  }

  /**
   * This function delete the file on the disk if it exist.
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
  def queryGenotypeFile(parquetFilePath: Path): Unit =
  {
    //Here we load the parquet file into a DataFrame object.  This will allow us to query the data
    //Consult the following documentation if you want to know what your query options are: https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrame
    val dataFrame:DataFrame = sqc.read.parquet(parquetFilePath.toString())

    println("")
    println("******************************************************************");
    println("******************************************************************");

    //Prints the schema of the Parquet file
    //dataFrame.printSchema()

    //Select some data and output it to the console
    dataFrame.select("variant.start", "variant.end").filter("start >= 20").filter("start <= 30").show()

    println("******************************************************************");
    println("******************************************************************");
    println("")
  }

  /**
   *
   * @param userParquetFilePath
   * @param messageParquetFilePath
   */
  def queryUserAndMessageFiles(userParquetFilePath:Path, messageParquetFilePath:Path): Unit =
  {
    //Here we load the parquet files into DataFrames object.  This will allow us to query the data
    //Consult the following documentation if you want to know what your query options are: https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrame
    val usersDataFrame:DataFrame = sqc.read.parquet(userParquetFilePath.toString())
    val messagesDataFrame:DataFrame = sqc.read.parquet(messageParquetFilePath.toString())

    val usersMessagesDataFrame:DataFrame = usersDataFrame.join(messagesDataFrame, usersDataFrame("id") === messagesDataFrame("sender"), "inner")

    println("")
    println("******************************************************************");
    println("******************************************************************");

    //Prints the schema of the Parquet file
    //dataFrame.printSchema()

    //This example show the messages sent by the users with the id between 20 and 30
    usersMessagesDataFrame.select("id", "name", "age", "favorite_color", "recipient", "content")
                          .filter("id >= 20").filter("id <= 30")
                          .show()

    //This example show you how to select the users who sent message(s) to themself
    usersMessagesDataFrame.filter("sender = recipient")
                          .select("id", "name", "age", "favorite_color", "recipient", "content")
                          .filter("id >= 20")
                          .filter("id <= 30")
                          .show()

    println("******************************************************************");
    println("******************************************************************");
    println("")
  }

  /**
   *
   */
  def userAndMessages_test()
  {
    //This is the number of Genotype class that will be generated by the createParquetFile function.
    val NUMBER_OF_USERS:Int = 2000;
    val NUMBER_OF_MESSAGES:Int = 2000000;

    //Define the Users and Messages parquet file paths
    val userParquetFilePath:Path = new Path(DATA_PATH, "users.parquet")
    val messageParquetFilePath:Path = new Path(DATA_PATH, "messages.parquet")


    createUsersParquetFile(NUMBER_OF_USERS, userParquetFilePath)
    createMessagesParquetFile(NUMBER_OF_USERS, NUMBER_OF_MESSAGES, messageParquetFilePath)

    queryUserAndMessageFiles(userParquetFilePath, messageParquetFilePath);
  }
}