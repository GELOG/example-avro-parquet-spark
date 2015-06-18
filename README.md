Spark SQL, Avro and Parquet
===========================

This tutorial is based on the following tutorial: https://github.com/AndreSchumacher/avro-parquet-spark-example.

It shows how to query data stored as Avro objects stored
inside a columnar format (Parquet) via the Spark query
interface. The main intention of the tutorial is to show the seamless
integration of the functional RDD operators that come with Spark and
its SQL interface. For users who are unfamiliar with Avro we show how
to make use of Avro interface description language (IDL) inside a
Spark Maven project.

Building the example
--------------------

```
$ git clone https://github.com/apache/spark.git
$ cd spark
$ sbt/sbt clean publish-local
```

Then in a different directory

```
$ git clone https://github.com/GELOG/example-avro-parquet-spark.git
$ cd example-avro-parquet-spark
$ mvn package
```

Project setup
-------------

Here we are using Maven to build the project due to the available Avro
IDL compiler plugin. Obviously one could have achieved the same goal
using sbt.

There are two subprojects:

* `example-format`, which contains the Avro description of the primary
  data record we are using (`Adam format`)
* `example-code`, which contains the actual code that saves to parquets files and executes the queries

There are two ways to specify a schema for Avro records: via a
description in JSON format or via the IDL.  We chose the latter since
it is easier to comprehend.

Our example models the Adam Format database with is detailed here: https://github.com/GELOG/adam-ibs/wiki/ADAM_Format.


This file is stored as part of the `example-format` project and is
eventually compiled into a Java implementation of the class that
represents these types of records. Note that the different
attributes are defined via their name, their type and an optional
default value. For more information on how to specify Avro records see
[the Avro documentation](http://avro.apache.org/docs/current/idl.html).

Part of the description is also the _namespace_ of the protocol, which
will result in the package name of the classes that will be generated
from the description. We use the Avro maven plugin to do this
transformation. It is added to `example-format/pom.xml` as follows:

```xml
<plugin>
    <groupId>org.apache.avro</groupId>
    <artifactId>avro-maven-plugin</artifactId>
</plugin>
```

Data generation
---------------

Once the code generation has completed, this test builds objects of type `Genotype` this way:

```Scala
import org.bdgenomics.formats.avro.Genotype
import org.bdgenomics.formats.avro.Variant

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
```

We can create a set of Genotypes and store these inside a Parquet file as follows.

```Scala
  import parquet.avro.AvroParquetWriter

  def createParquetFile(numberOfGenotype: Int, schema: Schema, output: Path, conf: Configuration): Unit = {
        //We must make sure that the parquet file(s) are deleted because the following script doesn't replace the file.
    deleteIfExist(output.getParent().toString(), "genotypes.parquet");
    
    val parquetWriter = new AvroParquetWriter[IndexedRecord](output, schema)

    for (i <- 0 until numberOfGenotype by 1)
    {
      parquetWriter.write(createGenotype(i))
    }

    parquetWriter.close()
  }
```


Import into Spark SQL
---------------------

The data written in the last step can be directly imported as a DataFrame
inside Spark and then queried. This can be done as follows.

```Scala
val conf = new SparkConf(true).setMaster("local[10]").setAppName("ParquetAvroExample")

sqc = new SQLContext(new SparkContext(conf))

val dataFrame:DataFrame = sqc.read.parquet(DATA_PATH + fileName)
```


Querying the Genotype databases
---------------------------------------

After the files have been registered, they can queried via Spark functionalities, for example:

```Scala
dataFrame.select("variant.start", "variant.end").filter("start >= 20").filter("start <= 30").show()
```

The result will be returned as a sequence of `Row` objects, whose
fields can displayed by the `show()` functions (which only display the first 20 records). 
