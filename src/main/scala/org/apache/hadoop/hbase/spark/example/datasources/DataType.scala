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

package org.apache.hadoop.hbase.spark.example.datasources

import org.apache.hadoop.hbase.spark.datasources.HBaseSparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.apache.spark.{SparkConf, SparkContext}

class UserCustomizedSampleException(message: String = null, cause: Throwable = null) extends
  RuntimeException(UserCustomizedSampleException.message(message, cause), cause)

object UserCustomizedSampleException {
  def message(message: String, cause: Throwable) =
    if (message != null) message
    else if (cause != null) cause.toString()
    else null
}

case class IntKeyRecord(
                         col0: Integer,
                         col1: Boolean,
                         col2: Double,
                         col3: Float,
                         col4: Int,
                         col5: Long,
                         col6: Short,
                         col7: String,
                         col8: Byte)

object IntKeyRecord {
  def apply(i: Int): IntKeyRecord = {
    IntKeyRecord(if (i % 2 == 0) i else -i,
      i % 2 == 0,
      i.toDouble,
      i.toFloat,
      i,
      i.toLong,
      i.toShort,
      s"String$i extra",
      i.toByte)
  }
}

object DataType {
  val cat =
    s"""{
       |"table":{"namespace":"default", "name":"DataTypeExampleTable"},
       |"rowkey":"key",
       |"columns":{
       |"col0":{"cf":"rowkey", "col":"key", "type":"int"},
       |"col1":{"cf":"i", "col":"col1", "type":"boolean"},
       |"col2":{"cf":"i", "col":"col2", "type":"double"},
       |"col3":{"cf":"i", "col":"col3", "type":"float"},
       |"col4":{"cf":"i", "col":"col4", "type":"int"},
       |"col5":{"cf":"i", "col":"col5", "type":"bigint"},
       |"col6":{"cf":"i", "col":"col6", "type":"smallint"},
       |"col7":{"cf":"i", "col":"col7", "type":"string"},
       |"col8":{"cf":"i", "col":"col8", "type":"tinyint"}
       |}
       |}""".stripMargin

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("DataTypeExample").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    val data = (0 until 32).map { i =>
      IntKeyRecord(i)
    }
    sc.parallelize(data).toDF.write.options(
      Map(HBaseTableCatalog.tableCatalog -> cat, HBaseTableCatalog.newTable -> "1",
        HBaseSparkConf.HBASE_CONFIG_LOCATION -> "hbase-site.xml", HBaseSparkConf.USE_HBASECONTEXT -> "false")
    )
      .format("org.apache.hadoop.hbase.spark")
      .save()
  }
}
