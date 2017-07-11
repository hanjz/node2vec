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

package com.navercorp

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.time.LocalDate

import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.test.{TestHive, TestHiveContext}

import scala.util.Random
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SQLImplicits}
import org.scalatest.FunSuiteLike
import org.scalatest.SuiteMixin
import org.scalatest.BeforeAndAfterAll

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, ListBuffer, WrappedArray}

class node2vecSuite extends org.scalatest.FunSuiteLike with org.scalatest.BeforeAndAfterAll {
  var sc: SparkContext = _
  var sqlContext: SQLContext = _

  val videoActorSchema = StructType(Array(
    StructField("video_id", StringType, true),
    StructField("artists", StringType, true)
  ))

  var videoRows = new ListBuffer[Row]()

  videoRows += Row("100", "4976: Jack, 6167: Kate")
  videoRows += Row("101", "4976: Jack, 6167: Kate")
  videoRows += Row("102", "4975: Tom, 6167: Kate")

  val videos = TestHive.createDataFrame(TestHive.sparkContext.parallelize(videoRows), videoActorSchema)

  TestHive.sql("CREATE TABLE IF NOT EXISTS videoActor (video_id varchar(128), artists string)")

  videos.write.mode("overwrite").saveAsTable("videoActor")

  val videoActorTable =  TestHive.sql(s"SELECT video_id, artists FROM videoActor")

  override def beforeAll() {
    super.beforeAll()

    sc = TestHive.sparkContext
    sqlContext = new SQLContext(sc)
  }

  test("read hive-table") {
    //    videoActorTable.show()
    videoActorTable.flatMap{
      row => {
        val videoID = row(0).asInstanceOf[String]
        val video_artists = row(1).asInstanceOf[String].
          trim().split(",").
          map{ s => s"$videoID  ${s.trim().split(":")(0)}"}
        video_artists
      }
    }
  }

}
