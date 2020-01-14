import java.math.BigInteger
import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.graph.implementations.{AbstractEdge, SingleGraph, SingleNode}
import org.graphstream.graph.{Graph => GraphStream}
import com.mongodb.spark.MongoSpark
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.types.DoubleType

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSONArray
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.io.StdIn


object graph {
  def main(args:Array[String]):Unit={

    val sparkConf = new SparkConf()
      .setAppName("GraphStreamDemo")
      .setMaster("local")
      .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.4.0")
    val sparkSession=SparkSession.builder.config(sparkConf)
    val sc = new SparkContext(sparkConf)
    val ctx =new  SQLContext(sc)
    val inputuri = "mongodb://127.0.0.1:27017"
    val dataFrame=ctx.read
      .format("com.mongodb.spark.sql")
      .options(Map(
        "uri"->inputuri,
        "database"->"movie-all",
        "collection"->"movie"
      ))
      .load().filter(col("year")===2013)
//    print(dataFrame.count())


    //movie节点
    val movieDF = dataFrame
      .select(col("id").cast(LongType),
        col("title").cast(StringType))
//    movieDF.show()


    //解析dataframe
    val directorDF = dataFrame
      .select(col("id").cast(LongType),
        col("title"),
        col("director").cast(StringType))
    val actorListDF = dataFrame
      .select(col("id").cast(LongType),
        col("title"),
        col("actor").cast(StringType))
    val tagListDF = dataFrame
      .select(col("id").cast(LongType),
        col("title"),
        col("tags").cast(StringType))

    //拆解actor和tag
    val actorDF = actorListDF.withColumn("actor",explode(split(col("actor"),",|\\[|\\]")))
    val tagDF = tagListDF.withColumn("tags",explode(split(col("tags"),",|\\[|\\]")))

    val actorDF0 = actorDF.withColumn("type",col("id")*0+1)
    val tagDF0=tagDF.withColumn("type",col("id")*0+2)
    val directorDF0=directorDF.withColumn("type",col("id")*0+3)

    //合成新表
    val newNames = Seq("id","title","word","type")
    val actorDF2 = actorDF0.toDF(newNames:_*)
    val tagDF2 = tagDF0.toDF(newNames:_*)
    val directorDF2 = directorDF0.toDF(newNames:_*)
    val allTagDF = actorDF2.union(tagDF2.union(directorDF2)).filter(col("word")!=="")
//    allTagDF.show()


    //构造成边
    val relation = allTagDF.join(allTagDF,allTagDF("word")===allTagDF("word") and allTagDF("type")===allTagDF("type"))
//    print(relation.count())
    val relationRename = relation.toDF("id1","title1","word1","type1","id2","title2","word2","type2")
    val relationDis = relationRename.filter(col("id1")>col("id2"))
        relationDis.filter(col("id1")===10574621).show()


    //整合边权重
//    relationDis.groupBy("id1","id2").count().show()
    val relationWithWeight = relationDis.groupBy("id1","id2").agg(("type1","sum"))
    val relationWithWeightRename = relationWithWeight.toDF("id1","id2","weight")
    val relationnew = relationWithWeightRename.distinct()
    relationnew.filter(col("id1")===10574621).show()


    // 写入MongoDB
    relationWithWeightRename.write.options(Map(
      "spark.mongodb.output.uri"->"mongodb://127.0.0.1:27017/movie-all.edge"
    )).mode("overwrite").format("com.mongodb.spark.sql").save()
    movieDF.write.options(Map(
      "spark.mongodb.output.uri"->"mongodb://127.0.0.1:27017/movie-all.vertex"
    )).mode("overwrite").format("com.mongodb.spark.sql").save()


    //构造边RDD
//    val edges = EdgeRDD.fromEdges(relationnew.rdd.map(row=>Edge(
//      row.getAs[Long]("id1"),row.getAs[Long]("id2"), row(2)
//    )))
    val edges : RDD[Edge[Long]]=relationnew.rdd.map(row=>Edge(
      row.getAs[Long]("id1"),row.getAs[Long]("id2"), row.getAs[Long]("weight")))
    //构造节点RDD
    val vertices :RDD[(VertexId,String)] = movieDF.rdd.map(
      row=>(row.getAs[Long](0), row(1).toString)
    )
    val srcGraph = Graph(vertices, edges)


    println("==================start================")
    println("请输入电影名称：")
    val name:String = StdIn.readLine()

    val text = "["+name+"]"
    //目标节点
    var vid=0L
    srcGraph.vertices.filter{case (id,title)=>title==text}.collect.foreach{case(id,title)=>vid=id}


    //找邻居
    val list=List[(Long,Long)]()
//    srcGraph.edges.filter{case (id,title)=>title==text}.collect.foreach{case(id,title)=>vid=id}
    srcGraph.edges.filter{case Edge(id1,id2,weight)=>id1==vid}.collect.foreach{
      case Edge(id1,id2,weight) =>list :+ (id2,weight)
    }

    val neighbors:RDD[(Long,Long)] = sc.parallelize(list)
    //根据权重排序
    val neighborsSorted = neighbors.sortBy(f =>f._2)


    var listWithName = List[(Long,String)]()
    for(x <- neighborsSorted){
      srcGraph.vertices.filter{case (id,title)=>id==x._1}.collect.foreach{case(id,title)=>(x._1,title) +: listWithName}
    }

    print("推荐电影:")
    for(x <- listWithName){
      print(x)
    }



    //可视化
        val graph: SingleGraph = new SingleGraph("graphDemo")

        graph.setAttribute("ui.stylesheet", "url(file:\\G:\\spark\\src\\main\\resources\\graphStyle.css)")
        graph.setAttribute("ui.quality")
        graph.setAttribute("ui.antialias")
        //    load the graphx vertices into GraphStream
        for ((id, _) <- srcGraph.vertices.collect()){
          val node = graph.addNode(id.toString).asInstanceOf[SingleNode]
        }
        //    load the graphx edges into GraphStream edges
        for (Edge(x, y, _) <- srcGraph.edges.collect()){
          val edge = graph.addEdge(x.toString ++ y.toString, x.toString, y.toString, true).asInstanceOf[AbstractEdge]
        }
        graph.display()
  }

}
