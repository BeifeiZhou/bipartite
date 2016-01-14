package bipartite

import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object recsys {
  def main(args: Array[String]): Unit = {
  val sc = new SparkContext(new SparkConf().setAppName("Graphx").setMaster("local"))
  val data = sc.textFile("train10000.txt")
    .map(s => s.split(","))
    .filter(s => s(2).toDouble > 0.5)
    .map(s => (s(0), s(1))) 
    
   val userItemDict = data.groupBy(s => s._1).map{s=>
    val itemList = s._2.toList.map(ss => ss._2)
    (s._1, itemList)
   }.collect.toMap
   val itemUserDict = data.map(s => (s._2, s._1)).groupBy(s => s._1).map{s=>
     val userList = s._2.toList.map(ss => ss._2)
     (s._1, userList)
   }.collect.toMap
    
   val users = data.map(s => s._1).distinct
   val usersCll = users.collect.toList
   val items = data.map(s => s._2).distinct
   val itemsCll = items.collect.toList
   
   val edges = data.map(s => Edge(s._1.toLong, s._2.toLong, 0)) 
   val vertices = data.map(s => s._1.toLong).map(s => (s,"")) ++ data.map(s => s._2.toLong).map(s => (s, ""))
   val graph = Graph(vertices , edges, "")
   val inDegrees = graph.inDegrees.map(s => (s._1.toString, s._2)).collect.toMap
   val outDegrees = graph.outDegrees.map(s => (s._1.toString, s._2)).collect.toMap
   val weightItems = items.cartesian(items)  

   def weightCalculation(x: (String, String)) = { 
    val kxj = inDegrees(x._2)
    val userI = itemUserDict(x._1)
    val userJ = itemUserDict(x._2)

    val commonUsers = userI.intersect(userJ)
    val sum = if (commonUsers.length == 0) 0 else commonUsers.map(s => 1/outDegrees(s).toDouble).sum
    1/kxj.toDouble*sum
   }   
   val weight = weightItems.map(s => (s, weightCalculation(s))).filter(s => s._2 != 0.0).collect.toMap  
   
   def topK (x: (String, List[String]), y: Int) = {
     val uncllItems = x._2    
     val scores = uncllItems.map{s=>
       val cllItems = itemsCll.diff(uncllItems)
       val score = cllItems.map{ss => 
         val tmp = if (weight.contains(s, ss)) weight((s, ss)) else 0.0
         tmp
       }.sum
       score
     }
     x._1+","+uncllItems.zip(scores).sortWith((a,b) => a._2 > b._2).map(s => s._1).take(y).mkString(",")
   }   
   
   val userUncollectItems = data.groupBy(s => s._1).map{s=>
    val itemList = s._2.toList.map(ss => ss._2)
    (s._1, itemsCll.diff(itemList))
   }
   
   val top50 = userUncollectItems.map(s => topK(s, 50))
   top50.take(100).foreach(println)
  }
}
