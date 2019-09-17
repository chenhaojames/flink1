package api.day1

import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
  * @author chenhao
  * @description <p>
  *              created by chenhao 2019/9/10 15:31
  */
object PageRankImpl {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val pageInputPath = ""
    val linkInputPath = ""
    val pages: DataSet[Page] = env.readCsvFile[Page](pageInputPath)
    val links = env.readCsvFile[Link](linkInputPath)
    var numPages = 100
    val pagesWithRanks = pages.map(p => Page(p.pageId,1.0/numPages))
    val adjacencyLists  = links.map(p => AdjacencyList(p.sourceId,Array(p.targetId)))
      .groupBy("sourceId")
      .reduce((l1,l2) => AdjacencyList(l1.sourceId,l1.targetIds ++ l2.targetIds))
    val maxIterMination = 100



  }

  case class Link(sourceId: Long, targetId: Long)
  case class Page(pageId: Long, rank: Double)
  case class AdjacencyList(sourceId: Long, targetIds: Array[Long])
}
