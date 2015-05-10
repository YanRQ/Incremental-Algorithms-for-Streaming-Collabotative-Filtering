
package org.apache.spark.mllib.linalg.distributed

import com.brkyvz.spark.recommendation.LatentFactor
import com.brkyvz.spark.recommendation.LatentMatrixFactorizationModel
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer
import collection.mutable.HashMap
import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.util.random.XORShiftRandom

import org.apache.spark.{SparkException, Logging, Partitioner}
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
case class Rating(user: Int, product: Int, rating: Double)
case class coordinate(var x: Int, var y: Int)
case class LatentFactor(bias:Array[Array[Double]])

class CollabFilterModel(val numPartForRow: Int,
                        val numPartForCol: Int,
                        var blockMat: BlockMatrix,
                        val numUsers: Int,
                        val numProducts: Int,
                        val kValues: Int,
                        val stepSize: Double,
                        val lambda: Double,
                        val features: Int,
                        //      var U: Array[Array[Double]] = null,
                        //        var V: Array[Array[Double]] = null,
                        var U: Array[Array[Double]]=null,
                        var V: Array[Array[Double]]=null,
                        var  blockMatFlag: DenseMatrix,
                        var blockMatrixFlagUpdate: DenseMatrix,
                        val seed: Int,
                        val rowsPerBlock: Int,
                        val colsPerBlock: Int
                         ) {
  private var flagU = new Array[Boolean](numPartForRow)
  private var flagV = new Array[Boolean](numPartForCol)
  val perRow = numUsers / numPartForRow
  val perCol = numProducts / numPartForCol
  var availableBlock = new Array[coordinate](5)

  //  def HashMap(entry: RDD[(i: Int, j: Int)]) = {
  //
  //  }
  def selectBlock(): Unit = {
    var count: Int = 0
    var t: Int = 0
    var j: Int = 0
    var num: Int = 0
    var tmpU = flagU
    var tmpV = flagV
    while (num < 5) {
      while (t < numPartForRow) {
        while (j < numPartForCol) {
          if (blockMatrixFlagUpdate.apply(t, j) == 1.0 && tmpU(t) == true && tmpV(j) == true) {
            availableBlock(num).x = t
            availableBlock(num).y = j
            num += 1
            tmpU(t) = false
            tmpV(j) = false
            j += 1
            blockMatrixFlagUpdate.update(t, j, 0.0)
          }
          t += 1
          count += 1
        }
      }
      if (count == numPartForCol * numPartForRow) num = 10 // need fix
    }
  }

  def checkWhenEnd(): Boolean = {
    var t: Int = 0
    var j: Int = 0
    while (t < numPartForRow) {
      while (j < numPartForCol) {
        if (blockMatrixFlagUpdate.apply(t, j) == 1) {
          false
        }
        j += 1
      }
      t += 1
    }
    true
  }

  def Filter(data: ((Int, Int), Matrix)): Boolean = {
    var t = 0
    while (t < 5) {
      if (data._1._1 == availableBlock(t).x && data._1._2 == availableBlock(t).y) {
        t = 10
        true
      }
      t += 1
    }
    false
  }

  def update(data: RDD[MatrixEntry], decayFactor: Double, timeUnit: String): Unit = {
    //sc.textfile("")
    val rand = new XORShiftRandom(seed)
    //Initialize U
/*    val U = U match {
      case null => Array.fill(numUsers)(Array.fill(kValues)(rand.nextInt(1)))
      case _ => U
    }*/
	if(U==null){
		U=Array.fill(numUsers)(Array.fill(kValues)(rand.nextDouble()))
	}
    //Initialize V
/*    val V = V match {
      case null => Array.fill(numProducts)(Array.fill(kValues)(rand.nextInt(1)))
      case _ => V
    }*/
	if(V==null){
		V=Array.fill(numProducts)(Array.fill(kValues)(rand.nextDouble()))
	}	

    blockMatrixFlagUpdate = blockMatFlag

    //case null => DenseMatrix.map(i,j,rand.nextInt(1)) })
    // null => new BlockMatrix((Int,Int,new DenseMatrix.map()),perRow,perCol,numUsers,numProducts)

    // val coordMat: CoordinateMatrix = new CoordinateMatrix(data, numUsers, numProducts)
    // blockMat = coordMat.toBlockMatrix(perRow, perCol)
    val sc = data.sparkContext
    val dataCol = data.collect()

    dataCol.foreach { entry =>
      val col = (entry.j / colsPerBlock).toInt
      val row = (entry.i / rowsPerBlock).toInt
      blockMatFlag.update(row, col, 1.0)
      flagU(row) = true
      flagV(col) = true
    }

    var U_br = sc.broadcast(U)
    var V_br = sc.broadcast(V)
    var blockMatFlag_br = sc.broadcast(blockMatFlag)
    var flagU_br = sc.broadcast(flagU)
    var flagV_br = sc.broadcast(flagV)

    val mark = data.map { x => (((x.i / rowsPerBlock).toInt, (x.j / colsPerBlock).toInt), (x.i, x.j, x.value))}

    val tmpblockMat: RDD[((Int, Int), Matrix)] = blockMat.blocks.join(mark).map {
      case ((blockRowIndex, blockColIndex), (matrix, (userID, itemID, rating))) =>
        val effrow = (userID - blockRowIndex * rowsPerBlock).toInt
        val effcol = (itemID - blockColIndex * colsPerBlock).toInt
        matrix.update(effrow, effcol, rating)
        ((blockRowIndex, blockColIndex), matrix)
    }
    val tmpblockMatAll = new BlockMatrix(tmpblockMat, numUsers, numProducts)

    blockMat = tmpblockMatAll

    while (!checkWhenEnd()) {
      selectBlock()
      blockMat.blocks.filter(entry => Filter(entry)).map { case ((blockRowIndex, blockColIndex), matrix) =>
        for(i <-0 until rowsPerBlock-1) {
          for (j <- 0 until colsPerBlock - 1) {
            val u=U_br.value
            val v=V_br.value
            (U,V)=SGD(matrix.apply(i,j),i,j,blockRowIndex,blockColIndex,0.1,0.1,u,v)
          }
        }
        U_br=sc.broadcast(U)
        V_br=sc.broadcast(V)
      }
    }


  }


  private[spark] def SGD(
                          rating: Double,
                          effrow: Int,
                          effcol: Int,
                          blockRowIndex:Int,
                          blockColIndex:Int,
                          userPenalty:Double,
                          proPenalty:Double,
                          uFeatures: Array[Array[Double]],
                          pFeatures: Array[Array[Double]]): (LatentFactor,LatentFactor)  = {//(Array[Array[Double]], Array[Array[Double]])

    var dotProduct=0.0

    for(k <-0 until kValues-1){
      dotProduct += uFeatures(blockRowIndex+effrow)(k)*pFeatures(blockColIndex+effcol)(k)
    }

    val ratingDiff=dotProduct - rating

    for(k<-0 until kValues-1){
      val oldUserWeight=uFeatures(blockRowIndex+effrow)(k)
      val oldProWeight=pFeatures(blockColIndex+effcol)(k)
      uFeatures(blockRowIndex+effrow)(k) -= stepSize*(ratingDiff*oldProWeight + userPenalty * oldUserWeight)
      pFeatures(blockColIndex+effcol)(k) -= stepSize*(ratingDiff*oldUserWeight + proPenalty * oldProWeight)
    }

    (new LatentFactor(uFeatures), new LatentFactor(pFeatures))

  }


  //    def updateBookkeeping(data: RDD[(Int, Int), BlockMatrix]): Unit {
  //
  //    }


}


//var B = blockMat.

//matrix.map((numRow,numCol,value)=>RDD)

/*
    while(!unlockFlagAll) {
      data.foreach { case (i, j, v) =>
        val indexU = checkU(i)
        val indexV = checkV(j)

  //def updateBlocks(i, j, indexU, indexV)Untitled Document = v
  //      if(checkFlag(indexU, indexV)) {
  //        lockFlag(indexU, indexV)
  //        runSGD(i, j, v)
  //        unlockFlag(indexU, indexV)

        }
      def selectBlock() {
      }
     // blockMat.map(runSGD())


      }
    }

    new CollabFilterModel(numPartForRow, numPartForCol, blockMat) {
    }
  }

*/




class CollabFilter() {
  var model: CollabFilterModel = new CollabFilterModel()


  def train(data: DStream[Rating]) {
    data.foreachRDD { rdd =>
      model = model.update(rdd)
    }
  }

  def predictOn(data: DStream[Vector]): DStream[Double] = {
    data.map(model.predict)
  }


}

object CF {


}


