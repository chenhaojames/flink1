package xia.v.lan.scala.t1

/**
  * @author chenhao
  * @description <p>
  *              created by chenhao 2019/7/5 14:22
  */
object HelloScala {

  def testArray():Unit = {
    var myList = Array(1.9, 2.9, 3.4, 3.5)

    // 输出所有数组元素
    for ( x <- myList ) {
      println( x )
    }

    // 计算数组所有元素的总和
    var total = 0.0;
    for ( i <- 0 to (myList.length - 1)) {
      total += myList(i);
    }
    println("总和为 " + total);

    // 查找数组中的最大元素
    var max = myList(0);
    for ( i <- 1 to (myList.length - 1) ) {
      if (myList(i) > max) max = myList(i);
    }
    println("最大值为 " + max);
  }

  def testConcatArray: Unit = {
    var myList1 = Array(1.9, 2.9, 3.4, 3.5)
    var myList2 = Array(8.9, 7.9, 0.4, 1.5)

    var myList3 =   Array.concat(myList1,myList2)

    // 输出所有数组元素
    for ( x <- myList3 ) {
      println( x )
    }
  }

  def testCollection:Unit = {
    // 定义整型 List
    val x = List(1,2,3,4)

    // 定义 Set
    val x1 = Set(1,3,5,7)

    // 定义 Map
    val x2 = Map("one" -> 1, "two" -> 2, "three" -> 3)

    // 创建两个不同类型元素的元组
    val x3 = (10, "Runoob")

    // 定义 Option
    val x4:Option[Int] = Some(5)

    val ite = x.iterator;
    for(t <- ite){
      println(t)
    }
  }

  def main(args: Array[String]): Unit = {
    println(" ' " + "i")
    factor = 5
    println( "muliplier(1) value = " +  multiplier(1) )
    println( "muliplier(2) value = " +  multiplier(2) )
    testArray()
    testConcatArray
    testClass
  }

  def testPattern:Unit = {
    val pattern = "Scala".r
    val str = "Scala is Scalable and cool"
    println(pattern.findFirstIn(str))
  }

  /**
    * 方法
    * @param x
    * @return
    */
  def m(x: Int) = x + 3

  /**
    * 函数
    */
  val f = (x: Int) => x + 3

  /**
    * 闭包
    */
  var factor = 3
  val multiplier = (i:Int) => i * factor


  def testClass() : Unit = {
    val pt = new Point(10, 20);

    // 移到一个新的位置
    pt.move(10, 10);
  }

}

class Point(xc: Int, yc: Int) {
  var x: Int = xc
  var y: Int = yc

  def move(dx: Int, dy: Int) {
    x = x + dx
    y = y + dy
    println ("x 的坐标点: " + x);
    println ("y 的坐标点: " + y);
  }

}

class Location(val xc: Int, val yc: Int,
               val zc :Int) extends Point(xc, yc){
  var z: Int = zc

  def move(dx: Int, dy: Int, dz: Int) {
    x = x + dx
    y = y + dy
    z = z + dz
    println ("x 的坐标点 : " + x);
    println ("y 的坐标点 : " + y);
    println ("z 的坐标点 : " + z);
  }
}

trait Equal {
  def isEqual(x: Any): Boolean
  def isNotEqual(x: Any): Boolean = !isEqual(x)
}

class Point1(xc: Int, yc: Int) extends Equal {
  var x: Int = xc
  var y: Int = yc
  def isEqual(obj: Any) =
    obj.isInstanceOf[Point1] &&
      obj.asInstanceOf[Point1].x == x

  def matchTest(x: Any): Any = x match {
    case 1 => "one"
    case "two" => 2
    case y: Int => "scala.Int"
    case _ => "many"
  }
}
