package xia.v.lan.hotitems

/**
  * @author chenhao
  * @description <p>
  *              created by chenhao 2019/9/2 14:11
  */
class ItemViewCount(){

  var itemId: Long = 0
  var windowEnd: Long = 0
  var viewCount: Long = 0

  def this(itemId:Long,windowEnd:Long,viewCount:Long){
    this()
    this.itemId = itemId
    this.windowEnd = windowEnd
    this.viewCount = viewCount
  }
}
