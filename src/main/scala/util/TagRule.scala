package util

trait TagRule {
  /**
    * 设置打标签的抽象方法
    * @param args 可以是任何类型 并且可以是多个
    * @return
    */
  def makeTag(args:Any*):List[(String,Int)]

}
