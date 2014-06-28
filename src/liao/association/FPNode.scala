package liao.association
import java.util.ArrayList
import java.util.List
/**
 * Node of Frequency Patterns Tree
 */
class FPNode (
    var itemID: String = null,
    var counter: Int = 1,
    var parent: FPNode = null,
    var childs: List[FPNode] = new ArrayList[FPNode],
    var nodeLink: FPNode = null) extends Serializable{
  def getChildWithID(id: String): FPNode =  {
    var ret: FPNode = null
    for (i <- 0 until childs.size()) {
      val child = childs.get(i)
      if (child.itemID.equals(id)) ret = child
    }
    ret
  }
}

object FPNode {
  def main(args: Array[String]) = {
    val node = new FPNode
    println(node.itemID)
    println(node.childs.size())
    node.childs.add(null)
    println(node.childs.size())
    //node.setItemId("12")
    println(node.itemID)
  }
}