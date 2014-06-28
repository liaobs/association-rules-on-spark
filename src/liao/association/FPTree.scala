package liao.association

import java.util.ArrayList
import java.util.Collections
import java.util.Comparator
import java.util.HashMap
import java.util.List
import java.util.Map

/**
 * Frequency Patterns Tree
 */
class FPTree(
    var headerList: List[String] = null,
    var mapItemNodes: Map[String, FPNode] = new HashMap[String, FPNode](1000),
    var hasMoreThanOnePath: Boolean = false,
    var mapItemLastNode: Map[String, FPNode] = new HashMap[String, FPNode](1000),
    var root: FPNode = new FPNode) extends Serializable{
  /**
   *  Function to fix the node link for an item after inserting a new node.
   *  @param item  the item of the new node
   *  @param newNode the new node that has been inserted.
   **/
  def fixNodeLinks(item: String, newNode: FPNode) = {
    val lastNode = mapItemLastNode.get(item)
    // if not null, then we add the new node to the node link of the last node
    if(lastNode != null) lastNode.nodeLink = newNode
    // Finally, we set the new node as the last node 
    mapItemLastNode.put(item, newNode)
    val headernode = mapItemNodes.get(item)
    if(headernode == null) mapItemNodes.put(item, newNode)
  }

	/**
	 * Function for adding a transaction to the fp-tree (for the initial construction
	 * of the FP-Tree).
	 * @param transaction
	 */
	def addTransaction(transaction: List[String]) = {
		var currentNode = root
		// For each item in the transaction
		for(i <- 0 until transaction.size()){
		  val item = transaction.get(i)
		  if (item != null && item.size > 0) {
			// look if there is a node already in the FP-Tree
		  val child = currentNode.getChildWithID(item)
			if(child == null){ 
				// there is no node, we create a new one
				val newNode = new FPNode()
				newNode.itemID = item
				newNode.parent = currentNode
				// we link the new node to its parent
				currentNode.childs.add(newNode)
				
				// check if more than one path
				if(!hasMoreThanOnePath && currentNode.childs.size() > 1) {
					hasMoreThanOnePath = true;
				}
				
				// we take this node as the current node for the next for loop iteration 
				currentNode = newNode;
				
				// We update the header table.
				// We check if there is already a node with this id in the header table
				fixNodeLinks(item, newNode);
			}else{ 
				// there is a node already, we update it
				child.counter += 1
				currentNode = child
			}
		}
		}
	}
	/**
	 * Function for adding a prefixpath to a fp-tree.
	 * @param prefixPath  The prefix path
	 * @param mapSupportBeta  The frequencies of items in the prefixpaths
	 * @param relativeMinsupp
	 */
	def addPrefixPath(
			prefixPath: List[FPNode], 
			mapSupportBeta: Map[String, Int], 
			relativeMinsupp: Int) = {
		// the first element of the prefix path contains the path support
		val pathCount = prefixPath.get(0).counter  
		
		var currentNode = root
		// For each item in the transaction  (in backward order)
		// (and we ignore the first element of the prefix path)
		for(i <- prefixPath.size()-1 to 1){ 
			val pathItem = prefixPath.get(i);
			// if the item is not frequent we skip it
			if(mapSupportBeta.get(pathItem.itemID) >= relativeMinsupp){
				val child = currentNode.getChildWithID(pathItem.itemID);
				if(child == null){ 
					// there is no node, we create a new one
					val newNode = new FPNode();
					newNode.itemID = pathItem.itemID;
					newNode.parent = currentNode;
					newNode.counter = pathCount;  // SPECIAL 
					currentNode.childs.add(newNode);

					// check if more than one path
					if(!hasMoreThanOnePath && currentNode.childs.size() > 1) {
						hasMoreThanOnePath = true;
					}
				
					currentNode = newNode;
					// We update the header table.
					// We check if there is already a node with this id in the header table
					fixNodeLinks(pathItem.itemID, newNode);
				}else{ 
					// there is a node already, we update it
					child.counter += pathCount;
					currentNode = child;
				}
			}
		}
	}

	/**
	 * Function for creating the list of items in the header table, in 
	 * descending order of frequency.
	 * @param mapSupport the frequencies of each item.
	 */
	def createHeaderList(mapSupport: Map[String, Int]) {
		// create an array to store the header list with
		// all the items stored in the map received as parameter
		headerList =  new ArrayList[String](mapItemNodes.keySet())
		
		// sort the header table by decreasing order of support
		Collections.sort(headerList, new Comparator[String](){
			def compare(id1: String, id2: String): Int = {
				// compare the support
				val compare = mapSupport.get(id2) - mapSupport.get(id1);
				// if the same support, we check the lexical ordering!
				if(compare ==0){ 
					id1.compareTo(id2)
				}
				// otherwise use the support
				compare
			}
		})
	}

}

