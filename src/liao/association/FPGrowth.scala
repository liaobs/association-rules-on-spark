package liao.association

import java.util.HashMap
import java.util.List
import java.util.ArrayList
import java.util.Map
import java.lang.Math
import java.util.Collections;
import java.util.Comparator;
import scala.collection.mutable.StringBuilder
import scala.collection.mutable.ArrayBuffer


import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

class FPGrowth(
		var transactionsNum: Int = 0,
		var relativeMinsupp: Int = 0) extends Serializable{
	/**
	 * Count each item occurrences in all transactions
	 * each transaction is an element of RDD
	 */
	def getItemCountPairs(transactions: RDD[String]): RDD[(String, Int)] = {
		val items = transactions.flatMap(t => t.split("\\s")).map(t => (t, 1))
		items.reduceByKey(_+_)
	}
	/**
	 * Set the total number of transactions
	 */
	def setTransactionsNum(transactions: RDD[String]) = {
		this.transactionsNum = transactions.count.toInt
	}
	/**
	 * Transform item-count pair RDD into a map [item, count]
	 * the results will return to the machine, which executed drive program
	 * Note: using java Hashmap
	 */
	
	def transformMapSupport(
			itemCountPairs: RDD[(String, Int)]): Map[String, Int] = {
		val pairs = itemCountPairs.collect
		val mapSupp = 
			new HashMap[String, Int](itemCountPairs.count.toInt + 10, 0.99f)
		pairs.foreach(p => mapSupp.put(p._1, p._2))
		mapSupp
	}
	
	/**
	 * Convert the absolute minimum support ratio(Double) to a relative minimum
	 * support(Int)
	 * Note: call the function after calling setTransactionsNum
	 */
	def setMinSupport(minSupportRate: Double) = {
		relativeMinsupp = Math.ceil(minSupportRate * transactionsNum).toInt
	}
	/**
	 * Main function to get frequent itemsets by sampling transactions
	 * IF transactionsNum > sampleSize do-sample ELSE the whole transactions
	 */
	def frequentItemSetsBySamples(
			transactionsRDD: RDD[String], 
			minsuppRate: Double, 
			sampleSize: Int): ArrayList[(Array[String], Int)] = {
		val transactionsNum = transactionsRDD.count.toInt
		val sampledTransactions = 
			if (transactionsNum <= sampleSize) transactionsRDD 
			else transactionsRDD.sample(
					false, 
					sampleSize.toDouble / transactionsNum, 
					sampleSize)
		frequentItemSets(sampledTransactions, minsuppRate)
	}
	/**
	 * Main function to get frequent itemsets by filtering invalid 
	 * transactions. For those transactions which have only one item, 
	 * they are invalid and should be filtered
	 */
	def frequentItemSetsByFilter(
			transactionsRDD: RDD[String], 
			minsuppRate: Double, 
			itemsOfEachTransaction: Int): ArrayList[(Array[String], Int)] = {
		val filteredTransactions = transactionsRDD.filter{t => 
			t.split("\\s").length < itemsOfEachTransaction}
		frequentItemSets(filteredTransactions, minsuppRate)
	}
	
	
	/**
	 * Run the algorithm to get frequent itemsets with support
	 */
	def frequentItemSets(
			transactionsRDD: RDD[String], 
			minsuppRate: Double):ArrayList[(Array[String], Int)] = {
		//[1] Scan the whole transactions to get each item count
		val itemCountPairs = this.getItemCountPairs(transactionsRDD)
		val mapSupport = this.transformMapSupport(itemCountPairs)
		//[2] Set required parameters
		this.setTransactionsNum(transactionsRDD)
		this.setMinSupport(minsuppRate)
		//test
		println("transaction number:" + this.transactionsNum)
		println("min support:" + this.relativeMinsupp)
		// Create the FPTree
		val tree = new FPTree
		//Using broadcast variable to create FPTree
		//Note: (1) create only one tree in global
		//(2) Use Java data structure in FPTree, in order to update the global
		//variable. if using Scala structure, the broadcast variable is immutable,
		// which can not be changed.
		val sc = transactionsRDD.context
		val sharedTree = sc.broadcast(tree)
		//[3] Scan each transaction to insert into the FPTree
		transactionsRDD.foreach{ line =>
			if (line != null && !line.isEmpty()) {
			// split the transaction into items
			val lineSplited = line.split("\\s")
			// create an array list to store the items
			val transaction = new ArrayList[String]
			// for each item in the transaction
			for(i <- 0 until lineSplited.size){  
				// if it is frequent, add it to the transaction
				// otherwise not because it cannot be part of a frequent itemset.
				val itemString = lineSplited(i)
				if(mapSupport.get(itemString) >= relativeMinsupp) 
					transaction.add(itemString)
			}
			// sort item in the transaction by descending order of support
			Collections.sort(transaction, new Comparator[String](){
				def compare(item1: String, item2: String): Int = {
					// compare the support
					val compare = mapSupport.get(item2) - mapSupport.get(item1);
					// if the same support, we check the lexical ordering!
					if(compare == 0) item1.compareTo(item2) else compare
				}
			})
			// add the sorted transaction to the FPTree.
			if (transaction.size() > 0) sharedTree.value.addTransaction(transaction)
			}
		}
		//[4] Create the header table for the tree
		tree.createHeaderList(mapSupport)
		//[5] Mine the FP-Tree by calling the recursive method.
		// Initially, the prefix alpha is empty.
		val patterns = new ArrayList[(Array[String], Int)]
		val prefixAlpha = new Array[String](0)
		if(tree.headerList.size() > 0) {
			fpgrowth(tree, prefixAlpha, transactionsNum, mapSupport, patterns)
		}
		patterns
	}
	
	
	/**
	 * Mine an FP-Tree having more than one path.
	 * @param tree  the FP-tree
	 * @param prefix  the current prefix, named "alpha"
	 * @param mapSupport the frequency of items in the FP-Tree
	 * Note: (1) Recursive Function must have one more results to return. But the 
	 * algorithm doesn't need the return.
	 * (2) @param patterns is return
	 */
	def fpgrowthMoreThanOnePath(
			tree: FPTree, 
			prefixAlpha: Array[String], 
			prefixSupport: Int, 
			mapSupport: Map[String, Int],
			patterns: ArrayList[(Array[String], Int)]): FPTree = {
		//Process each frequent item in the header table list in reverse order.
		for(i <- tree.headerList.size()-1 to 0 by -1){
			val item = tree.headerList.get(i)
			val support = mapSupport.get(item)
			// if the item is not frequent, we skip it
			if(support >=  relativeMinsupp){
				// Create Beta by concatening Alpha with the current item
				// and add it to the list of frequent patterns
				val beta = new Array[String](prefixAlpha.length+1)
				System.arraycopy(prefixAlpha, 0, beta, 0, prefixAlpha.length)
				beta(prefixAlpha.length) = item
			
				// calculate the support of beta
				val betaSupport = 
					if (prefixSupport < support)  prefixSupport 
					else support
				patterns.add((beta, betaSupport))
				// === Construct beta's conditional pattern base ===
				// It is a subdatabase which consists of the set of prefix paths
				// in the FP-tree co-occuring with the suffix pattern.
				val prefixPaths = new ArrayList[List[FPNode]]
				var path = tree.mapItemNodes.get(item)
				while(path != null){
					// if the path is not just the root node
					if(path.parent.itemID != null){
						// create the prefixpath
						val prefixPath = new ArrayList[FPNode]
						// add this node.
						prefixPath.add(path);   // NOTE: we add it just to keep its support,
						// actually it should not be part of the prefixPath
						//Recursively add all the parents of this node.
						var parent = path.parent;
						while(parent.itemID != null){
							prefixPath.add(parent);
							parent = parent.parent;
						}
						// add the path to the list of prefixpaths
						prefixPaths.add(prefixPath);
					}
					// We will look for the next prefixpath
					path = path.nodeLink;
				}
			
				// (A) Calculate the frequency of each item in the prefixpath
				val mapSupportBeta = 
					new HashMap[String, Int](2 * prefixPaths.size, 0.95f)
				// for each prefixpath
				for(i <- 0 until prefixPaths.size()){
					val prefixPath = prefixPaths.get(i)
					// the support of the prefixpath is the support of its first node.
					val pathCount = prefixPath.get(0).counter;  
					// for each node in the prefixpath,
					// except the first one, we count the frequency
					for(j <- 1 until prefixPath.size()){ 
						val node = prefixPath.get(j);
						// if the item doesn't first see, 
						// make the sum with the value already stored
						if(mapSupportBeta.containsKey(node.itemID)){
							mapSupportBeta.put(
									node.itemID, 
									mapSupportBeta.get(node.itemID) + pathCount)
						}else{
							// if the first time we see that node id, 
							// just add the path count
							mapSupportBeta.put(node.itemID, pathCount)
						}
					}
				}
			
				// (B) Construct beta's conditional FP-Tree
				val treeBeta = new FPTree
				// add each prefixpath in the FP-tree
				for(k <- 0 until prefixPaths.size()){
					val prefixPath = prefixPaths.get(k)
					treeBeta.addPrefixPath(
							prefixPath, 
							mapSupportBeta, 
							relativeMinsupp)
				}  
				// Create the header list.
				treeBeta.createHeaderList(mapSupportBeta);
				// Mine recursively the Beta tree if the root as child(s)
				if(treeBeta.root.childs.size() > 0){
					// recursive call
					fpgrowth(treeBeta, beta, betaSupport, mapSupportBeta, patterns)
				}
			}
			
		}
		tree
	}
	
	
	/**
	 * This method mines pattern from a Prefix-Tree recursively
	 * @param tree  The Prefix Tree
	 * @param prefix  The current prefix "alpha"
	 * @param mapSupport The frequency of each item in the prefix tree.
	 */
	def fpgrowth(
			tree: FPTree, 
			prefixAlpha: Array[String], 
			prefixSupport: Int, 
			mapSupport: Map[String, Int],
			patterns: ArrayList[(Array[String], Int)]) : FPTree= {
		// We need to check if there is a single path in the prefix tree or not.
		if(tree.hasMoreThanOnePath == false){
			// That means that there is a single path, so we 
			// add all combinations of this path, concatenated with the prefix 
			// "alpha", to the set of patterns found.
			addAllCombinationsForPathAndPrefix(
					tree.root.childs.get(0), 
					prefixAlpha, 
					patterns)
		}else{ // There is more than one path
			fpgrowthMoreThanOnePath(
					tree, 
					prefixAlpha, 
					prefixSupport, 
					mapSupport, 
					patterns)
		}
		tree
	}
	
	/**
	 * This method is for adding recursively all combinations of nodes in 
	 * a path, concatenated with a given prefix,
	 * to the set of patterns found.
	 * @param nodeLink the first node of the path
	 * @param prefix  the prefix
	 * @param minsupportForNode the support of this path.
	 */
	def addAllCombinationsForPathAndPrefix(
			node: FPNode,
			prefix: Array[String],
			patterns: ArrayList[(Array[String], Int)]): FPNode = {
		// Concatenate the node item to the current prefix
		val itemset = new Array[String](prefix.length+1)
		System.arraycopy(prefix, 0, itemset, 0, prefix.length);
		itemset(prefix.length) = node.itemID;
		patterns.add((itemset, node.counter))
		//showPatterns(itemset, node.counter)
		if(node.childs.size() != 0) {
			addAllCombinationsForPathAndPrefix(node.childs.get(0), itemset, patterns)
			addAllCombinationsForPathAndPrefix(node.childs.get(0), prefix, patterns)
		}
		node
	}
	
	def showPatterns(items: Array[String], freq: Int) = {
		println("-------------")
		items.foreach(println)
		println("freq:" + freq)
		println("=============")
	}
	/**
	 * Calculate association rules based on given patterns and conf, lift
	 */
	def associationRules(
			patterns: ArrayList[(Array[String], Int)],
			transactionsNumber: Int,
			minConf: Double,
			minLift: Double): Array[(String, String, Double, Double, Double)] = {
		val patternsNumber = patterns.size()
		// map random two items(or single one) to their count
		// "item1+item2" -> support
		val subsetSuppMap = new scala.collection.mutable.HashMap[String, Double]
		val subsetsWithTwoItems = new scala.collection.mutable.HashSet[String]
		for (i <- 0 until patternsNumber) {
			val pattern = patterns.get(i)
			val items = pattern._1
			val support = pattern._2.toDouble / transactionsNumber.toDouble
			val subsets = subsetsOfItems(items)
			subsets.foreach { s =>
				val subsetSize = if (items.size > 1) 2 else 1
				subsetSuppMap += (s -> support)
				if (subsetSize > 1) subsetsWithTwoItems += s
			}
		}
		// item_master, item_dep, support, conf, lift
		val ret = new ArrayBuffer[(String, String, Double, Double, Double)]
		subsetsWithTwoItems.foreach { subset =>
			val mark = subset.indexOf("+++")
			if (mark > 0) {
				val x = subset.substring(0, mark)
				val y = subset.substring(mark + 3)
				if (subsetSuppMap.contains(x) && 
						subsetSuppMap.contains(y) &&
						subsetSuppMap.contains(subset)) {
					val supp = subsetSuppMap.apply(subset)
					val xSupp = subsetSuppMap.apply(x)
					val ySupp = subsetSuppMap.apply(y)
					
					val confX2Y = supp / xSupp
					val confY2X = supp / ySupp
					
					val lift = supp / (xSupp * ySupp)
					//test
					//println(x + "=>" + y + "\tsupp:" + supp + "\tconf:" + confX2Y + "\tlift:" + lift )
					//println(y + "=>" + x + "\tsupp:" + supp + "\tconf:" + confY2X + "\tlift:" + lift )
					if (confX2Y > minConf && lift > minLift) 
						ret.append((x, y, supp, confX2Y, lift))
					if (confY2X > minConf && lift > minLift) 
						ret.append((y, x, supp, confY2X, lift))
				}
			}
		}
		ret.toArray
	}
	/**
	 * get subsets(size of two) of given items
	 * eg: {a b c d e} => {ab ac ad ae bc bd be cd ce de}
	 */
	def subsetsOfItems(items: Array[String]): Array[String] = {
		val size = items.size
		if (size < 2) items
		else {
			val ret = new ArrayBuffer[String]
			for (i <- 0 to size -2) {
				for (j <- i+1 to size -1) {
					ret.append(items(i) + "+++"+  items(j))
				}
			}
			ret.toArray
		}
	}
	
	

}

object FPGrowth {
	def main(args: Array[String]) = {
		val server = "local[2]"
    val jobName = "association rules mining"
    val sparkJAR = "./lib/spark-assembly-1.0.0-hadoop1.0.4.jar"	
    System.setProperty("spark.executor.memory", "1500m")
    System.setProperty("spark.storage.memoryFraction", "0.4")
    val sc = new SparkContext(server, jobName, sparkJAR)

		val filePath = "path-to-transaction"
		val rawFile = sc.textFile(filePath)
		val fpgrowth = new FPGrowth
		val transactions = rawFile.map(line => transform(line)).filter(t => t != null).filter(t => t.split("\\s").size > 1)
		val patterns = fpgrowth.frequentItemSets(transactions, 0.0001)
		
    println("patterns number:" + patterns.size())

    // get association rules
		val rules = fpgrowth.associationRules(patterns, rawFile.count.toInt, 0.01, 1.0)
		println("association rules number:" + rules.size)
		/*
		rules.foreach{ rule =>
			println("%s=>%s\tsupp:%f\tconf:%f\tlift:%f".format(rule._1, rule._2, rule._3, rule._4, rule._5))
		}
		*/
		// 
		//val rulesRDD = sc.makeRDD(rules)
		//val output = "/home/tr/Desktop/rules.txt"
		//rulesRDD.saveAsTextFile(output)
		val sortedRules = sortRules(rules)
		val ret = sc.makeRDD(sortedRules)
		ret.saveAsTextFile("path-to-output-the-results")
	}
	def transform(raw: String): String = {
		val index = raw.indexOf(",") + 3
		if (index >= raw.length()) null
		else {
			val t = raw.substring(index, raw.length - 1)
			t.replaceAll(", ", " ")
		}
	}
	
	def sortRules(rules: Array[(String, String, Double, Double, Double)]): Array[String] = {
		val masterItems = new scala.collection.mutable.HashMap[String, ArrayList[(String, String, Double, Double, Double)]]
		rules.foreach { rule =>
			if (masterItems.contains(rule._1)) {
				masterItems.apply(rule._1).add(rule)
			} else {
				val temp = new ArrayList[(String, String, Double, Double, Double)]
				temp.add(rule)
				masterItems += ((rule._1, temp))
			}
		}
		// sort dependency for each master item, based on confidence and lift
		val masterItemsCollection = masterItems.toArray
		masterItemsCollection.foreach { master =>
			Collections.sort(master._2, new Comparator[(String, String, Double, Double, Double)]() {
				def compare(
					d1: (String, String, Double, Double, Double), 
					d2: (String, String, Double, Double, Double)): Int = {
					val comp = ((d2._4 - d1._4) * 10000000).toInt
							if (comp == 0) (d1._5 * 10000000).toInt.compare((d2._5 * 10000000).toInt)
							else comp
				}
			})
		}
		val ret = new Array[String](masterItemsCollection.size)
		for (j <- 0 until masterItemsCollection.size) {
			val item = masterItemsCollection.apply(j)
			val m = item._1
			val d = item._2
			val buffer = new StringBuilder
			buffer.append(m + "\t=>\t[")
			for (i <- 0 until d.size()) {
				buffer.append(d.get(i).toString + "\t")
			}
			buffer.append("]")
			ret(j) = buffer.toString
		}
		ret

	}
	
	
	def rulesToString(rules: Array[(String, String, Double, Double, Double)]): String = {
		val buffer = new StringBuilder
		rules.foreach { rule =>
			buffer.append("[%s] => [%s]\tsupp:%f\tconf:%f\tlift:%f".format(rule._1, rule._2, rule._3, rule._4, rule._5))
		}
		buffer.toString
	}
}
