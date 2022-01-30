package org.data.algorithms.spark.bonus_chapter

import org.apache.spark.sql.SparkSession

object TFIDF {

  def keepDocNameAndClean(tuple2: (String,String)): (String, String) = {
    val fullPath = tuple2._1
    val tokens = fullPath.split("/")
    // get the last element of a list of doc path tokens
    val shortDocNames = tokens.last
    /*
    * short_doc_name : "doc1"
    *
    * replace \n by " "
    */
    val document = tuple2._2
    val documentRevised = document.replace('\n', ' ').trim
    return (shortDocNames,documentRevised)
  }

  def createKeyValuePair (doc : (String, String), wordLengthThreshold: Int): Array[((String, String), Int)] = {
    val docId = doc._1
    val documentContent = doc._2
    val tokens = documentContent.split("\\s+")
    val fileteredTokens = tokens.filter(_.length > wordLengthThreshold)
    return {
      for(w<-fileteredTokens)
        yield ((docId,w),1)
    }

  }

  def main(args: Array[String]): Unit = {
    //create an instance of SparkSession
    val spark = SparkSession
      .builder()
      .appName("TF-IDF")
      .master("local[*]")
      .getOrCreate()
    // define input path
    val inputPath = args(0)
    println(s"input_path= ${inputPath} ")
    /*
    * define word length threshold
    * drop a word if its length is less than or equal to word_length_threshold
    * drop words like "a", "of", ...
    */
    val wordLengthThreshold = args(1).toInt
    println(s"wordLengthThreshold= ${wordLengthThreshold}")
    /*
    *----------------------
    * Step-1: prepare input
    *----------------------
    * read docs and create docs as
    * docs : RDD[(key, value)]
    * where
    *    key: document_path
    *    value: content of document
    */
    val docs = spark.sparkContext.wholeTextFiles(inputPath)
    val numOfDocs = docs.count()
    println(s"doc= ${docs.collect().mkString("Array(", ", ", ")")}")
    // keep only the name of the file (drop full path)
    val lines = docs.map(keepDocNameAndClean)
    println(s"lines= ${lines.collect().mkString("Array(", ", ", ")")}")
    /*
    *---------------------
    * Step-2: calculate TF
    *---------------------
    * Find a frequency of a word within a document:
    * So we need to find: ((docID, word), frequency)
    * where key is (docID, word) and value is frequency.
    * Get the term frequency for a particular word
    * corresponding to its docID.
    * mapped = [((docID, word), 1)]
    */
    val mapped = lines.flatMap(createKeyValuePair(_,wordLengthThreshold))
    val reduced = mapped.reduceByKey((x,y)=>x+y)
    println(s"reduced= ${reduced.collect().mkString("Array(", ", ", ")")}")
    // tf = [(word, (docID, freq))]
    val tf = reduced.map(x => (x._1._2, (x._1._1, x._2)))
    println(s"tf= ${tf.collect().mkString("Array(", ", ", ")")}")

    // find frequency of unique words in all of the documents
    val words = reduced.map( x=>(x._1._2, 1))
    // words = [(word, 1)]
    println(s"words= ${words.collect().mkString("Array(", ", ", ")")}")
    val frequencies = words.reduceByKey( (x,y)=> x+y)
    println(s"frequencies= ${frequencies.collect().mkString("Array(", ", ", ")")}")
    // frequencies = [(word, freq)]
    
    /*
    *----------------------
    * Step-3: calculate IDF
    *----------------------
    */
    val idf = frequencies.map( x => (x._1, math.log10(numOfDocs/x._2)))
    println(s"idf= ${idf.collect().mkString("Array(", ", ", ")")}")
    /*
    *---------------------------
    * Step-4: prepare for TF-IDF
    *---------------------------
    */
    val tfIDF = tf.join(idf).
      map( x => (x._2._1._1,(x._1, x._2._1._2, x._2._2, x._2._1._2*x._2._2))).
      sortByKey()
    println(s"tfIDF= ${tfIDF.collect().mkString("Array(", ", ", ")")}")
    /*
    *-------------------------------------------------------
    * Step-5: prepare final output of TF-IDF  as a DataFrame
    *-------------------------------------------------------
    */
    val tfIdfMapped = tfIDF.map( x => (x._1, x._2._1, x._2._2, x._2._3, x._2._4))
    import spark.implicits._
    val df = tfIdfMapped.toDF(Array("DocID", "Token", "TF", "IDF", "TF-IDF"):_*)
    df.show(100, truncate=false)
  }

}
