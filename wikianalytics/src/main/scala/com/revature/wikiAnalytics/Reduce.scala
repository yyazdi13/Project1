package com.revature.wikiAnalytics

import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

class Reduce extends Reducer[Text, Text, Text, IntWritable]{
  override def reduce(
      key: Text,
      values: java.lang.Iterable[Text],
      context: Reducer[Text, Text, Text, IntWritable]#Context
  ): Unit = {
      // val alphabet = Array("a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z")
      val prev = key.toString()
      val iter = values.iterator()
      var max = 0;
      var prevCurrPair = prev
      if(prev.equalsIgnoreCase("Hotel_California")){
        while(iter.hasNext()) {
          val nextValue = iter.next().toString
          val curr = nextValue.split("\\t")(0)
          val n = nextValue.split("\\t")(1).toInt
          max += n
          prevCurrPair = s"$prev\t$curr"
        }
      }
      
      // var thirdLetter = "a"
      // var fourthLetter = "a"
      // var secondLetter = "a"
      // val firstLetter = prev.toLowerCase.substring(0,1)
      // if (prev.length > 1){
      //  secondLetter = prev.toLowerCase.substring(1,2)
      // }
      // if (prev.length > 2){
      //   thirdLetter = prev.toLowerCase.substring(2,3)
      // } 
      // if (prev.length > 3){
      //     fourthLetter = prev.toLowerCase.substring(3,4)
      // }
      // if (alphabet.indexOf(firstLetter) >= 7 && alphabet.indexOf(secondLetter) >= 14 && alphabet.indexOf(thirdLetter) >= 19 && alphabet.indexOf(fourthLetter) >= 4){
      //   context.write(new Text(prevCurrPair), new IntWritable(max))
      // }
    context.write(new Text(prevCurrPair), new IntWritable(max))

  }
}
