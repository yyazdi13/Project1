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
      val prev = key.toString()
      val iter = values.iterator()
      var max = 0;
      var prevCurrPair = prev
        while(iter.hasNext()) {
          val nextValue = iter.next().toString
          val curr = nextValue.split("\\t")(0)
          val n = nextValue.split("\\t")(1).toInt
          max += n
          prevCurrPair = s"$prev\t$curr"
          context.write(new Text(prevCurrPair), new IntWritable(n))
        }

  }
}
