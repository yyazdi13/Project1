package com.revature.wikiAnalytics

import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text

class Map extends Mapper[LongWritable, Text, Text, Text] {
  override def map(key: LongWritable, 
  value: Text, 
  context: Mapper[LongWritable, Text, Text, Text]#Context): Unit = {
      val record = value.toString().split("\\t")
      if (record(2) == "link") {
      context.write(new Text(record(0)), new Text(s"${record(1)}\t${record(3)}"))
      }
    }
}
