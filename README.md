# Project1
Analysis of Wikipedia data sets using big data tools like Hadoop and HiveQL

## Description
Using big data tools to answer questions about datasets from Wikipedia. There are a series of basic analysis questions.

## Overview
* Which English wikipedia article got the most traffic on January 20, 2021?
* What English wikipedia article has the largest fraction of its readers follow an internal link to another wikipedia    article?
* What series of wikipedia articles, starting with Hotel California, keeps the largest fraction of its readers clicking on internal links? 
* Find an example of an English wikipedia article that is relatively more popular in the Americas than elsewhere.
* Analyze how many users will see the average vandalized wikipedia page before the offending edit is reversed.
* Run an analysis you find interesting on the wikipedia datasets

## Tools
* HiveQL
* MapReduce
* Hadoop
* Scala
* SBT
* Maven
* YARN
* Scala metals

## Usage
The answers to the overview questions can be found in src/main/scala.../seed.hql. To run a MapReduce job,
Set up a local Hadoop environment and SBT assembly, then hadoop jar target/scala-2.13/wikiAnalytics.jar