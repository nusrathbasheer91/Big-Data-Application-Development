val acc = "bdad17/loudacre/accounts"
val mydata = sc.textFile(acc)
val userData = mydata.keyBy(line=>line.split(',')(0))
val log = "bdad17/loudacre/weblog/2014-03-15.log"
val logdata = sc.textFile(log)
val log_data = logdata.map(line=>line.split(' ')(2)).map((_,1))
val log_agg = log_data.reduceByKey(_+_)
val user_log = userData.join(log_agg)
val results = user_log.map(k=> (k._1+" "+k._2._2+" "+k._2._1.split(',')(3)+" "+k._2._1.split(',')(4)))
results.take(5)
