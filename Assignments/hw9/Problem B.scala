val acc = "bdad17/loudacre/accounts"
val mydata = sc.textFile(acc)
val postal_user = mydata.keyBy(line=>line.split(',')(8))
val postal_usrname = postal_user.mapValues(line=>(line.split(',')(3),line.split(',')(4)))
val results = postal_usrname.groupByKey().sortByKey()
results.take(5)