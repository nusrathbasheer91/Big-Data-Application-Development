val in = "bdad17/project/raw_data/LA_2010.csv"
val out = "bdad17/project/hw9out"

val crimecsv = sc.textFile(in)
val crimemap = crimecsv.map(line=> line.split(","))

val crimesub = crimemap.map(line=>(line(2),line(8)))

val crimes = crimesub.filter(line => (line._1!="Date Occurred") && (line._1!=""))
val crimed = crimes.map(line=> (line._1.split("/")(0)+"/"+line._1.split("/")(2).substring(2,4) , line._2))
val reducedcrime = crimed.countByValue()
val output_file = reducedcrime.map{case ((k,v),cnt)=>"01"+"\t"+k.split("/")(0)+"\t20"+k.split("/")(1)+"\t"+v+"\t"+cnt}
val op = sc.parallelize(output_file.toList)
op.saveAsTextFile(out+"/monthlycrimevs2")