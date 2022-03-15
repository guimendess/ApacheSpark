-- Databricks notebook source
-- MAGIC 
-- MAGIC %python
-- MAGIC def createDummyData(tableName, databaseName=None, seed=None, rows=10000, name="Name", id="ID", password="Password", amount="Amount", percent="Percent", probability="Probability", yesNo="YesNo", UTCTime="UTCTime"): 
-- MAGIC   
-- MAGIC   # usage:
-- MAGIC   #  use "EmployeeName" in place of default "Name" column
-- MAGIC   #  df = createDummyData(tableName="whatever", rows=100, name="EmployeeName")
-- MAGIC   #
-- MAGIC   # returns: DataFrame
-- MAGIC   
-- MAGIC   import sys, string, decimal, time, random, os
-- MAGIC   from builtins import round
-- MAGIC   from pyspark.sql import Row
-- MAGIC   from pyspark.sql.functions import when, length, col
-- MAGIC 
-- MAGIC   # Pull from environment, don't assume global variables.
-- MAGIC   username = spark.conf.get("com.databricks.training.username")
-- MAGIC   userhome = spark.conf.get("com.databricks.training.userhome")
-- MAGIC   
-- MAGIC   dbName = databaseName
-- MAGIC   if dbName is None:
-- MAGIC     dbName = username
-- MAGIC     for char in ["-", "=", "[", "]", "\\", ";", "\'", ",", ".", "/", "!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "+", "{", "}", "|", ":", "\"", "<", ">", "?"]:
-- MAGIC       dbName = dbName.replace(char, "_")
-- MAGIC     
-- MAGIC   spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(dbName))
-- MAGIC                    
-- MAGIC   ipsum = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud   exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
-- MAGIC            
-- MAGIC   ipsum = ipsum.replace(".", "").replace(",", "").lower().split(" ")
-- MAGIC 
-- MAGIC   # sadly, we can't use the hash function from builtins
-- MAGIC   # https://stackoverflow.com/questions/27522626/hash-function-in-python-3-3-returns-different-results-between-sessions
-- MAGIC   
-- MAGIC   
-- MAGIC   bigList = []
-- MAGIC 
-- MAGIC          
-- MAGIC   def setSeed(i, seed):
-- MAGIC     import hashlib
-- MAGIC     
-- MAGIC     if seed is None:
-- MAGIC       hashObject = hashlib.sha1(bytes(userhome, encoding="utf-8"))
-- MAGIC       hashSeed = int(hashObject.hexdigest(), 16)
-- MAGIC       
-- MAGIC     else:       
-- MAGIC       hashObject = hashlib.sha1(bytes(seed, encoding="utf-8"))
-- MAGIC       hashSeed = int(hashObject.hexdigest(), 16)
-- MAGIC       
-- MAGIC     random.seed(hashSeed + i)
-- MAGIC       
-- MAGIC     return None 
-- MAGIC   
-- MAGIC   
-- MAGIC   for i in range(0, rows):
-- MAGIC     
-- MAGIC         
-- MAGIC     setSeed(i, seed)
-- MAGIC     
-- MAGIC     def firstName(): 
-- MAGIC       return str(ipsum[random.randint(1, len(ipsum)-1)]).capitalize() 
-- MAGIC     
-- MAGIC     def lastName():
-- MAGIC       return str(ipsum[random.randint(1, len(ipsum)-1)]).capitalize()
-- MAGIC     
-- MAGIC     def randomName():
-- MAGIC       return firstName() + " " + lastName()
-- MAGIC       
-- MAGIC     # return str(ipsum[rand.randint(1, len(ipsum)-1)]).capitalize() + " " + str(ipsum[rand.randint(1, len(ipsum)-1)]).capitalize()
-- MAGIC 
-- MAGIC     # various sizes of random integers
-- MAGIC     def randomInt1to100(): 
-- MAGIC       return random.randint(1,100)
-- MAGIC 
-- MAGIC     def randomInt1to1000(): 
-- MAGIC       return random.randint(1,1000)
-- MAGIC 
-- MAGIC     def randomInt1toMax(): 
-- MAGIC       return random.randint(1, sys.maxsize)
-- MAGIC 
-- MAGIC     def randomDouble():
-- MAGIC       return random.random()
-- MAGIC 
-- MAGIC     def randomBoolean():
-- MAGIC       return random.choice([True, False])
-- MAGIC 
-- MAGIC     # random password  
-- MAGIC     def randomString():
-- MAGIC       chars = string.ascii_letters + string.digits
-- MAGIC       return ''.join(random.choice(chars) for i in range(8,20))
-- MAGIC 
-- MAGIC     # random money amount
-- MAGIC     def randomAmount(): 
-- MAGIC       return float(decimal.Decimal(random.randrange(0, 1000000))/100)
-- MAGIC 
-- MAGIC     # random timestamp in UTC format from Jan 1, 2000 to now
-- MAGIC     def randomUTCTimestamp():
-- MAGIC       return random.randint(946684800, round(time.time()))  
-- MAGIC 
-- MAGIC     # def randomList():
-- MAGIC     # return (list(Row(i, randomName(), randomInt1toMax(), randomString(), randomAmount(), randomInt1to100(), randomDouble(), randomBoolean(), randomUTCTimestamp())) for j in range(rows))
-- MAGIC     bigList.append(list(Row(i, randomName(), randomInt1toMax(), randomString(), randomAmount(), randomInt1to100(), randomDouble(), randomBoolean(), randomUTCTimestamp())))
-- MAGIC     
-- MAGIC      # return bigList.append(list(Row(i, randomName(), randomInt1toMax(), randomString(), randomAmount(), randomInt1to100(), randomDouble(), randomBoolean(), randomUTCTimestamp())))
-- MAGIC       #re
-- MAGIC 
-- MAGIC     randomSchema = "{} integer, {} string, {} long, {} string, {} double, {} integer, {} float, {} boolean, {} integer".format("index", name, id, password, amount, percent, probability, yesNo, UTCTime)
-- MAGIC 
-- MAGIC   tempDF = spark.createDataFrame(bigList, randomSchema)
-- MAGIC   # tempDF = spark.createDataFrame(randomList(), randomSchema)
-- MAGIC 
-- MAGIC   df = tempDF.withColumn(percent, when(col(percent) % 7 == 0, None).otherwise(col(percent)))
-- MAGIC 
-- MAGIC   # Create the actual table
-- MAGIC   fullTableName = dbName + "." + tableName
-- MAGIC   df.write.mode("overwrite").saveAsTable(fullTableName)  
-- MAGIC 
-- MAGIC   # If the table existed or not, refresh it and re-initialize our dataframe.
-- MAGIC   spark.sql("REFRESH TABLE {}".format(fullTableName))
-- MAGIC 
-- MAGIC   return spark.read.table(fullTableName).orderBy("index")
-- MAGIC   
-- MAGIC None # Suppress output