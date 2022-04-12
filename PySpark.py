#ติดตั้ง Spark และ PySpark (Google Colab)
!apt-get update                                                                          # อัพเดท Package ทั้งหมดใน VM ตัวนี้
!apt-get install openjdk-8-jdk-headless -qq > /dev/null                                  # ติดตั้ง Java Development Kit (จำเป็นสำหรับการติดตั้ง Spark)
!wget -q https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop2.7.tgz # ติดตั้ง Spark 3.1.2
!tar xzvf spark-3.1.2-bin-hadoop2.7.tgz                                                  # Unzip ไฟล์ Spark 3.1.2
!pip install -q findspark==1.3.0                                                         # ติดตั้ง Package Python สำหรับเชื่อมต่อกับ Spark 
# Set enviroment variable ให้ Python รู้จัก Spark
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.2-bin-hadoop2.7"
# ติดตั้ง PySpark ลงใน Python
!pip install pyspark==3.1.2
# Server ของ Google Colab มีกี่ Core
!cat /proc/cpuinfo

# สร้าง Spark Session เพิ้อใช้งาน Spark
# ใช้ local[*] เพื่อเปิดการใช้งานการประมวลผลแบบ multi-core (Spark จะใช้ CPU ทุก core ที่อนุญาตให้ใช้งานในเครื่อง)
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Load data ใส่ spark
dt = spark.read.csv('/content/ws2_data.csv', header = True, inferSchema = True, )

# ดูข้อมูล
dt.show()

# ดูข้อมูล 100 แถวแรก
dt.show(100)

# ดูประเภทข้อมูลแต่ละคอลัมน์
dt.dtypes

# อีกคำสั่งในการดูข้อมูลแต่ละคอลัมน์ (Schema)
dt.printSchema()

# นับจำนวนแถวและ column
print((dt.count(), len(dt.columns)))

# สรุปข้อมูลสถิติ
dt.describe().show()

# อีกคำสั่งในการสรุปข้อมูลสถิติ
# ReferenceL: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.summary.html
dt.summary().show()

# สรุปข้อมูลสถิติเฉพาะ column ที่ระบุ
dt.select("price").describe().show()

# Check missing value and show missing value row
dt.summary("count").show()
dt.where(dt.user_id.isNull()).show()

#คำสั่ง Spark ในการค้นหาข้อมูลที่ต้องการ
# ข้อมูลที่เป็นตัวเลข
dt.where(dt.price >= 1).show()
# ข้อมูลที่เป็นตัวหนังสือ
dt.where(dt.country == 'Canada').show()
# การซื้อทั้งหมดที่เกิดขึ้นในเดือนเมษายน และเดือนสิงหาคม มีกี่แถว
dt.where(dt.timestamp.startswith("2021-04")).count()
dt.where(dt.timestamp.startswith("2021-08")).count()

# ใช้ package seaborn matplotlib และ pandas ในการ plot ข้อมูล
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
# แปลง Spark Dataframe เป็น Pandas Dataframe - ใช้เวลาประมาณ 6 วินาที
dt_pd = dt.toPandas()
# ดูตัวอย่างข้อมูล
dt_pd.head()
# Boxplot - แสดงการกระจายตัวของข้อมูลตัวเลข
sns.boxplot(x = dt_pd['book_id'])
# Histogram - แสดงการกระจายตัวของข้อมูลตัวเลข
# bins = จำนวน bar ที่ต้องการแสดง
sns.histplot(dt_pd['price'], bins=10)
# สร้าง Plot เพื่อดูความสัมพันธ์ระหว่าง book_id กับ price
sns.scatterplot(x = dt_pd.book_id, y = dt_pd.price)
# สร้าง interactive chart
import plotly.express as px
fig = px.scatter(dt_pd, 'book_id', 'price')
fig.show()

# Data Cleansing with Spark
# แปลง string เป็น datetime
from pyspark.sql import functions as f

dt_clean = dt.withColumn("timestamp",
                        f.to_timestamp(dt.timestamp, 'yyyy-MM-dd HH:mm:ss')
                        )
dt_clean.show()
# นับยอด transaction ช่วงครึ่งเดือนแรก ของเดือนมิถุนายน
dt_clean.where( (f.dayofmonth(dt_clean.timestamp) <= 15) & ( f.month(dt_clean.timestamp) == 6 ) ).count()

# Syntactical Anomalies
# ใน Data set ชุดนี้ มีข้อมูลจากกี่ประเทศ
dt_clean.select("Country").distinct().count()
# แทนที่ ... ด้วยจำนวนประเทศ เพื่อดูรายชื่อประเทศทั้งหมด
# sort = ทำให้ข้อมูลเรียงตามตัวอักษร อ่านง่ายขึ้น
# show() ถ้าไม่ใส่ตัวเลขจะขึ้นมาแค่ 20 อัน และใส่ False เพื่อให้แสดงข้อมูลในคอลัมน์แบบเต็ม ๆ (หากไม่ใส่ คอลัมน์ที่ยาวจะถูกตัดตัวหนังสือ)
dt_clean.select("Country").distinct().sort("Country").show( 58, False )
# เปลี่ยน ... เป็นชื่อประเทศที่คุณคิดว่าผิด
dt_clean.where(dt_clean['Country'] == 'Japane').show()
# เปลี่ยน ... เป็นชื่อประเทศที่คุณคิดว่าผิด และ ...2 เป็นชื่อประเทศที่ถูกต้อง
from pyspark.sql.functions import when
dt_clean_country = dt_clean.withColumn("CountryUpdate", when(dt_clean['Country'] == 'Japane', 'Japan').otherwise(dt_clean['Country']))
# ตรวจสอบข้อมูลที่แก้ไขแล้ว
dt_clean_country.select("CountryUpdate").distinct().sort("CountryUpdate").show(58, False)
# เอาคอลัมน์ CountryUpdate ไปแทนที่คอลัมน์ Country
dt_clean = dt_clean_country.drop("Country").withColumnRenamed('CountryUpdate', 'Country')

# Semantic Anomalies
# ดูว่าข้อมูล user_id ตอนนี้หน้าตาเป็นอย่างไร
dt_clean.select("user_id").show(10)
# นับจำนวน user_id ทั้งหมด
dt_clean.select("user_id").count()
# ใช้เว็บไซต์ https://www.regex101.com เพื่อสร้าง Regular Expression ตามรูปแบบที่เราต้องการ
# แทนที่ ... ด้วย Regular Expression ของรูปแบบ user_id ที่เราต้องการ
# คำใบ้: ใน Regular Expression ที่เราต้องการ มี ^ นำหน้า และลงท้ายด้วย $
dt_clean.where(dt_clean["user_id"].rlike("^[a-z0-9]{8}$")).count()
# แทนที่ ... ด้วย Regular Expression ของรูปแบบ user_id ที่เราต้องการ
dt_correct_userid = dt_clean.filter(dt_clean["user_id"].rlike("^[a-z0-9]{8}$"))
dt_incorrect_userid = dt_clean.subtract(dt_correct_userid)
dt_incorrect_userid.show(10)
# แทนค่าที่ผิด ด้วยค่าที่ถูกต้อง
dt_clean_userid = dt_clean.withColumn("userid_update", when(dt_clean['user_id'] == 'ca86d17200', 'ca86d172').otherwise(dt_clean['user_id']))
# ตรวจสอบผลลัพธ์
dt_correct_userid = dt_clean_userid.filter(dt_clean_userid["user_id"].rlike("^[a-z0-9]{8}$"))
dt_incorrect_userid = dt_clean_userid.subtract(dt_correct_userid)
dt_incorrect_userid.show(10)
# เอาคอลัมน์ user_id_update ไปแทนที่ user_id 
dt_clean = dt_clean_userid.drop("user_id").withColumnRenamed('userid_update', 'user_id')

# Missing values
# วิธีที่ 1 ในการเช็ค Missing Value
# ใช้เทคนิค List Comparehension - ทบทวนได้ใน Pre-course Python https://school.datath.com/courses/road-to-data-engineer-2/contents/6129b780564a8
# เช่น [ print(i) for i in [1,2,3] ]
# col = คำสั่ง Spark ในการเลือกคอลัมน์
# sum = คำสั่ง Spark ในการคิดผลรวม
from pyspark.sql.functions import col, sum
dt_nulllist = dt_clean.select([ sum(col(colname).isNull().cast("int")).alias(colname) for colname in dt_clean.columns ])
dt_nulllist.show()
# วิธีที่ 2 ในการเช็ค Missing Value
dt_clean.summary("count").show()
# ดูช้อมูลว่าแถวไหนมี user_id เป็นค่าว่างเปล่า (โค้ดเดียวกับ Exercise 1)
dt_clean.where( dt_clean.user_id.isNull() ).show()
# แทน user_id ที่เป็น NULL ด้วย 00000000 
dt_clean_user_id = dt_clean.withColumn("userid_update", when(dt_clean['user_id'].isNull(), '00000000').otherwise(dt_clean['user_id']))
dt_clean = dt_clean_user_id.drop("user_id").withColumnRenamed('userid_update', 'user_id')
# เช็คว่า user ID ที่เป็น NULL หายไปแล้วจริงมั้ย
dt_clean.where( dt_clean.user_id.isNull() ).show()

# Outliers
# ใช้ Boxplot ในการหาค่า Outlier ของราคาหนังสือ
dt_clean_pd = dt_clean.toPandas()
sns.boxplot(x = dt_clean_pd['price'])
# Check book_id ที่ราคามากกว่า 80
dt_clean.where( dt_clean.price > 80 ).select("book_id").distinct().show()

# Clean ข้อมูลด้วย Spark SQL
# แปลงข้อมูลจาก Spark DataFrame ให้เป็น TempView ก่อน
dt.createOrReplaceTempView("data")
dt_sql = spark.sql("SELECT * FROM data")
dt_sql.show()
# แปลงโค้ดสำหรับลิสต์ชื่อประเทศ(SQL)
dt_sql_country = spark.sql("""
SELECT distinct country
FROM data
ORDER BY country
""")
dt_sql_country.show(100)
# แปลงโค้ดสำหรับแทนที่ชื่อประเทศ(SQL)
dt_sql_result = spark.sql("""
SELECT timestamp, user_id, book_id,
  CASE WHEN country = 'Japane' THEN 'Japan' ELSE country END AS country,
price
FROM data
""")
dt_sql_result.show()
# เช็คผลลัพธ์ว่าถูกจริงมั้ย
dt_sql_result.select("country").distinct().sort("country").show(58, False)
# ใช้คำสั่ง RLIKE ใน SQL เพื่อตรวจเช็ครูปแบบ Regular Expression 
# Answer here: เช็คว่ามีข้อมูล user_id ที่ไม่เป็นตัวหนังสือหรือตัวเลข 8 หลักมั้ย
dt_sql_user_id = spark.sql("""
SELECT *
FROM data
WHERE user_id NOT RLIKE '^[a-z0-9]{8}$'
""")
dt_sql_user_id.show(100)
# Answer here: แทนค่า (คำใบ้: ใช้ CASE WHEN)
dt_sql_uid_result = spark.sql("""
SELECT timestamp,
CASE WHEN user_id = 'ca86d17200' THEN 'ca86d172' ELSE user_id END AS user_id, 
book_id, country, price
FROM data
""")
dt_sql_uid_result.show()
# เช็คว่าข้อมูลที่ผิด หายไปหรือยัง
dt_sql_uid_result.where( dt_sql_uid_result.user_id == 'ca86d17200' ).show()
# Save data เป็น CSV
# เซฟเป็น partitioned files (ใช้ multiple workers)
dt_clean.write.csv('Cleaned_data.csv', header = True)
# เซฟเป็น 1 ไฟล์ (ใช้ single worker)
dt_clean.coalesce(1).write.csv('Cleaned_Data_Single.csv', header = True)
# วิธีอ่านไฟล์ที่มีหลาย Part
all_parts = spark.read.csv('/content/Cleaned_data.csv/part-*.csv', header = True, inferSchema = True)