import java.sql.{Connection, DriverManager, Statement}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Main extends App {
  val conf = new Configuration()

  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)

  val connection: Connection = DriverManager.getConnection("jdbc:hive2://quickstart.cloudera:10000/fall2019_pranav;user=pranav;password=pranav.")
  val stmt: Statement = connection.createStatement()

  conf.addResource(new Path("/home/pranav/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/home/pranav/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
  val hadoop: FileSystem = FileSystem.get(conf)

  val stagingpath = new Path("/user/fall2019/pranav/project4")
  if (hadoop.exists(stagingpath)) hadoop.delete(stagingpath, true)
  hadoop.mkdirs(stagingpath)

  val trips_a = new Path("/user/fall2019/pranav/project4/trips")
  val calendar_a = new Path("/user/fall2019/pranav/project4/calendar_dates")
  val frequencies_a = new Path("/user/fall2019/pranav/project4/calendar_dates")

  hadoop.mkdirs(trips_a)
  hadoop.mkdirs(calendar_a)
  hadoop.mkdirs(frequencies_a)
  val drop_trips = "DROP TABLE fall2019_pranav.ext_trips"
  stmt.execute(drop_trips)

  val query_trips = " CREATE EXTERNAL TABLE IF NOT EXISTS fall2019_pranav.ext_trips ( " +
    " route_id INT, " +
    " service_id STRING, " +
    " trip_id STRING, " +
    " trip_headsign STRING, " +
    " direction_id INT, " +
    " shape_id INT, " +
    " wheelchair_accessible INT, " +
    " note_fr STRING, " +
    " note_en STRING " +
    ")" +
    "ROW FORMAT DELIMITED " +
    "FIELDS TERMINATED BY ',' " +
    "STORED AS TEXTFILE " +
    "LOCATION '/user/fall2019/pranav/project4/trips/' " +
    "TBLPROPERTIES ('skip.header.line.count' = '1', 'serialization.null.format' = '')"
  stmt.execute(query_trips)
  println("created external table for trips\n")

  val drop_calendar = "DROP TABLE fall2019_pranav.ext_calendar_dates"
  stmt.execute(drop_calendar)
  val query_calendar_dates = "CREATE EXTERNAL TABLE IF NOT EXISTS fall2019_pranav.ext_calendar_dates ( " +
    "service_id STRING, " +
    "cdate STRING, " +
    "exception_type INT " +
    ") " +
    " ROW FORMAT DELIMITED " +
    " FIELDS TERMINATED BY ',' " +
    "STORED AS TEXTFILE " +
    "LOCATION '/user/fall2019/pranav/project4/calendar_dates/' " +
    "TBLPROPERTIES ('skip.header.line.count' = '1', 'serialization.null.format' = '') "
  stmt.execute(query_calendar_dates)
  print("created external table for calendar_dates\n")

  val drop_frequencies = "DROP TABLE fall2019_pranav.ext_frequencies"
  stmt.execute(drop_frequencies)
  val query_frequencies = "CREATE EXTERNAL TABLE IF NOT EXISTS fall2019_pranav.ext_frequencies ( " +
    "trip_id STRING, " +
    "start_time STRING, " +
    "end_time STRING, " +
    "headway_secs INT " +
    ") " +
    " ROW FORMAT DELIMITED " +
    " FIELDS TERMINATED BY ',' " +
    "STORED AS TEXTFILE " +
    "LOCATION '/user/fall2019/pranav/project4/frequencies/' " +
    "TBLPROPERTIES ('skip.header.line.count' = '1', 'serialization.null.format' = '') "
  stmt.execute(query_frequencies)
  print("created external table for frequencies\n")

  val query_dynamic = "set hive.exec.dynamic.partition.mode=nonstrict;"
  stmt.execute(query_dynamic) // To allow dynamic partitiions permission

  val drop_enriched = "DROP TABLE enriched_trip"
  stmt.execute(drop_enriched)
  val query_enriched_trip = "CREATE TABLE IF NOT EXISTS enriched_trip ( " +
    "route_id INT, " +
    "service_id STRING, " +
    "trip_id STRING, " +
    "trip_headsign STRING, " +
    "direction_id INT, " +
    "shape_id INT, " +
    "note_fr STRING, " +
    "note_en STRING, " +
    "cdate STRING, " +
    "exception_type INT, " +
    "start_time STRING, " +
    "end_time STRING, " +
    "headway_secs INT " +
    ") " +
    "PARTITIONED BY (wheelchair_accessible INT)" +
    "STORED AS PARQUET " +
    "TBLPROPERTIES('parquet.compression' = 'GZIP') "
  stmt.execute(query_enriched_trip)
  print("created enriched_trip table\n")
  hadoop.copyFromLocalFile(new Path("/home/pranav/Downloads/trips.txt"), new Path("/user/fall2019/pranav/project4/trips"))
  hadoop.copyFromLocalFile(new Path("/home/pranav/Downloads/frequencies.txt"), new Path("/user/fall2019/pranav/project4/frequencies"))
  hadoop.copyFromLocalFile(new Path("/home/pranav/Downloads/calendar_dates.txt"), new Path("/user/fall2019/pranav/project4/calendar_dates"))
  print("copied files to hdfs\n")

  val insertquery_enrich_trip = "INSERT OVERWRITE TABLE enriched_trip PARTITION(wheelchair_accessible) "
  val selectquery_enrich_trip = " SELECT tr.route_id, tr.service_id, tr.trip_id, tr.trip_headsign, tr.direction_id, " +
    "tr.shape_id, tr.note_fr,tr.note_en, ca.cdate,ca.exception_type, fr.start_time,fr.end_time, " +
    "fr.headway_secs, tr.wheelchair_accessible " +
    " FROM ext_trips tr LEFT JOIN ext_calendar_dates ca ON tr.service_id = ca.service_id " +
    "LEFT JOIN ext_frequencies fr ON tr.trip_id = fr.trip_id"


  stmt.execute(insertquery_enrich_trip + selectquery_enrich_trip)
  print("inserted data to enriched table\n")


  stmt.close()
  connection.close()
}

