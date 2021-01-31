// Databricks notebook source
import org.apache.spark.sql.functions._

// COMMAND ----------

val mountName = "s3data"
val mountPath = "/mnt/"+mountName

// COMMAND ----------

val bronzeDF = spark.sql("SELECT * FROM csgodata_furia_bronze.ids")

// COMMAND ----------

// MAGIC %sql 
// MAGIC CREATE DATABASE IF NOT EXISTS csgodata_furia_silver

// COMMAND ----------

val database = "csgodata_furia_silver"
val layer = "_silver_db"

// COMMAND ----------

// **Utilities**

// defining flame grenades table

val incendiariesSilverDF = bronzeDF.withColumn("incendiaries", explode($"incendiaries"))
  .select("incendiaries.*","id")
  .select("id", "thrower_name", "thrower_side", "round_number", "heatmap_point.X","heatmap_point.Y",
          "thrower_steamid", "tick","seconds")

val molotovSilverDF = bronzeDF.withColumn("molotovs", explode($"molotovs"))
  .select("molotovs.*","id")
  .select("id", "thrower_name", "thrower_side", "round_number", "heatmap_point.X","heatmap_point.Y",
          "thrower_steamid", "tick","seconds")

val flamesSilverDF = incendiariesSilverDF.union(molotovSilverDF)

// defining smokes table

val smokesSilverDF = bronzeDF
  .withColumn("rounds", explode($"rounds"))
  .select("rounds.*","id")
  .withColumn("smokes_started", explode($"smokes_started"))
  .select("smokes_started.*","id")
  .select("id","thrower_name", "thrower_side", "round_number", "heatmap_point.X","heatmap_point.Y",
          "thrower_steamid", "seconds")

// **Kills**

// defining kills table

val killsSilverDF = bronzeDF.withColumn("kills", explode($"kills"))
  .select("kills.*","id","map_name")
  .select("id","map_name","killer_side", "killer_team", "killed_side", "killed_team",
         "killer_name", "killed_name", "assister_name", "heatmap_point.killer_x","heatmap_point.killer_y","round_number",
         "heatmap_point.victim_x","heatmap_point.victim_y","seconds","weapon.*","time_death_seconds")

// **Rounds**

// defining rounds table

val roundsSilverDF = bronzeDF.withColumn("rounds", explode($"rounds"))
  .select("id","rounds.*","map_name")
  .select("id","map_name","number", "tick", "end_tick", "end_reason", "winner_side", "kills",
       "winner_name", "team_t_name", "team_ct_name", "equipement_value_team_t",
       "equipement_value_team_ct", "start_money_team_t", "start_money_team_ct",
       "bomb_defused_count", "bomb_exploded_count", "bomb_planted_count","flashbang_thrown_count", "smoke_thrown_count", "he_thrown_count", "molotov_thrown_count", "incendiary_thrown_count", "duration", "damage_health_count","damage_armor_count", "average_health_damage_per_player")
  .withColumnRenamed("number", "round_number")
  .withColumn("kills", size($"kills"))
  .withColumnRenamed("kills", "kills_count")

// **Teams**

// defining team Terrorist table

val trSilverDF = bronzeDF.select("team_t","id","map_name")
  .select("id","map_name","team_t.*")
  .withColumn("team_players", explode($"team_players"))
  .select("id","map_name","team_players.*")
  .select("id","map_name","name","team_name","avg_time_death","deaths","entry_hold_kill_loss_count",
         "entry_hold_kill_won_count","entry_kill_loss_count","entry_kill_won_count",
         "entry_kills","kill_count","kill_per_round","trade_kill_count")
  .withColumn("entry_kills",size($"entry_kills"))
  .withColumn("deaths",size($"deaths"))

val ctSilverDF = bronzeDF.select("team_ct","id","map_name")
  .select("id","map_name","team_ct.*")
  .withColumn("team_players", explode($"team_players"))
  .select("id","map_name","team_players.*")
  .select("id","map_name","name","team_name","avg_time_death","deaths","entry_hold_kill_loss_count",
         "entry_hold_kill_won_count","entry_kill_loss_count","entry_kill_won_count",
         "entry_kills","kill_count","kill_per_round","trade_kill_count")
  .withColumn("entry_kills",size($"entry_kills"))
  .withColumn("deaths",size($"deaths"))

val playersSilverDF = trSilverDF.union(ctSilverDF)

// **Bombs**

// defining bomb planted table

val bombPlantedSilverDF = bronzeDF.withColumn("rounds", explode($"rounds"))
  .select("id","rounds.*","map_name")
  .select("id","map_name","number", "end_reason", "winner_side",
       "winner_name", "team_t_name", "team_ct_name", "bomb_planted.*", "equipement_value_team_ct", "start_money_team_t", "start_money_team_ct",
       "bomb_defused_count", "bomb_exploded_count", "bomb_planted_count")
  .withColumnRenamed("number", "round_number")

// defining bomb defused table

val bombDefusedSilverDF = bronzeDF.withColumn("rounds", explode($"rounds"))
  .select("id","rounds.*","map_name")
  .select("id","map_name","number", "end_reason", "winner_side",
       "winner_name", "team_t_name", "team_ct_name", "bomb_defused.*", "equipement_value_team_ct", "start_money_team_t", "start_money_team_ct",
       "bomb_defused_count", "bomb_exploded_count", "bomb_planted_count")
  .withColumnRenamed("number", "round_number")

// defining bomb exploded table

val bombExplodedSilverDF = bronzeDF.withColumn("rounds", explode($"rounds"))
  .select("id","rounds.*","map_name")
  .select("id","map_name","number", "end_reason", "winner_side",
       "winner_name", "team_t_name", "team_ct_name", "bomb_exploded.*", "equipement_value_team_ct", "start_money_team_t", "start_money_team_ct",
       "bomb_defused_count", "bomb_exploded_count", "bomb_planted_count")
  .withColumnRenamed("number", "round_number")


// COMMAND ----------

// MAGIC %sql 
// MAGIC CREATE DATABASE IF NOT EXISTS csgodata_furia_silver

// COMMAND ----------

// ** DATA PERSIST **

// Creating Incendiaries Silver Table

val incendiariesSilverId = "incendiaries"+layer

if(!spark.catalog.tableExists(database+"."+incendiariesSilverId)){
  incendiariesSilverDF
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(database+"."+incendiariesSilverId)
  
} else {
  incendiariesSilverDF
    .createOrReplaceTempView("incendiaries")
    spark.sql(s"""
    MERGE INTO ${database+"."+incendiariesSilverId} as target
    USING incendiaries as source
    ON target.id = source.id
    WHEN NOT MATCHED THEN
     INSERT *
    """)
}

// Creating Molotovs Silver Table

val molotovsSilverId = "molotovs"+layer

if(!spark.catalog.tableExists(database+"."+molotovsSilverId)){
  molotovSilverDF 
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(database+"."+molotovsSilverId)
  
} else {
  molotovSilverDF 
    .createOrReplaceTempView("molotov")
    spark.sql(s"""
    MERGE INTO ${database+"."+molotovsSilverId} as target
    USING molotov as source
    ON target.id = source.id
    WHEN NOT MATCHED THEN
     INSERT *
    """)
}

// Creating Flames Silver Table

val flamesSilverId = "flames"+layer

if(!spark.catalog.tableExists(database+"."+flamesSilverId)){
  flamesSilverDF 
    .write
    .format("delta")
    .mode("append")
    .option("path", mountPath+"/pipelineExport/silver/gr/flames/")
    .saveAsTable(database+"."+flamesSilverId)
  
} else {
  flamesSilverDF 
    .createOrReplaceTempView("flames")
    spark.sql(s"""
    MERGE INTO ${database+"."+flamesSilverId} as target
    USING flames as source
    ON target.id = source.id
    WHEN NOT MATCHED THEN
     INSERT *
    """)
}

// Creating Smokes Silver Table

val smokesSilverId = "smokes"+layer

if(!spark.catalog.tableExists(database+"."+smokesSilverId)){
  smokesSilverDF
    .write
    .format("delta")
    .mode("append")
    .option("path", mountPath+"/pipelineExport/silver/gr/smokes/")
    .saveAsTable(database+"."+smokesSilverId)
  
} else {
  smokesSilverDF
    .createOrReplaceTempView("smokes")
    spark.sql(s"""
    MERGE INTO ${database+"."+smokesSilverId} as target
    USING smokes as source
    ON target.id = source.id
    WHEN NOT MATCHED THEN
     INSERT *
    """)
}

// Creating Kills Silver Table

val killsSilverId = "kills"+layer

if(!spark.catalog.tableExists(database+"."+killsSilverId)){
  killsSilverDF
    .write
    .format("delta")
    .mode("append")
    .option("path", mountPath+"/pipelineExport/silver/kills/")
    .saveAsTable(database+"."+killsSilverId)
  
} else {
  killsSilverDF
    .createOrReplaceTempView("kills")
    spark.sql(s"""
    MERGE INTO ${database+"."+killsSilverId} as target
    USING kills as source
    ON target.id = source.id
    WHEN NOT MATCHED THEN
     INSERT *
    """)
}

// Creating Rounds Silver Table

val roundsSilverId = "rounds"+layer

if(!spark.catalog.tableExists(database+"."+roundsSilverId)){
  roundsSilverDF
    .write
    .format("delta")
    .mode("append")
    .option("path", mountPath+"/pipelineExport/silver/rounds/")
    .saveAsTable(database+"."+roundsSilverId)
  
} else {
  roundsSilverDF
    .createOrReplaceTempView("rounds")
    spark.sql(s"""
    MERGE INTO ${database+"."+roundsSilverId} as target
    USING rounds as source
    ON target.id = source.id
    WHEN NOT MATCHED THEN
     INSERT *
    """)
}

// Creating TR Silver Table

val playersSilverId = "players"+layer

if(!spark.catalog.tableExists(database+"."+playersSilverId)){
  playersSilverDF
    .write
    .format("delta")
    .mode("append")
    .option("path", mountPath+"/pipelineExport/silver/players/")
    .saveAsTable(database+"."+playersSilverId)
  
} else {
  playersSilverDF
    .createOrReplaceTempView("players")
    spark.sql(s"""
    MERGE INTO ${database+"."+playersSilverId} as target
    USING players as source
    ON target.id = source.id
    WHEN NOT MATCHED THEN
     INSERT *
    """)
}

// Creating Bomb Planted Silver Table

val bombPlantedId = "bomb_planted"+layer

if(!spark.catalog.tableExists(database+"."+bombPlantedId)){
  bombPlantedSilverDF
    .write
    .format("delta")
    .mode("append")
    .option("path", mountPath+"/pipelineExport/silver/bombs/planted/")
    .saveAsTable(database+"."+bombPlantedId)
  
} else {
  bombPlantedSilverDF
    .createOrReplaceTempView("planted")
    spark.sql(s"""
    MERGE INTO ${database+"."+bombPlantedId} as target
    USING planted as source
    ON target.id = source.id
    WHEN NOT MATCHED THEN
     INSERT *
    """)
}

// Creating Bomb Planted Silver Table

val bombDefusedId = "bomb_defused"+layer

if(!spark.catalog.tableExists(database+"."+bombDefusedId)){
  bombDefusedSilverDF
    .write
    .format("delta")
    .mode("append")
    .option("path", mountPath+"/pipelineExport/silver/bombs/defused/")
    .saveAsTable(database+"."+bombDefusedId)
  
} else {
  bombDefusedSilverDF
    .createOrReplaceTempView("defused")
    spark.sql(s"""
    MERGE INTO ${database+"."+bombDefusedId} as target
    USING defused as source
    ON target.id = source.id
    WHEN NOT MATCHED THEN
     INSERT *
    """)
}

// Creating Bomb Planted Silver Table

val bombExplodedId = "bomb_exploded"+layer

if(!spark.catalog.tableExists(database+"."+bombExplodedId)){
  bombExplodedSilverDF
    .write
    .format("delta")
    .mode("append")
    .option("path", mountPath+"/pipelineExport/silver/bombs/exploded/")
    .saveAsTable(database+"."+bombExplodedId)
  
} else {
  bombExplodedSilverDF
    .createOrReplaceTempView("exploded")
    spark.sql(s"""
    MERGE INTO ${database+"."+bombExplodedId} as target
    USING exploded as source
    ON target.id = source.id
    WHEN NOT MATCHED THEN
     INSERT *
    """)
}

// COMMAND ----------


