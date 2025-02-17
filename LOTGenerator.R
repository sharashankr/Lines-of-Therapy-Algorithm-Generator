library(R6)
library(readxl)
library(RPostgres)
library(gtools)
library(lubridate)
library(stringr)
library(zoo)
library(tidyverse)
library(dplyr)


#' @import R6

#' @export
LOTGenerator <- R6::R6Class(
  "LOTGenerator",
  
  public = list(
    conn = NULL,
    # Database connection
    schema = NULL,
    # Schema name
    file_path = NULL,
    # File path for the input data
    identifier = NULL,
    
    # Constructor to initialize the object
    initialize = function(conn, schema, file_path, identifier) {
      self$conn <- conn
      self$schema <- schema
      self$file_path <- file_path
      self$identifier <- identifier
    },
    
    # Method to create a drug reference from a file
    create_drug_reference = function() {
      # Read drug and treatment input data from specified sheets
      df <- read_excel(self$file_path, sheet = "Code_Input")
      df_No_Code <-
        read_excel(self$file_path, sheet = "Treatment_Input")
      
      drug_reference <- data.frame()
      
      # Loop through unique drugs and vocabularies to create a reference
      for (c in unique(df$Drug)) {
        for (d in unique(df$Vocabulary)) {
          filtered_df <- df %>% filter(Drug == c & Vocabulary == d)
          codes <- unique(filtered_df$Code)
          codes <- paste(unlist(codes), collapse = "|")
          
          new_row <-
            data.frame(Name = toupper(c),
                       Vocabulary = d,
                       Code = codes)
          
          drug_reference <- rbind(drug_reference, new_row)
        }
      }
      
      # Create rows for drugs without codes
      for (c in unique(df_No_Code$upd_name)) {
        new_row_description <- data.frame(
          Name = toupper(c),
          Vocabulary = 'DESCRIPTION',
          Code = paste0("%", toupper(c), "%")
        )
        drug_reference <- rbind(drug_reference, new_row_description)
      }
      
      return(drug_reference)
    },
    
    # Method to generate SQL queries based on a dataframe
    generate_sql_queries = function(dff) {
      sqls <- list()
      
      for (d in 1:nrow(dff)) {
        row <- dff[d, ]
        ddays_sup <-
          ifelse(
            !is.na(row$days_sup_avl) &&
              row$days_sup_avl == "N",
            row$days_sup,
            row$days_sup_avl
          )
        
        # Determine SQL comparison operator based on vocabulary
        funct <-
          ifelse(row$Vocabulary == "DESCRIPTION", 'ilike', 'similar to')
        to_compare <- row$Code
        
        # Construct the SQL query
        if (row$End == 'N') {
          sql <- sprintf(
            "SELECT %10$s AS patientid, '%8$s' AS upd_name, %1$s, %2$s AS startdate, %2$s AS enddate, %9$s AS grace_period,
             '%12$s' AS Category, '%13$s' AS Sub_Category
            FROM %5$s.%6$s
            WHERE %1$s %11$s '%7$s'
            AND %2$s IS NOT NULL",
            row$col_name,
            row$Start,
            row$End,
            ddays_sup,
            row$database,
            row$Table,
            to_compare,
            row$upd_name,
            row$grace_period,
            row$ID,
            funct,
            row$Category,
            row$Sub_Category
          )
        } else {
          sql <- sprintf(
            "SELECT %10$s AS patientid, '%8$s' AS upd_name, %1$s, %2$s AS startdate, %3$s AS enddate, %9$s AS grace_period,
           '%12$s' AS Category, '%13$s' AS Sub_Category
            FROM %5$s.%6$s
            WHERE %1$s %11$s '%7$s'
            AND %2$s IS NOT NULL",
            row$col_name,
            row$Start,
            row$End,
            ddays_sup,
            row$database,
            row$Table,
            to_compare,
            row$upd_name,
            row$grace_period,
            row$ID,
            funct,
            row$Category,
            row$Sub_Category
          )
        }
        
        sqls <- append(sqls, sql)
      }
      
      return(paste(sqls, collapse = " UNION \n"))
    },
    
    # Method to generate data from drug reference and additional inputs
    generate_data = function() {
      drug_reference <- self$create_drug_reference()
      
      dff <-
        read_excel(self$file_path, sheet = "Treatment_Input") %>%
        mutate(
          Name = toupper(upd_name),
          grace_period = case_when(
            class == 'IO' ~ days_sup * 4,
            class == 'Non-IO' ~ days_sup * 2,
            class == 'PARP' ~ days_sup * 3,
            class == 'Targeted' ~ days_sup * 4
          )
        ) %>%
        inner_join(drug_reference, by = 'Name')
      
      dfff <- read_excel(self$file_path, sheet = "Database")
      
      dff <- dff %>%
        inner_join(
          dfff %>% select(
            database,
            Table,
            Start,
            End,
            Vocabulary,
            days_sup_avl,
            col_name,
            ID
          ),
          by = 'Vocabulary'
        )
      
      sql_query <- self$generate_sql_queries(dff)
      
      # Drop the existing table if it exists
      dbGetQuery(
        self$conn,
        sprintf(
          "DROP TABLE IF EXISTS %s.%s_LOT_Data;",
          self$schema,
          self$identifier
        )
      )
      
      # Create a new table with the generated SQL query
      create_query <-
        sprintf("CREATE TABLE %s.%s_LOT_Data AS (%s)",
                self$schema,
                self$identifier,
                sql_query)
      dbGetQuery(self$conn, create_query)
    },
    
    
    
    # Method to create a table with given parameters
    create_cohort = function() {
      df <- read_excel(self$file_path, sheet = "Cohort")
      
      # Extract values based on the 'Element' column
      Table_Schema <-
        as.character(df$Input[df$Element == "Table_Schema"])  # Extract value for Table_Schema
      Table <-
        as.character(df$Input[df$Element == "Table"])  # Extract value for Table
      Patient_ID <-
        as.character(df$Input[df$Element == "Patient_ID"])  # Extract value for Patient_ID
      Treatment_Index <-
        as.character(df$Input[df$Element == "Treatment_Index"])
      Setting_Index <-
        as.character(df$Input[df$Element == "Setting_Index"])
      Before_Index <-
        as.character(df$Input[df$Element == "Before_Index"])
      After_Index <-
        as.character(df$Input[df$Element == "After_Index"])
      
      
      # Drop the output table if it exists
      dbGetQuery(
        self$conn,
        sprintf(
          "DROP TABLE IF EXISTS %s.%s_LOT_cohort;",
          self$schema,
          self$identifier
        )
      )
      
      dbGetQuery(
        self$conn,
        sprintf(
          "CREATE TABLE %s.%s_LOT_cohort as
                         SELECT %s as patientid, %s as treatment_index from %s.%s",
          self$schema,
          self$identifier,
          Patient_ID,
          Treatment_Index,
          Table_Schema,
          Table
        )
      )
      return(
        list(
          Setting_Index = Setting_Index,
          Before_Index = Before_Index,
          After_Index = After_Index
        )
      )
    }
    
    ,
    # Method to create a table with given parameters
    setting_classifier = function() {
      df <- read_excel(self$file_path, sheet = "Cohort")
      
      # Extract values based on the 'Element' column
      Table_Schema <-
        as.character(df$Input[df$Element == "Table_Schema"])  # Extract value for Table_Schema
      Table <-
        as.character(df$Input[df$Element == "Table"])  # Extract value for Table
      Patient_ID <-
        as.character(df$Input[df$Element == "Patient_ID"])  # Extract value for Patient_ID
      Setting_Index <-
        as.character(df$Input[df$Element == "Setting_Index"])
      Before_Index <-
        as.character(df$Input[df$Element == "Before_Index"])
      After_Index <-
        as.character(df$Input[df$Element == "After_Index"])
      
      
      # Drop the output table if it exists
      dbGetQuery(
        self$conn,
        sprintf(
          "DROP TABLE IF EXISTS %s.%s_LOT_classification;",
          self$schema,
          self$identifier
        )
      )
      
      dbGetQuery(
        self$conn,
        sprintf(
          "CREATE TABLE %s.%s_LOT_classification as
                         SELECT %s as patientid, %s as setting_index from %s.%s",
          self$schema,
          self$identifier,
          Patient_ID,
          Setting_Index,
          Table_Schema,
          Table
        )
      )
    },
    
    
    line_fix = function(input_table,
                        line_name_1,
                        line_name_2,
                        duration,
                        col) {
      # Ensure duration is an integer if it is not NA
      if (!is.null(duration) && !is.na(duration)) {
        duration <- as.integer(duration)
        
        # Construct the SQL sub-query for switch criteria with duration
        sub_query <- sprintf(
          "(switch_flag = 'Yes' AND LAG(switch_flag) OVER (PARTITION BY patientid ORDER BY line_of_therapy, seq_n) = 'Yes' AND switch_interval <= %d) THEN 'Y'",
          duration
        )
      } else {
        # Construct the SQL sub-query for switch criteria without duration
        sub_query <-
          "(switch_flag = 'Yes' AND LAG(switch_flag) OVER (PARTITION BY patientid ORDER BY line_of_therapy, seq_n) = 'Yes') THEN 'Y'"
      }
      
      # Drop the temporary table if it exists
      dbGetQuery(self$conn,
                 sprintf("DROP TABLE IF EXISTS %s.temp_line_fix;", self$schema))
      
      # Construct SQL query to create the temporary line fix table
      sql <- sprintf(
        "CREATE TABLE %s.temp_line_fix AS
    WITH STEP_1 AS (
        SELECT
            patientid::VARCHAR(255),   -- Convert patientid to VARCHAR
            line_name,                 -- Original line name
            category,
            sub_category,
            line_of_therapy,           -- Original line of therapy
            seq_n,                     -- Sequence number of therapy
            line_start,                -- Start date of therapy
            line_end,                  -- End date of therapy
            setting_index,
            line_setting,              -- Setting of therapy

            -- Determine if a line should switch
            CASE
                WHEN (%s SIMILAR TO '%s' OR
                      %s SIMILAR TO '%s')
                THEN 'Yes'                 -- Flag for switch
                ELSE 'No'
            END AS switch_flag,

            -- Calculate the switch interval
            CASE
                WHEN switch_flag = 'Yes' AND LAG(switch_flag) OVER (PARTITION BY patientid ORDER BY line_of_therapy, seq_n) SIMILAR TO 'Yes'
                THEN DATEDIFF(DAY, line_end, LEAD(line_start) OVER (PARTITION BY patientid ORDER BY line_of_therapy, seq_n))
                ELSE NULL
            END AS switch_interval
        FROM %s.%s
    ),
    STEP_2 AS (
        SELECT *,
            CASE
                WHEN %s
                ELSE 'N'                   -- Default linefix value
            END AS linefix
        FROM STEP_1
    ),
    STEP_3 AS (
        SELECT
            patientid,
            line_name,
            category,
            sub_category,
            line_of_therapy,
            seq_n,
            line_start,
            line_end,
            setting_index,
            line_setting,
            switch_flag,
            switch_interval,
            linefix,

            -- Determine new line of therapy based on linefix flag
            CASE
                WHEN linefix = 'Y'
                THEN LAG(line_of_therapy) OVER (PARTITION BY patientid ORDER BY line_of_therapy, seq_n)
                ELSE line_of_therapy
            END AS new_line_of_therapy,

            -- Determine new sequence number based on linefix flag
            CASE
                WHEN linefix = 'Y'
                THEN LAG(seq_n) OVER (PARTITION BY patientid ORDER BY line_of_therapy, seq_n) + 1
                ELSE seq_n
            END AS new_seq_n
        FROM STEP_2
    )
    SELECT patientid,
            line_name,
            category,
            sub_category,
            new_line_of_therapy AS line_of_therapy,
            new_seq_n AS seq_n,
            line_start,
            line_end,
            setting_index,
            line_setting
    FROM STEP_3",
        self$schema,
        col,
        line_name_1,
        col,
        line_name_2,
        self$schema,
        input_table,
        sub_query
      )
      
      
      # Execute the SQL to create the temp table
      dbGetQuery(self$conn, sql[1])
      
      # Drop the existing output table if it exists
      dbGetQuery(self$conn,
                 sprintf("DROP TABLE IF EXISTS %s.%s;", self$schema, input_table))
      
      
      # Create the final output table from the temp table
      dbGetQuery(
        self$conn,
        sprintf(
          "CREATE TABLE %s.%s AS SELECT * FROM %s.temp_line_fix;",
          self$schema,
          input_table,
          self$schema
        )
      )
      
    }
    
    
    ,
    
    episodes = function() {
      self$create_cohort()
      
      # Drop existing table if it exists
      
      sql <-
        sprintf("DROP TABLE IF EXISTS %s.%s_episodes_a;",
                self$schema,
                self$identifier)
      dbGetQuery(self$conn, sql)
      
      # Create table with episode information
      sql <- sprintf(
        "
    CREATE TABLE %s.%s_episodes_a AS (
      SELECT patientid, upd_name AS drug_name, category, sub_category, EPISODE_NUM,
             MIN(startdt) AS EPISODE_START_DATE,
             MAX(enddt) AS EPISODE_END_DATE
      FROM (
        SELECT *,
               SUM(IS_EPISODE_START) OVER (PARTITION BY patientid, upd_name, category, sub_category ORDER BY startdt ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS EPISODE_NUM
        FROM (
          SELECT *,
                 CASE
                   WHEN (PREV_startdt IS NULL) OR (startdt - PREV_enddt > grace_period) THEN 1
                   ELSE 0
                 END AS IS_EPISODE_START,
                 CASE
                   WHEN (NEXT_startdt IS NULL) OR (NEXT_startdt - enddt > grace_period) THEN 1
                   ELSE 0
                 END AS IS_EPISODE_END
          FROM (
            SELECT patientid, upd_name, category, sub_category,
                   TO_DATE(startdt, 'YYYY-MM-DD') AS startdt,
                   TO_DATE(enddt, 'YYYY-MM-DD') AS enddt,
                   grace_period,
                   LAG(startdt) OVER (PARTITION BY patientid, upd_name, category, sub_category ORDER BY startdt) AS PREV_startdt,
                   LEAD(startdt) OVER (PARTITION BY patientid, upd_name, category, sub_category ORDER BY startdt) AS NEXT_startdt,
                   LAG(enddt) OVER (PARTITION BY patientid, upd_name, category, sub_category ORDER BY enddt) AS PREV_enddt,
                   LEAD(enddt) OVER (PARTITION BY patientid, upd_name, category, sub_category ORDER BY enddt) AS NEXT_enddt
            FROM (
              SELECT patientid, upd_name, category, sub_category,
                     TO_DATE(startdate, 'YYYY-MM-DD') AS startdt,
                     TO_DATE(enddate, 'YYYY-MM-DD') AS enddt,
                     grace_period
              FROM (
              select * from (select a.*, dia_date from %s.%s_LOT_Data a inner join
              (select patientid, treatment_index as dia_date from %s.%s_LOT_cohort) b on a.patientid=b.patientid)
              where startdate >= dia_date
              )
            ) AS A
          ) AS B
        ) AS C
      ) AS D
      GROUP BY patientid, upd_name, category, sub_category, EPISODE_NUM
    );",
        self$schema,
        self$identifier,
        self$schema,
        self$identifier,
        self$schema,
        self$identifier
      )
      
      dbGetQuery(self$conn, sql)
      
      sql <-
        sprintf("DROP TABLE IF EXISTS %s.%s_episodes_b;",
                self$schema,
                self$identifier)
      dbGetQuery(self$conn, sql)
      
      sql <- sprintf(
        "CREATE TABLE %s.%s_episodes_b AS
        WITH RankedEpisodes AS (
    SELECT
        patientid,
        drug_name,
        category, sub_category,
        episode_num,
        episode_start_date,
        episode_end_date,
        ROW_NUMBER() OVER (PARTITION BY patientid, drug_name, category, sub_category ORDER BY episode_start_date) AS row_num
    FROM %s.%s_episodes_a
),
OverlappingEpisodes AS (
    SELECT
        a.patientid,
        a.drug_name,
        a.episode_num,
        a.episode_start_date,
        a.episode_end_date,
        a.row_num,
        a.category,
        a.sub_category,
        b.episode_start_date AS next_episode_start,
        b.episode_end_date AS next_episode_end,
        b.row_num AS next_row_num
    FROM RankedEpisodes a
    LEFT JOIN RankedEpisodes b
        ON a.patientid = b.patientid
        AND a.drug_name = b.drug_name
        AND a.row_num != b.row_num
        AND a.episode_start_date <= b.episode_end_date
        AND a.episode_end_date >= b.episode_start_date
),
FilteredEpisodes AS (
    SELECT
        a.patientid,
        a.drug_name,
        a.category,
        a.sub_category,
        a.episode_num,
        a.episode_start_date,
        a.episode_end_date
    FROM OverlappingEpisodes a
    WHERE NOT EXISTS (
        SELECT 1
        FROM OverlappingEpisodes b
        WHERE a.patientid = b.patientid
        AND a.drug_name = b.drug_name
        AND a.episode_start_date <= b.episode_end_date
        AND a.episode_end_date >= b.episode_start_date
        AND a.row_num > b.row_num
    )
)
SELECT
    patientid,
    drug_name,
    category,
    sub_category,
    ROW_NUMBER() OVER (PARTITION BY patientid, drug_name, category, sub_category ORDER BY episode_start_date) AS episode_num,
    episode_start_date,
    episode_end_date
FROM FilteredEpisodes
ORDER BY patientid, drug_name, episode_start_date;
", self$schema, self$identifier, self$schema, self$identifier)
  dbGetQuery(self$conn, sql)
  
  sql <-
    sprintf(
      "DROP TABLE IF EXISTS %s.%s_episodes;",
      self$schema,
      self$identifier
    )
  dbGetQuery(self$conn, sql)
  
  sql <- sprintf(
    "CREATE TABLE %s.%s_episodes AS
    WITH RankedEpisodes AS (
    SELECT
        patientid,
        drug_name,
        category,
        sub_category,
        episode_num,
        episode_start_date,
        episode_end_date,
        ROW_NUMBER() OVER (PARTITION BY patientid, drug_name, category, sub_category, episode_start_date, episode_end_date ORDER BY episode_start_date) AS row_num
    FROM %s.%s_episodes_b
)
SELECT
    patientid,
    drug_name,
    category,
    sub_category,
    episode_num,
    episode_start_date,
    episode_end_date
FROM RankedEpisodes
WHERE row_num = 1
ORDER BY patientid, drug_name, episode_start_date;"
  ,
  self$schema,
  self$identifier,
  self$schema,
  self$identifier
  )

dbGetQuery(self$conn, sql)
    },

regimens = function()
{
  sql <-
    sprintf("DROP TABLE IF EXISTS %s.%s_regimens_a;",
            self$schema,
            self$identifier)
  dbGetQuery(self$conn, sql)
  
  sql <- sprintf(
    "CREATE TABLE %s.%s_regimens_a AS
    (SELECT DISTINCT PATIENTID, DT
     FROM (
       SELECT PATIENTID, EPISODE_START_DATE AS DT
       FROM %s.%s_episodes
       WHERE EPISODE_START_DATE <> EPISODE_END_DATE
       UNION
       SELECT PATIENTID, EPISODE_END_DATE AS DT
       FROM %s.%s_episodes
       WHERE EPISODE_START_DATE <> EPISODE_END_DATE
     ) AS A);",
    self$schema,
    self$identifier,
    self$schema,
    self$identifier,
    self$schema,
    self$identifier
  )
  dbGetQuery(self$conn, sql)
  
  
  
  # Part 02 :  Calculating Mid-dates
  
  sql <-
    sprintf("DROP TABLE IF EXISTS %s.%s_regimens_b;",
            self$schema,
            self$identifier)
  dbGetQuery(conn, sql)
  
  sql <- sprintf(
    "CREATE TABLE %s.%s_regimens_b AS (
          SELECT A.PATIENTID,
                 A.REGIMEN_START_DATE,
                 A.REGIMEN_END_DATE,
                 A.REGIMEN_MID_DATE,
                 TRIM(UPPER(B.drug_name)) AS PRODUCT_NAME,
                 B.category,
                 B.sub_category,
                 B.EPISODE_START_DATE,
                 B.EPISODE_END_DATE,
                 ROW_NUMBER() OVER (PARTITION BY A.PATIENTID, A.REGIMEN_START_DATE, A.REGIMEN_END_DATE ORDER BY B.drug_name, B.category,
                 B.sub_category) AS ROW_NUM
          FROM (
              SELECT PATIENTID,
                     REGIMEN_START_DATE,
                     REGIMEN_END_DATE,
                     REGIMEN_START_DATE + 0.5 * (REGIMEN_END_DATE - REGIMEN_START_DATE) AS REGIMEN_MID_DATE
              FROM (
                  SELECT PATIENTID,
                         DT AS REGIMEN_START_DATE,
                         MAX(DT) OVER (PARTITION BY PATIENTID ORDER BY DT ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS REGIMEN_END_DATE
                  FROM %s.%s_regimens_a
              ) A
          ) A
          INNER JOIN %s.%s_episodes B
          ON A.PATIENTID = B.PATIENTID
          AND A.REGIMEN_MID_DATE >= B.EPISODE_START_DATE
          AND A.REGIMEN_MID_DATE < B.EPISODE_END_DATE
      );



      ",
    self$schema,
    self$identifier,
    self$schema,
    self$identifier,
    self$schema,
    self$identifier
  )
  dbGetQuery(conn, sql)
  
  sql <- sprintf(
    "ALTER TABLE %s.%s_regimens_b
                ALTER COLUMN PRODUCT_NAME TYPE VARCHAR(2000);",
    self$schema,
    self$identifier
  )
  dbGetQuery(conn, sql)
  
  sql <- sprintf(
    "ALTER TABLE %s.%s_regimens_b
                ALTER COLUMN CATEGORY TYPE VARCHAR(2000);",
    self$schema,
    self$identifier
  )
  dbGetQuery(conn, sql)
  
  
  sql <- sprintf(
    "ALTER TABLE %s.%s_regimens_b
                ALTER COLUMN SUB_CATEGORY TYPE VARCHAR(2000);",
    self$schema,
    self$identifier
  )
  dbGetQuery(conn, sql)
  
  sql <-
    sprintf("DROP TABLE IF EXISTS %s.%s_regimens_a;",
            self$schema,
            self$identifier)
  dbGetQuery(conn, sql)
  
  
  # Part 03 :  Creating Regimens
  sql <-
    sprintf("DROP TABLE IF EXISTS %s.%s_regimens_c",
            self$schema,
            self$identifier)
  dbGetQuery(conn, sql)
  
  sql <- sprintf(
    "CREATE TABLE %s.%s_regimens_c AS (
          WITH RECURSIVE REGIMEN_AGGREGATE (PATIENTID, REGIMEN_START_DATE, REGIMEN_END_DATE, PRODUCT_NAME, CATEGORY,
                     SUB_CATEGORY, LVL) AS (
              SELECT PATIENTID,
                     REGIMEN_START_DATE,
                     REGIMEN_END_DATE,
                     PRODUCT_NAME,
                     CATEGORY,
                     SUB_CATEGORY,
                     1
              FROM %s.%s_regimens_b
              WHERE ROW_NUM = 1

              UNION ALL

              SELECT INDIRECT.PATIENTID,
                     INDIRECT.REGIMEN_START_DATE,
                     INDIRECT.REGIMEN_END_DATE,
                     CASE
                         WHEN LENGTH(DIRECT.PRODUCT_NAME) + LENGTH(INDIRECT.PRODUCT_NAME) + 1 > 2000
                         THEN LEFT(DIRECT.PRODUCT_NAME || ',' || INDIRECT.PRODUCT_NAME, 2000)
                         ELSE DIRECT.PRODUCT_NAME || ',' || INDIRECT.PRODUCT_NAME
                     END AS PRODUCT_NAME,
                     CASE
                         WHEN LENGTH(DIRECT.CATEGORY) + LENGTH(INDIRECT.CATEGORY) + 1 > 2000
                         THEN LEFT(DIRECT.CATEGORY || ',' || INDIRECT.CATEGORY, 2000)
                         ELSE DIRECT.CATEGORY || ',' || INDIRECT.CATEGORY
                     END AS CATEGORY,
                     CASE
                         WHEN LENGTH(DIRECT.SUB_CATEGORY) + LENGTH(INDIRECT.SUB_CATEGORY) + 1 > 2000
                         THEN LEFT(DIRECT.SUB_CATEGORY || ',' || INDIRECT.SUB_CATEGORY, 2000)
                         ELSE DIRECT.SUB_CATEGORY || ',' || INDIRECT.SUB_CATEGORY
                     END AS SUB_CATEGORY,
                     LVL + 1
              FROM REGIMEN_AGGREGATE AS DIRECT
              JOIN %s.%s_regimens_b AS INDIRECT
              ON DIRECT.PATIENTID = INDIRECT.PATIENTID
              AND DIRECT.REGIMEN_START_DATE = INDIRECT.REGIMEN_START_DATE
              WHERE INDIRECT.ROW_NUM = DIRECT.LVL + 1
          ),
          Ranked_AGGREGATE AS (
              SELECT PATIENTID, REGIMEN_START_DATE, REGIMEN_END_DATE, PRODUCT_NAME, CATEGORY, SUB_CATEGORY, LVL,
                     RANK() OVER (PARTITION BY PATIENTID, REGIMEN_START_DATE ORDER BY LVL DESC) AS RNK
              FROM REGIMEN_AGGREGATE
          )
          SELECT PATIENTID, REGIMEN_START_DATE, REGIMEN_END_DATE, PRODUCT_NAME, CATEGORY, SUB_CATEGORY, LVL
          FROM Ranked_AGGREGATE
          WHERE RNK = 1
      );",
    self$schema,
    self$identifier,
    self$schema,
    self$identifier,
    self$schema,
    self$identifier
  )
  dbGetQuery(conn, sql)
  
  sql <-
    sprintf("DROP TABLE IF EXISTS %s.%s_regimens_b;",
            self$schema,
            self$identifier)
  dbGetQuery(conn, sql)
  

  sql <-
    sprintf("DROP TABLE if exists %s.%s_regimens;",
            self$schema,
            self$identifier)
  dbGetQuery(conn, sql)

  
  sql <- sprintf(
    "CREATE TABLE %s.%s_regimens
      AS
      (SELECT *
      FROM %s.%s_regimens_c
      WHERE REGIMEN_END_DATE - REGIMEN_START_DATE >= 8);
      ",
    self$schema,
    self$identifier,
    self$schema,
    self$identifier
  )
  dbGetQuery(conn, sql)
  
  sql <-
    sprintf("DROP TABLE if EXISTS %s.%s_regimens_d;",
            self$schema,
            self$identifier)
  dbGetQuery(conn, sql)
}
,

lines = function()
{

  sql <-
    sprintf("DROP TABLE if exists %s.%s_lines_a;",
            self$schema,
            self$identifier)
  dbGetQuery(conn, sql)
  
  sql <- sprintf(
    "CREATE TABLE %s.%s_lines_a
      AS
      (SELECT *
      FROM (SELECT A.*,
                   RANK() OVER (PARTITION BY PATIENTID,PRODUCT_NAME,REGIMEN_START_DATE ORDER BY BASE_REGIMEN_ST_DATE) AS RANK_
            FROM (SELECT A.*,
                         SUM(REGIMEN_FLAG) OVER (PARTITION BY PATIENTID,BASE_REGIMEN_ST_DATE ORDER BY REGIMEN_START_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS LOT_FLAG
                  FROM (SELECT B.*,
                               A.PRODUCT_NAME AS BASE_REGIMEN_NAME,
                               A.CATEGORY AS BASE_CATEGORY,
                               A.SUB_CATEGORY AS BASE_SUB_CATEGORY,
                               A.REGIMEN_START_DATE AS BASE_REGIMEN_ST_DATE,
                               A.REGIMEN_END_DATE AS BASE_REGIMEN_END_DATE,
                               CASE
                                 WHEN A.PRODUCT_NAME LIKE '%%' ||REPLACE  (B.PRODUCT_NAME,',','%%') || '%%' THEN 0
                                 ELSE 1
                               END AS REGIMEN_FLAG,
                               CASE
                                 WHEN A.CATEGORY LIKE '%%' ||REPLACE  (B.CATEGORY,',','%%') || '%%' THEN 0
                                 ELSE 1
                               END AS CATEGORY_FLAG,
                               CASE
                                 WHEN A.SUB_CATEGORY LIKE '%%' ||REPLACE  (B.SUB_CATEGORY,',','%%') || '%%' THEN 0
                                 ELSE 1
                               END AS SUB_CATEGORY_FLAG
                        FROM %s.%s_regimens A,
                             %s.%s_regimens B
                        WHERE A.PATIENTID = B.PATIENTID
                        AND   A.REGIMEN_START_DATE <= B.REGIMEN_START_DATE) A) A
            WHERE LOT_FLAG = 0) A
      WHERE RANK_ = 1);",
    self$schema,
    self$identifier,
    self$schema,
    self$identifier,
    self$schema,
    self$identifier
  )
  dbGetQuery(conn, sql)
  
  sql <-
    sprintf("DROP TABLE if exists %s.%s_lines_b;",
            self$schema,
            self$identifier)
  dbGetQuery(conn, sql)
  
  sql <- sprintf(
    "CREATE TABLE %s.%s_lines_b
      AS
      (SELECT PATIENTID,
             PRODUCT_NAME,
             CATEGORY,
             SUB_CATEGORY,
             REGIMEN_START_DATE,
             REGIMEN_END_DATE,
             BASE_REGIMEN_NAME,
             BASE_CATEGORY,
             BASE_SUB_CATEGORY,
             BASE_REGIMEN_ST_DATE,
             BASE_REGIMEN_END_DATE,
             SUM(LOT_INCREMENT_FLAG) OVER (PARTITION BY PATIENTID ORDER BY REGIMEN_START_DATE ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS LINE_OF_THERAPY,
             REGIMEN_END_DATE - REGIMEN_START_DATE AS REGIMEN_LEN
      FROM (SELECT A.*,
                   CASE
                     WHEN MAX(BASE_REGIMEN_NAME) OVER (PARTITION BY PATIENTID ORDER BY REGIMEN_START_DATE ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) = BASE_REGIMEN_NAME THEN 0
                     ELSE 1
                   END AS LOT_INCREMENT_FLAG
            FROM %s.%s_lines_a  A) A) ;",
    self$schema,
    self$identifier,
    self$schema,
    self$identifier
  )
  dbGetQuery(conn, sql)
  
  sql <-
    sprintf("DROP TABLE if exists %s.%s_lines_a;",
            self$schema,
            self$identifier)
  dbGetQuery(conn, sql)
  
  sql <-
    sprintf("DROP TABLE if exists %s.%s_lines_c;",
            self$schema,
            self$identifier)
  dbGetQuery(conn, sql)
  
  sql <- sprintf(
    "
      CREATE TABLE %s.%s_lines_c
      AS
      (SELECT PATIENTID,
             BASE_REGIMEN_NAME AS LINE_NAME,
             BASE_CATEGORY AS CATEGORY,
             BASE_SUB_CATEGORY AS SUB_CATEGORY,
             LINE_OF_THERAPY,
             MIN(REGIMEN_START_DATE) AS LINE_START,
             MAX(REGIMEN_END_DATE) AS LINE_END,
             SUM(REGIMEN_LEN) AS DAYS_ON_THERAPY
      FROM %s.%s_lines_b
      GROUP BY 1,
               2,
               3,
               4,
               5);",
    self$schema,
    self$identifier,
    self$schema,
    self$identifier
  )
  dbGetQuery(conn, sql)
  
  sql <-
    sprintf("DROP TABLE if exists %s.%s_lines_b;",
            self$schema,
            self$identifier)
  dbGetQuery(conn, sql)
  
  sql <-
    sprintf("DROP TABLE if exists %s.%s_lines_d;",
            self$schema,
            self$identifier)
  dbGetQuery(conn, sql)
  
  sql <- sprintf(
    "CREATE TABLE %s.%s_lines_d
      AS
      (SELECT a.*
      FROM %s.%s_lines_c a)
      ;",
    self$schema,
    self$identifier,
    self$schema,
    self$identifier
  )
  dbGetQuery(conn, sql)
  
  sql <-
    sprintf("DROP TABLE if exists %s.%s_lines_c;",
            self$schema,
            self$identifier)
  dbGetQuery(conn, sql)
  
  sql <-
    sprintf("DROP TABLE if exists %s.%s_lines;",
            self$schema,
            self$identifier)
  dbGetQuery(conn, sql)
  
  sql <- sprintf(
    "CREATE TABLE %s.%s_lines
      AS
      (SELECT a.*, treatment_index
      FROM %s.%s_lines_d  a inner join (select patientid, treatment_index from
      %s.%s_LOT_Cohort)b on a.patientid=b.patientid
      )
      ",
    self$schema,
    self$identifier,
    self$schema,
    self$identifier,
    self$schema,
    self$identifier
  )
  dbGetQuery(conn, sql)
  
  sql <-
    sprintf("DROP TABLE if exists %s.%s_lines_d;",
            self$schema,
            self$identifier)
  dbGetQuery(conn, sql)
  
}
,

setting = function()
{
  result <- self$create_cohort()
  Setting_Index <- result$Setting_Index
  Before_Index <- result$Before_Index
  After_Index <- result$After_Index
  
  sql <-
    sprintf("DROP TABLE if exists %s.%s_setting_;",
            self$schema,
            self$identifier)
  dbGetQuery(conn, sql)
  
  if (Setting_Index != 'N')
  {
    self$setting_classifier()
    sql <-
      sprintf("DROP TABLE if exists %s.%s_setting_;",
              self$schema,
              self$identifier)
    dbGetQuery(conn, sql)
    
    
    sql <-
      sprintf("DROP TABLE if exists %s.%s_setting_a;",
              self$schema,
              self$identifier)
    dbGetQuery(conn, sql)
    
    sql <- sprintf(
      "CREATE TABLE %s.%s_setting_
      AS
      (SELECT a.*, setting_index
      FROM %s.%s_lines  a inner join (select patientid, setting_index from
      %s.%s_LOT_classification)b on a.patientid=b.patientid
      )
      ",
      self$schema,
      self$identifier,
      self$schema,
      self$identifier,
      self$schema,
      self$identifier
    )
    dbGetQuery(conn, sql)
    
    
    
    
    sql <- sprintf(
      "CREATE TABLE %s.%s_setting_a
      AS
      WITH ranked_data AS (
          SELECT *,
                 ROW_NUMBER() OVER (PARTITION BY patientid ORDER BY line_start)
                 AS seq_n
          FROM %s.%s_setting_
      ),
      neo_adjuvant AS (
          SELECT *,
                 CASE
                     WHEN treatment_index IS NOT NULL AND treatment_index >
                     line_start
                          AND DATEDIFF(DAY, line_start, treatment_index) <= %s
                          AND line_of_therapy = 1
                     THEN 'NEO-ADJUVANT'
                     ELSE NULL
                 END AS flag
          FROM ranked_data
      ),
      adjuvant AS (
          SELECT *,
                 CASE
                     WHEN setting_index IS NOT NULL AND setting_index
                     < line_start
                          AND DATEDIFF(DAY, setting_index, 
                          ine_start) <= %s
                          AND seq_n = 1
                     THEN 'ADJUVANT'
                     ELSE NULL
                 END AS flag_2
          FROM neo_adjuvant
      )

      SELECT *
      FROM adjuvant;",
      self$schema,
      self$identifier,
      self$schema,
      self$identifier,
      Before_Index,
      After_Index
    )
    dbGetQuery(conn, sql)
    
    sql <-
      sprintf("DROP TABLE if exists %s.%s_setting;",
              self$schema,
              self$identifier)
    dbGetQuery(conn, sql)
    
    
    
    sql <- sprintf(
      "CREATE TABLE %s.%s_setting as
      SELECT patientid,line_name, category, sub_category, line_of_therapy,
      line_start, line_end, days_on_therapy, setting_index, seq_n,
             CASE
               WHEN TRIM(setting_desc) ilike '' OR setting_desc
               IS NULL THEN 'ADVANCED'
               ELSE setting_desc
             END AS line_setting

             FROM (SELECT *,
             COALESCE(flag,flag_2) AS setting_desc
      FROM %s.%s_setting_a);",
      self$schema,
      self$identifier,
      self$schema,
      self$identifier
    )
    dbGetQuery(conn, sql)
    
    sql <-
      sprintf("DROP TABLE if exists %s.%s_setting_a;",
              self$schema,
              self$identifier)
    dbGetQuery(conn, sql)
  }
  else
  {
    print("Cannot add line settings - index for line setting not provided")
    
    sql <-
      sprintf("DROP TABLE if exists %s.%s_setting;",
              self$schema,
              self$identifier)
    dbGetQuery(conn, sql)
    
    sql <- sprintf(
      "CREATE TABLE %s.%s_setting
      AS
      WITH ranked_data AS (
          SELECT *,'NA' as setting_index,
          'NA' as line_setting,
                 ROW_NUMBER() OVER (PARTITION BY patientid
                 ORDER BY line_start) AS seq_n
          FROM %s.%s_lines
      )
      SELECT * FROM ranked_data",
      self$schema,
      self$identifier,
      self$schema,
      self$identifier
    )
    
    dbGetQuery(conn, sql)
    
  }
},



line_adjust = function(input_table)
{
  
  flag <- FALSE
  
  
  while (!flag)
  {
    
    dbGetQuery(self$conn, sprintf("DROP TABLE IF EXISTS TEMP_CH;"))
    
    
    dbGetQuery(
      self$conn,
      sprintf(
        "CREATE TEMPORARY TABLE TEMP_CH AS
         SELECT * FROM %s.%s;",
        self$schema,
        input_table
      )
    )
    
    
    dbGetQuery(self$conn,
               sprintf("DROP TABLE IF EXISTS %s.%s;", 
                       self$schema, input_table))
    
    
    sql <- sprintf(
      "CREATE TABLE %s.%s AS
       WITH STEP_1 AS (
         SELECT *,
                line_of_therapy - LAG(line_of_therapy) 
                OVER (PARTITION BY patientid ORDER BY line_of_therapy) 
                AS line_of_therapy_1
         FROM TEMP_CH
       ),
       STEP_2 AS (
         SELECT patientid,
                line_name,
                category,
                sub_category,
                line_of_therapy,
                seq_n,
                line_start,
                line_end,
                setting_index,
                line_setting,
                CASE
                  WHEN line_of_therapy_1 > 1 
                  THEN (line_of_therapy - line_of_therapy_1 + 1)
                  ELSE line_of_therapy
                END AS new_line_of_therapy
         FROM STEP_1
       )

       SELECT patientid,
              line_name,
              category,
              sub_category,
              new_line_of_therapy AS line_of_therapy,
              seq_n,
              line_start,
              line_end,
              setting_index,
              line_setting
       FROM STEP_2;",
      self$schema,
      input_table
    )
    
    
    dbGetQuery(self$conn, sql)
    
    
    diff_count <- dbGetQuery(
      self$conn,
      "WITH STEP_1 AS (
         SELECT patientid,
                line_of_therapy,
                category,
                sub_category,
                line_of_therapy - LAG(line_of_therapy) OVER 
                (PARTITION BY patientid ORDER BY line_of_therapy) 
                AS line_of_therapy_1
         FROM TEMP_CH
       ),
       STEP_2 AS (
         SELECT MAX(line_of_therapy_1) AS max_line_of_therapy_1
         FROM STEP_1
       )
       SELECT * FROM STEP_2;"
    )
    
    
    value <- diff_count[1, 1]
    
    
    if (value > 1)
    {
      flag <- FALSE  
    } else
    {
      flag <- TRUE   
    }
  }
  
  
  dbGetQuery(
    self$conn,
    sprintf(
      "DROP TABLE IF EXISTS %s.%s_TEMP_CH;",
      self$schema,
      self$identifier
    )
  )
  
  
  dbGetQuery(
    self$conn,
    sprintf(
      "CREATE TABLE %s.%s_TEMP_CH AS
       WITH STEP_1 AS (
         SELECT patientid,
                line_name,
                category,
                sub_category,
                line_of_therapy,
                ROW_NUMBER() OVER (PARTITION BY patientid, 
                line_of_therapy ORDER BY line_start) AS seq_n,
                line_start,
                line_end,
                setting_index,
                line_setting
         FROM %s.%s
       )
       SELECT patientid,
              line_name,
              category,
              sub_category,
              line_of_therapy,
              seq_n,
              line_start,
              line_end,
              setting_index,
              line_setting
       FROM STEP_1;",
      self$schema,
      self$identifier,
      self$schema,
      input_table
    )
  )
  
  
  dbGetQuery(self$conn,
             sprintf("DROP TABLE IF EXISTS %s.%s", self$schema, input_table))
  
  dbGetQuery(
    self$conn,
    sprintf(
      "CREATE TABLE %s.%s AS
       SELECT * FROM %s.%s_TEMP_CH",
      self$schema,
      input_table,
      self$schema,
      self$identifier
    )
  )
},

fix_LOT = function(input_table)
{
  df <- read_excel(self$file_path, sheet = "Fix_LOT")
  
  for (c in unique(df$Fix))
  {
    filtered_df <- df %>% filter(Fix == c)
    col <- filtered_df$Column
    regimen_a <- filtered_df$Preceding_Regimen
    regimen_a <- strsplit(regimen_a, split = ',')
    regimen_a <- unlist(regimen_a)
    
    
    
    permutations_matrix <- permutations(length(regimen_a),
                                        length(regimen_a), regimen_a)
    
   
    collapsed_permutations_a <- apply(permutations_matrix, 1,
                                      function(row)
                                        paste(row, collapse = ","))
    collapsed_permutations_a <- unlist(collapsed_permutations_a)
    
    items_a <- paste(collapsed_permutations_a, collapse = "|")
    items_a <- gsub(" ", "", items_a)
    
    
    regimen_b <- filtered_df$Regimen_Fix
    regimen_b <- strsplit(regimen_b, split = ',')
    regimen_b <- unlist(regimen_b)
    
    
    permutations_matrix <- permutations(length(regimen_b),
                                        length(regimen_b), regimen_b)
    
    
    collapsed_permutations_b <- apply(permutations_matrix, 1,
                                      function(row)
                                        paste(row, collapse = ","))
    collapsed_permutations_b <- unlist(collapsed_permutations_b)
    
    items_b <- paste(collapsed_permutations_b, collapse = "|")
    items_b <- gsub(" ", "", items_b)
    
    table <- filtered_df$Table
    self$line_fix(
      input_table = filtered_df$Table,
      line_name_1 = items_a,
      line_name_2 = items_b,
      duration = filtered_df$duration,
      col = col
    )
    self$line_adjust(filtered_df$Table)
    print(sprintf(" Fix number %s is now corrected in your l
                  ines of therapy table", c))
  }
}

  )
)
