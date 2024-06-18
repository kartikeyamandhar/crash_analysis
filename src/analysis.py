import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, collect_set, row_number
from pyspark.sql.window import Window

class CrashAnalysis:
    def __init__(self, spark):
        self.spark = spark

    def analysis_1(self, df):
        """
        Number of crashes where more than 2 males were killed.

        Approach:
        - Filter the DataFrame to include only rows where the gender is 'MALE' and the death count is greater than 2.
        - Count the distinct crash IDs that meet these criteria.

        Input: 
        - DataFrame containing crash data with columns 'PRSN_GNDR_ID' and 'DEATH_CNT'.

        Output: 
        - Integer representing the number of crashes where more than 2 males were killed.
        """
        try:
            males_killed = df.filter((col('PRSN_GNDR_ID') == 'MALE') & (col('DEATH_CNT') > 2))
            result = males_killed.select('CRASH_ID').distinct().count()
            logging.info("Analysis 1 completed successfully.")
            return result
        except Exception as e:
            logging.error(f"Error in Analysis 1: {e}")
            raise

    def analysis_2(self, df):
        """
        Number of two-wheelers booked for crashes.

        Approach:
        - Filter the DataFrame to include only rows where the vehicle body style contains 'MOTORCYCLE'.
        - Count the distinct crash IDs that meet these criteria.

        Input: 
        - DataFrame containing crash data with column 'VEH_BODY_STYL_ID'.

        Output: 
        - Integer representing the number of two-wheelers booked for crashes.
        """
        df.select('VEH_BODY_STYL_ID').distinct().collect()
        # This displays the distinct values, we see MOTORCYCLE as our choice needed

        try:
            two_wheelers = df.filter(col('VEH_BODY_STYL_ID').contains('MOTORCYCLE'))
            result = two_wheelers.select('CRASH_ID').distinct().count()
            logging.info("Analysis 2 completed successfully.")
            return result
        except Exception as e:
            logging.error(f"Error in Analysis 2: {e}")
            raise

    def analysis_3(self, df_units, df_primary_person):
        """
        Top 5 vehicle makes where the driver died and airbags did not deploy.

        Approach:
        - Filter the primary person DataFrame to include only rows where the person type is 'DRIVER', injury severity 
        is 'KILLED', death count is greater than 0, and airbags were not deployed.
        - Join the filtered DataFrame with the units DataFrame on 'CRASH_ID'.
        - Group by 'VEH_MAKE_ID' and count the occurrences, then order by count in descending order and take the top 5.

        Input: 
        - DataFrames containing crash data with relevant columns including 'PRSN_TYPE_ID', 'PRSN_INJRY_SEV_ID', 'DEATH_CNT', 'PRSN_AIRBAG_ID', and 'VEH_MAKE_ID'.

        Output: 
        - List of top 5 vehicle makes where the driver died and airbags did not deploy.

        Comments:
        - We will first filter person type ID with 'DRIVER' since 'DRIVER OF MOTORCYCLE TYPE VEHICLE' (which is one of the values found upon exploring) cannot hold 
        valid since motorcycles do not have airbags
        - After filtering we will join with person desc dataframe and join with CRASH_ID however to find the make we 
        have to remove the not valid values. We can use both PRSN_INJRY_SEV_ID and DEATH_CNT > 0 to check for death, using both will result in the same answer as using either.

        """
        try:
            filtered_primary_person = df_primary_person.filter((col('PRSN_TYPE_ID') == 'DRIVER') & 
                                                               (col("PRSN_INJRY_SEV_ID") == "KILLED") &
                                                               (col('DEATH_CNT') > 0) & 
                                                               (col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED'))
            drivers_killed = df_units.join(filtered_primary_person, 'CRASH_ID')
            result = drivers_killed.filter(col('VEH_MAKE_ID') != 'NA') \
                                   .groupBy('VEH_MAKE_ID').count() \
                                   .orderBy(col('count').desc()).limit(5).collect()
            logging.info("Analysis 3 completed successfully.")
            return result
        except Exception as e:
            logging.error(f"Error in Analysis 3: {e}")
            raise

    def analysis_4(self, df_units, df_primary_person):
        """
        Number of vehicles with drivers having valid licenses involved in hit-and-run cases.

        Approach:
        - Filter the primary person DataFrame to exclude rows where the driver license type is 'NA' or 'UNKNOWN'.
        - Filter the units DataFrame to include only rows where the hit-and-run flag is 'Y'.
        - Join the filtered DataFrames on 'CRASH_ID'.
        - Count the distinct crash IDs that meet these criteria.

        Input: 
        - DataFrames containing crash data with relevant columns including 'DRVR_LIC_TYPE_ID' and 'VEH_HNR_FL'.

        Output: 
        - Integer representing the number of vehicles with drivers having valid licenses involved in hit-and-run cases.

        Comments:
        - The thought process is that first, we are going to filter and then join
        Out of the license types, we filter out anything that is NA and UNKNOWN, 
        we keep OTHERS (which is one of the values found upon exploring) as technically that is still licensed, 
        For UNKNOWN we see most of the other data is also unknown.
        """
        try:
            valid_licence = df_primary_person.filter(~df_primary_person.DRVR_LIC_TYPE_ID.isin(['NA','UNKNOWN']))
            hit_and_run = df_units.filter(col('VEH_HNR_FL')=='Y')
            valid_licence_hit_and_run = valid_licence.join(hit_and_run, 'CRASH_ID')
            result = valid_licence_hit_and_run.select('CRASH_ID').distinct().count()
            logging.info("Analysis 4 completed successfully.")
            return result
        except Exception as e:
            logging.error(f"Error in Analysis 4: {e}")
            raise

    def analysis_5(self, df):
        """
        State with the highest number of accidents where females were not involved.

        Approach:
        - Filter the DataFrame to exclude rows where the gender is 'FEMALE'.
        - Group by 'VEH_LIC_STATE_ID' and count the occurrences, then order by count in descending order and take the first result.

        Input: 
        - DataFrame containing crash data with column 'PRSN_GNDR_ID'.

        Output: 
        - Dictionary with the state and count of the highest number of accidents where females were not involved.
        """
        try:
            accidents_no_females = df.filter(col('PRSN_GNDR_ID') != 'FEMALE')
            state_accidents = accidents_no_females.groupBy('DRVR_LIC_STATE_ID').count().orderBy(col('count').desc()).first()
            result = {"state": state_accidents['DRVR_LIC_STATE_ID'], "count": state_accidents['count']}
            logging.info("Analysis 5 completed successfully.")
            return result
        except Exception as e:
            logging.error(f"Error in Analysis 5: {e}")
            raise

    def analysis_6(self, df):
        """
        3rd to 5th vehicle makes contributing to the largest number of injuries including death.

        Approach:
        - Filter the DataFrame to include only rows where the total injury count or death count is greater than 0.
        - Group by 'VEH_MAKE_ID' and count the occurrences, then order by count in descending order and take the 3rd to 5th results.

        Input: 
        - DataFrame containing crash data with columns 'TOT_INJRY_CNT' and 'DEATH_CNT'.

        Output: 
        - List of dictionaries representing the 3rd to 5th vehicle makes contributing to the largest number of injuries including death.
        
        Comments:
        -      
        Grouping the data by injury severity and collecting unique counts, the analysis 
        provides insights into the distribution and relationship between injury severity and the number of injuries and 
        deaths in the dataset.
        
        The above analysis was done with a hypothesis, that when we group by only the different injuries (Not Killed)
        The corresponding value in the TOT_INJRY_CNT column should be 1, and in DEATH_CNT should be 0, and vice-versa 
        should be true for Deaths (Not Injuries)
        The exploration proved to be right, so for our logic we have to select either injuries or deaths, 
        having values not equal to 0
       
        """

        try:
            injuries_deaths = df.filter((col('TOT_INJRY_CNT') > 0) | (col('DEATH_CNT') > 0))
            top_vehicle_makes = injuries_deaths.groupBy('VEH_MAKE_ID').count().orderBy(col('count').desc()).take(5)
            result = [{"VEH_MAKE_ID": row["VEH_MAKE_ID"], "count": row["count"]} for row in top_vehicle_makes[2:5]]
            logging.info("Analysis 6 completed successfully.")
            return result
        except Exception as e:
            logging.error(f"Error in Analysis 6: {e}")
            raise

    def analysis_7(self, df_primary_person, df_units):
        """
        Top ethnic user group for each body style involved in crashes.

        Approach:
        - Join the primary person DataFrame with the units DataFrame on 'CRASH_ID'.
        - Group by 'VEH_BODY_STYL_ID' and 'PRSN_ETHNICITY_ID', count the occurrences, and then use a window function to rank the ethnic groups for each body style.
        - Select the top-ranked ethnic group for each body style.

        Input: 
        - DataFrames containing crash data with relevant columns including 'VEH_BODY_STYL_ID' and 'PRSN_ETHNICITY_ID'.

        Output: 
        - List of dictionaries representing the top ethnic user group for each body style involved in crashes.
        """
        try:
            ethnic_body_style = df_primary_person.join(df_units, 'CRASH_ID')
            window_spec = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col('count').desc())
            top_ethnic_groups = ethnic_body_style.groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID').count() \
                                                 .withColumn('rank', row_number().over(window_spec)) \
                                                 .filter(col('rank') == 1) \
                                                 .select('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID', 'count')
            result = [{"VEH_BODY_STYL_ID": row["VEH_BODY_STYL_ID"], "PRSN_ETHNICITY_ID": row["PRSN_ETHNICITY_ID"], "count": row["count"]} for row in top_ethnic_groups.collect()]
            logging.info("Analysis 7 completed successfully.")
            return result
        except Exception as e:
            logging.error(f"Error in Analysis 7: {e}")
            raise

    def analysis_8(self, df_primary_person, df_units):
        """
        Top 5 zip codes with crashes where alcohol was a contributing factor.

        Approach:
        - Filter the primary person DataFrame to include only rows where the alcohol result is 'Positive'.
        - Join the filtered DataFrame with the units DataFrame on 'CRASH_ID'.
        - Filter out rows where 'DRVR_ZIP' is null.
        - Group by 'DRVR_ZIP' and count the occurrences, then order by count in descending order and take the top 5 results.

        Input: 
        - DataFrames containing crash data with relevant columns including 'PRSN_ALC_RSLT_ID' and 'DRVR_ZIP'.

        Output: 
        - List of dictionaries representing the top 5 zip codes with crashes where alcohol was a contributing factor.

        Comments:
        - The question says to check alcohol as contributing factor, at first thought the impulse
        is to check for the three columns CONTRIB_FACTR_1_ID,CONTRIB_FACTR_2_ID and CONTRIB_FACTR_P1_ID
        as it has the word contributing in it in the Data Dictionary excel, however, a more robust approach would be 
        to check for PRSN_ALC_RSLT_ID column that states alcohol test as Positive or Negative
        """
        try:
            filtered_units = df_primary_person.filter(df_primary_person['PRSN_ALC_RSLT_ID'] == 'Positive')
            joined_df = filtered_units.join(df_units, "CRASH_ID").filter(col('DRVR_ZIP').isNotNull())
            result = joined_df.groupBy('DRVR_ZIP').count().orderBy('count', ascending=False).limit(5).collect()
            result_dict = [{"DRVR_ZIP": row["DRVR_ZIP"], "count": row["count"]} for row in result]


            logging.info("Analysis 8 completed successfully.")
            return result_dict
        except Exception as e:
            logging.error(f"Error in Analysis 8: {e}")
            raise

    def analysis_9(self, df_units, df_damages):
        """
        Count of distinct crash IDs where no damaged property was observed, damage level is above 4, and the car is insured.

        Approach:
        - Filter the damages DataFrame to include only rows where the damaged property is 'NONE' or 'NONE1'.
        - Filter the units DataFrame to include only rows where the damage scale is above 4 and the insurance type is 'PROOF OF LIABILITY INSURANCE'.
        - Join the filtered DataFrames on 'CRASH_ID'.
        - Count the distinct crash IDs that meet these criteria.

        Input: 
        - DataFrames containing crash data with relevant columns including 'DAMAGED_PROPERTY', 'VEH_DMAG_SCL_1_ID', and 'FIN_RESP_TYPE_ID'.

        Output: 
        - Integer representing the count of distinct crash IDs where no damaged property was observed, the damage level is above 4, and the car is insured.
        """
        try:
            damages_none = df_damages.filter(df_damages['DAMAGED_PROPERTY'].isin(['NONE','NONE1']))
            filtered_units = df_units.filter(
                (df_units['VEH_DMAG_SCL_1_ID'].isin([ 'DAMAGED 5','DAMAGED 7 HIGHEST','DAMAGED 6'])) |
                (df_units['VEH_DMAG_SCL_2_ID'].isin([ 'DAMAGED 5','DAMAGED 7 HIGHEST','DAMAGED 6'])) &
                #Checking OR condition for Damage Levels as both represent damage accoridng to the question and the 
                #Data Disctionary excel
                (df_units['FIN_RESP_TYPE_ID'] == 'PROOF OF LIABILITY INSURANCE')
            )
            filtered_units = filtered_units.join(damages_none, 'CRASH_ID')
            result = filtered_units.select('CRASH_ID').distinct().count()
            logging.info("Analysis 9 completed successfully.")
            return result
        except Exception as e:
            logging.error(f"Error in Analysis 9: {e}")
            raise

    def analysis_10(self, df_units, df_primary_person, df_charges):
        """
        Top 5 vehicle makes where drivers are charged with speeding, have licenses, used top 10 vehicle colors, and are licensed in the top 25 states with the highest number of offenses.

        Approach:
        - Identify the top 10 vehicle colors and top 25 states with the highest number of offenses.
        - Filter the primary person DataFrame to include only rows where the driver license type is not 'NA' or 'UNKNOWN'.
        - Filter the charges DataFrame to include only rows where the charge contains 'SPEED'.
        - Join the filtered DataFrames on 'CRASH_ID'.
        - Filter the resulting DataFrame based on the top vehicle colors and states.
        - Group by 'VEH_MAKE_ID' and count the occurrences, then order by count in descending order and take the top 5 results.

        Input: 
        - DataFrames containing crash data with relevant columns including 'VEH_COLOR_ID', 'VEH_LIC_STATE_ID', 'DRVR_LIC_TYPE_ID', and 'CHARGE'.

        Output: 
        - List of dictionaries representing the top 5 vehicle makes where drivers are charged with speeding, have licenses, used top 10 vehicle colors, and are licensed in the top 25 states with the highest number of offenses.
        """
        try:
            top_25_state_list = [
                row[0]
                for row in df_units.filter(col("VEH_LIC_STATE_ID").cast("int").isNull())
                                   .groupby("VEH_LIC_STATE_ID").count()
                                   .orderBy(col("count").desc()).limit(25).collect()
            ]
            top_10_used_vehicle_colors = [
                row[0]
                for row in df_units.filter(df_units.VEH_COLOR_ID != "NA")
                                   .groupby("VEH_COLOR_ID").count()
                                   .orderBy(col("count").desc()).limit(10).collect()
            ]
            licensed_drivers = df_primary_person.filter(~df_primary_person.DRVR_LIC_TYPE_ID.isin(['NA','UNKNOWN']))
            filtered_charges = df_charges.filter(df_charges.CHARGE.contains('SPEED'))
            master_df = filtered_charges.join(licensed_drivers, 'CRASH_ID').join(df_units, 'CRASH_ID')
            master_df = master_df.filter(master_df.VEH_COLOR_ID.isin(top_10_used_vehicle_colors))
            master_df = master_df.filter(master_df.VEH_LIC_STATE_ID.isin(top_25_state_list))
            result = master_df.groupby("VEH_MAKE_ID").count().orderBy(col("count").desc()).limit(5).collect()
            logging.info("Analysis 10 completed successfully.")
            return result
        except Exception as e:
            logging.error(f"Error in Analysis 10: {e}")
            raise


