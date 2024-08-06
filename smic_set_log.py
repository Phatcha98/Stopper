from datetime import datetime
from django.core.management.base import BaseCommand, CommandError
from django.db import connections
from psycopg2.extras import execute_values 
from server.settings import logger
from server.util.custom_base_command import CustomBaseCommand
import fnmatch
import os
import pandas as pd
import psycopg2
import queue
import schedule
import sys

# Config
INPUT_PATH1 = "/home/mnt/10_17_72_74/g/SMT/Data_Reflow_SMIC_set_master"
INPUT_PATH2 = "/home/mnt/10_17_72_74/g/SMT/Data_Reflow_SMIC"

class Command(CustomBaseCommand):
   
    @logger.catch
    def run(self, INPUT_PATH1):

        logger.log("START", None)

        input_list1 = os.listdir(INPUT_PATH1) 
        for folder1 in input_list1:
            os.path.exists(os.path.join(INPUT_PATH1, folder1))
            for folder2 in os.listdir(os.path.join(INPUT_PATH1, folder1)): 
                if not os.path.exists(os.path.join(INPUT_PATH1, folder1, folder2)):
                    continue
                
            file_list = fnmatch.filter(os.listdir(os.path.join(INPUT_PATH1, folder1, folder2)), "*.PGM")
            for file_name in file_list: # Process the combined list
                if file_name == "NoName.PGM" or file_name == "ECOMode.PGM":
                    continue
                else:
                    dfs = []
                    file_path = os.path.join(INPUT_PATH1, folder1, folder2, file_name)
                    Modifi_date = (datetime.fromtimestamp(os.stat(os.path.join(file_path)).st_mtime))
                    time_file = Modifi_date.strftime('%Y-%m-%d %H:%M:%S')
                    # Assuming .PGM file is a text file
                    with open(file_path, "r") as file:
                        lines = file.readlines()
                    # Remove any leading/trailing whitespace from each line
                    lines = [line.strip() for line in lines]
                    # Find the index where the numeric data starts
                    data_start_index = next((index for index, line in enumerate(lines) if line.isdigit()), None)
                    # Extract the numeric data from the file, starting from the data_start_index
                    numeric_data = [line for line in lines[data_start_index:] if line.strip()]
                    # Create a DataFrame from the numeric data
                    df = pd.DataFrame({"value": numeric_data})
                    # Define the row mappings based on row indices (0-indexed) in the DataFrame
                    row_mappings = {
                        0: {"title": "upper_heater_zone1", "parameter_code": "A-MPRN014"},
                        1: {"title": "lower_heater_zone1", "parameter_code": "nocode"},
                        
                        2: {"title": "upper_heater_zone2", "parameter_code": "A-MPRN015"},
                        3: {"title": "lower_heater_zone2", "parameter_code": "nocode"},
                        
                        4: {"title": "upper_heater_zone3", "parameter_code": "A-MPRN016"},
                        5: {"title": "lower_heater_zone3", "parameter_code": "nocode"},
                        
                        6: {"title": "upper_heater_zone4", "parameter_code": "A-MPRN017"},
                        7: {"title": "lower_heater_zone4", "parameter_code": "nocode"},
                        
                        8: {"title": "upper_heater_zone5", "parameter_code": "A-MPRN018"},
                        9: {"title": "lower_heater_zone5", "parameter_code": "nocode"},
                        
                        10: {"title": "upper_heater_zone6", "parameter_code": "A-MPRN019"},
                        11: {"title": "lower_heater_zone6", "parameter_code": "nocode"},
                        
                        12: {"title": "upper_heater_zone7", "parameter_code": "A-MPRN020"},
                        13: {"title": "lower_heater_zone7", "parameter_code": "nocode"},
                        
                        14: {"title": "upper_heater_zone8", "parameter_code": "A-MPRN021"},
                        15: {"title": "lower_heater_zone8", "parameter_code": "nocode"},
            
                        16: {"title": "upper_heater_zone9", "parameter_code": "A-MPRN022"},
                        17: {"title": "lower_heater_zone9", "parameter_code": "nocode"},

                        18: {"title": "upper_heater_zone10", "parameter_code": "A-MPRN023"},
                        19: {"title": "lower_heater_zone10", "parameter_code": "nocode"},

                        30: {"title": "blower_speed_zone1_upper", "parameter_code": "A-MPRN023"},
                        31: {"title": "blower_speed_zone1_lower", "parameter_code": "nocode"},

                        32: {"title": "blower_speed_zone4_upper", "parameter_code": "A-MPRN023"},
                        33: {"title": "blower_speed_zone4_lower", "parameter_code": "nocode"},

                        34: {"title": "blower_speed_zone9_upper", "parameter_code": "A-MPRN023"},
                        35: {"title": "blower_speed_zone9_lower", "parameter_code": "nocode"},

                        36: {"title": "blower_speed_cooling_upper", "parameter_code": "A-MPRN023"},
                        37: {"title": "blower_speed_cooling_lower", "parameter_code": "nocode"},

                        61: {"title": "conveyor_speed", "parameter_code": "A-MPRN024"},
                        63: {"title": "o2_setting", "parameter_code": "A-MPRN025"},
                        74: {"title": "interval_time", "parameter_code": "A-MPRN026"},
                        # Add other row mappings as needed
                    }
                    # Map the rows to the corresponding columns
                    df["title"] = df.index.map(lambda x: row_mappings.get(x, {}).get("title"))
                    df["parameter_code"] = df.index.map(lambda x: row_mappings.get(x, {}).get("parameter_code"))
                    # Drop rows with missing mappings (non-numeric rows)
                    df = df.dropna()
                    # Reorder the columns
                    df = df[["title", "parameter_code", "value"]]
                    # Extract the information from the file path
                    file_path_parts = file_path.split("/")
                    line_no = file_path_parts[-3]
                    machine = file_path_parts[-2]

                    file_name = os.path.splitext(file_name)[0]  
                    update_by1 = file_name.split("_")[1] 
                    update_by = update_by1.split(".")[0] 
                    file_name1 = file_name.split("_")[0]
                    # Add the additional information to the DataFrame
                    df["line_no"] = line_no
                    df["machine"] = machine
                    df["program"] = file_name1
                    # Reorder the columns with the new additions
                    df = df[["line_no", "machine", "program", "title", "parameter_code", "value"]]
                    dfs.append(df)
                    reate_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    final_df = pd.concat(dfs, ignore_index=True)
                    final_df = final_df.drop_duplicates(subset=["line_no", "machine", "program", "title"])
                    pivot_df = final_df.pivot(index=["line_no", "machine", "program"], columns="title", values="value").reset_index()
                    pivot_df["process"] = "ZREF"
                    pivot_df["update_by"] = update_by
                    pivot_df["update_datetime"] = reate_time
                    pivot_df["create_time"] = time_file

                    db = connections["10.17.66.121.iot.smt"].settings_dict
                    with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"]) as conn:
                        with conn.cursor() as cur:
                            # delete_query = "DELETE FROM smt.smt_reflow_smic_set_log_master"
                            # cur.execute(delete_query)
                            # conn.commit()

                            insert_query = """
                                INSERT INTO smt.smt_reflow_smic_set_log_master (
                                    line_no, machine, program, upper_heater_zone1, lower_heater_zone1, upper_heater_zone2, lower_heater_zone2,
                                    upper_heater_zone3, lower_heater_zone3, upper_heater_zone4, lower_heater_zone4, upper_heater_zone5, lower_heater_zone5,
                                    upper_heater_zone6, lower_heater_zone6, upper_heater_zone7, lower_heater_zone7, upper_heater_zone8, lower_heater_zone8,
                                    upper_heater_zone9, lower_heater_zone9, upper_heater_zone10, lower_heater_zone10, conveyor_speed, o2_setting, process,
                                    update_datetime, interval_time, update_by, blower_speed_zone1_upper, blower_speed_zone1_lower, blower_speed_zone4_upper,
                                    blower_speed_zone4_lower, blower_speed_zone9_upper, blower_speed_zone9_lower, blower_speed_cooling_upper, 
                                    blower_speed_cooling_lower, create_time 
                                ) VALUES %s
                                ON CONFLICT (line_no, machine, program, process)
                                DO UPDATE
                                SET upper_heater_zone1 = EXCLUDED.upper_heater_zone1,
                                    lower_heater_zone1 = EXCLUDED.lower_heater_zone1,
                                    upper_heater_zone2 = EXCLUDED.upper_heater_zone2,
                                    lower_heater_zone2 = EXCLUDED.lower_heater_zone2,
                                    upper_heater_zone3 = EXCLUDED.upper_heater_zone3,
                                    lower_heater_zone3 = EXCLUDED.lower_heater_zone3,
                                    upper_heater_zone4 = EXCLUDED.upper_heater_zone4,
                                    lower_heater_zone4 = EXCLUDED.lower_heater_zone4,
                                    upper_heater_zone5 = EXCLUDED.upper_heater_zone5,
                                    lower_heater_zone5 = EXCLUDED.lower_heater_zone5,
                                    upper_heater_zone6 = EXCLUDED.upper_heater_zone6,
                                    lower_heater_zone6 = EXCLUDED.lower_heater_zone6,
                                    upper_heater_zone7 = EXCLUDED.upper_heater_zone7,
                                    lower_heater_zone7 = EXCLUDED.lower_heater_zone7,
                                    upper_heater_zone8 = EXCLUDED.upper_heater_zone8,
                                    lower_heater_zone8 = EXCLUDED.lower_heater_zone8,
                                    upper_heater_zone9 = EXCLUDED.upper_heater_zone9,
                                    lower_heater_zone9 = EXCLUDED.lower_heater_zone9,
                                    upper_heater_zone10 = EXCLUDED.upper_heater_zone10,
                                    lower_heater_zone10 = EXCLUDED.lower_heater_zone10,
                                    conveyor_speed = EXCLUDED.conveyor_speed,
                                    o2_setting = EXCLUDED.o2_setting,
                                    process = EXCLUDED.process,
                                    update_datetime = EXCLUDED.update_datetime,
                                    interval_time = EXCLUDED.interval_time,
                                    update_by = EXCLUDED.update_by,
                                    blower_speed_zone1_upper = EXCLUDED.blower_speed_zone1_upper,
                                    blower_speed_zone1_lower = EXCLUDED.blower_speed_zone1_lower,
                                    blower_speed_zone4_upper = EXCLUDED.blower_speed_zone4_upper,
                                    blower_speed_zone4_lower = EXCLUDED.blower_speed_zone4_lower,
                                    blower_speed_zone9_upper = EXCLUDED.blower_speed_zone9_upper,
                                    blower_speed_zone9_lower = EXCLUDED.blower_speed_zone9_lower,
                                    blower_speed_cooling_upper = EXCLUDED.blower_speed_cooling_upper,
                                    blower_speed_cooling_lower = EXCLUDED.blower_speed_cooling_lower,
                                    create_time = EXCLUDED.create_time
                            """
                            data_values = [(row['line_no'], row['machine'], row['program'], row['upper_heater_zone1'], row['lower_heater_zone1'],
                                row['upper_heater_zone2'], row['lower_heater_zone2'], row['upper_heater_zone3'], row['lower_heater_zone3'],
                                row['upper_heater_zone4'], row['lower_heater_zone4'], row['upper_heater_zone5'], row['lower_heater_zone5'],
                                row['upper_heater_zone6'], row['lower_heater_zone6'], row['upper_heater_zone7'], row['lower_heater_zone7'],
                                row['upper_heater_zone8'], row['lower_heater_zone8'], row['upper_heater_zone9'], row['lower_heater_zone9'], 
                                row['upper_heater_zone10'], row['lower_heater_zone10'], row['conveyor_speed'], row['o2_setting'], row['process'], 
                                row['update_datetime'], row['interval_time'], row['update_by'], row['blower_speed_zone1_upper'], row['blower_speed_zone1_lower'], 
                                row['blower_speed_zone4_upper'], row['blower_speed_zone4_lower'], row['blower_speed_zone9_upper'], row['blower_speed_zone9_lower'], 
                                row['blower_speed_cooling_upper'], row['blower_speed_cooling_lower'], row['create_time'])
                            for _, row in pivot_df.iterrows()]

                        try:
                            with conn.cursor() as cursor:
                                execute_values(cursor, insert_query, data_values)
                            conn.commit()
                        except Exception as ex:
                            print(ex)

                        logger.success(
                            f"commit smt_reflow_smic_set_log_master",
                            input_process=f"input {str(file_path)}",
                            output_process="database table smt_reflow_smic_set_log_master"
                        )
        
        pass
        self.smt_data_reflow_smic_set_log(INPUT_PATH2)
        
        logger.log("STOP", None)

    def smt_data_reflow_smic_set_log(self, INPUT_PATH2):
        # print(f"{timezone.now().strftime('%Y-%m-%d %H:%M:%S')} RUN smt_reflow_smic_set_log")
        input_list2 = os.listdir(INPUT_PATH2)
        dfs = []
        for folder1 in input_list2:
            os.path.exists(os.path.join(INPUT_PATH2, folder1))
            for folder2 in os.listdir(os.path.join(INPUT_PATH2, folder1)): 
                if not os.path.exists(os.path.join(INPUT_PATH2, folder1, folder2, "SET")):
                    continue
            file_list = fnmatch.filter(os.listdir(os.path.join(INPUT_PATH2, folder1, folder2, "SET")), "*.pgm")
            # pattern = os.path.join(INPUT_PATH2, "Line18", folder2, "SET", "RGPZ466.pgm")
            # file_list = glob.glob(pattern)
            for file_name in file_list:  # Process the combined list
                file_path = os.path.join(INPUT_PATH2, folder1, folder2, "SET", file_name)
                Modifi_date = (datetime.fromtimestamp(os.stat(os.path.join(file_path)).st_mtime))
                time_file = Modifi_date.strftime('%Y-%m-%d %H:%M:%S')
                # # print(file_path)
                # Assuming .PGM file is a text file
                with open(file_path, "r") as file:
                    lines = file.readlines()
                # Remove any leading/trailing whitespace from each line
                lines = [line.strip() for line in lines]
                # Find the index where the numeric data starts
                data_start_index = next((index for index, line in enumerate(lines) if line.isdigit()), None)
                # Extract the numeric data from the file, starting from the data_start_index
                numeric_data = [line for line in lines[data_start_index:] if line.strip()]
                # Create a DataFrame from the numeric data
                df = pd.DataFrame({"value": numeric_data})
                # Define the row mappings based on row indices (0-indexed) in the DataFrame
                row_mappings = {
                    0: {"title": "upper_heater_zone1", "parameter_code": "A-MPRN014"},
                    1: {"title": "lower_heater_zone1", "parameter_code": "nocode"},
                    
                    2: {"title": "upper_heater_zone2", "parameter_code": "A-MPRN015"},
                    3: {"title": "lower_heater_zone2", "parameter_code": "nocode"},
                    
                    4: {"title": "upper_heater_zone3", "parameter_code": "A-MPRN016"},
                    5: {"title": "lower_heater_zone3", "parameter_code": "nocode"},
                    
                    6: {"title": "upper_heater_zone4", "parameter_code": "A-MPRN017"},
                    7: {"title": "lower_heater_zone4", "parameter_code": "nocode"},
                    
                    8: {"title": "upper_heater_zone5", "parameter_code": "A-MPRN018"},
                    9: {"title": "lower_heater_zone5", "parameter_code": "nocode"},
                    
                    10: {"title": "upper_heater_zone6", "parameter_code": "A-MPRN019"},
                    11: {"title": "lower_heater_zone6", "parameter_code": "nocode"},
                    
                    12: {"title": "upper_heater_zone7", "parameter_code": "A-MPRN020"},
                    13: {"title": "lower_heater_zone7", "parameter_code": "nocode"},
                    
                    14: {"title": "upper_heater_zone8", "parameter_code": "A-MPRN021"},
                    15: {"title": "lower_heater_zone8", "parameter_code": "nocode"},
                    
                    16: {"title": "upper_heater_zone9", "parameter_code": "A-MPRN022"},
                    17: {"title": "lower_heater_zone9", "parameter_code": "nocode"},

                    18: {"title": "upper_heater_zone10", "parameter_code": "A-MPRN023"},
                    19: {"title": "lower_heater_zone10", "parameter_code": "nocode"},

                    30: {"title": "blower_speed_zone1_upper", "parameter_code": "A-MPRN023"},
                    31: {"title": "blower_speed_zone1_lower", "parameter_code": "nocode"},

                    32: {"title": "blower_speed_zone4_upper", "parameter_code": "A-MPRN023"},
                    33: {"title": "blower_speed_zone4_lower", "parameter_code": "nocode"},

                    34: {"title": "blower_speed_zone9_upper", "parameter_code": "A-MPRN023"},
                    35: {"title": "blower_speed_zone9_lower", "parameter_code": "nocode"},

                    36: {"title": "blower_speed_cooling_upper", "parameter_code": "A-MPRN023"},
                    37: {"title": "blower_speed_cooling_lower", "parameter_code": "nocode"},
                    
                    61: {"title": "conveyor_speed", "parameter_code": "A-MPRN024"},
                    63: {"title": "o2_setting", "parameter_code": "A-MPRN025"},
                    74: {"title": "interval_time", "parameter_code": "A-MPRN026"},
                    # Add other row mappings as needed
                }
                # Map the rows to the corresponding columns
                df["title"] = df.index.map(lambda x: row_mappings.get(x, {}).get("title"))
                df["parameter_code"] = df.index.map(lambda x: row_mappings.get(x, {}).get("parameter_code"))
                # Drop rows with missing mappings (non-numeric rows)
                df = df.dropna()
                # Reorder the columns
                df = df[["title", "parameter_code", "value"]]
                # Extract the information from the file path
                file_path_parts = file_path.split("/")
                line_no = file_path_parts[-4]
                machine = file_path_parts[-3]
                file_name = os.path.splitext(file_name)[0] 
                # file_name_pg =  file_name.split("/")[10]
                 # Remove the file extension from the file name
                # Add the additional information to the DataFrame
                df["line_no"] = line_no
                df["machine"] = machine
                df["program"] = file_name
                # Reorder the columns with the new additions
                df = df[["line_no", "machine", "program", "title", "parameter_code", "value"]]
                # Append the processed DataFrame to the list
                dfs.append(df)
                # Concatenate all DataFrames into a single DataFrame
                final_df = pd.concat(dfs, ignore_index=True)
            reate_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # ทำการ pivot ข้อมูลใน final_df
            pivot_df = final_df.pivot(index=["line_no", "machine", "program"], columns="title", values="value").reset_index()
            pivot_df["process"] = "ZREF"
            pivot_df["update_datetime"] = reate_time
            pivot_df["create_time"] = time_file

            db = connections["10.17.66.121.iot.smt"].settings_dict
            with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"]) as conn:
                with conn.cursor() as cur:
                        # delete_query = "DELETE FROM smt.smt_reflow_smic_set_log_master"
                        # cur.execute(delete_query)
                        # conn.commit()

                        insert_query = """
                            INSERT INTO smt.smt_reflow_smic_set_log (
                                line_no, machine, program, upper_heater_zone1, lower_heater_zone1, upper_heater_zone2, lower_heater_zone2,
                                upper_heater_zone3, lower_heater_zone3, upper_heater_zone4, lower_heater_zone4, upper_heater_zone5, lower_heater_zone5,
                                upper_heater_zone6, lower_heater_zone6, upper_heater_zone7, lower_heater_zone7, upper_heater_zone8, lower_heater_zone8,
                                upper_heater_zone9, lower_heater_zone9, upper_heater_zone10, lower_heater_zone10, conveyor_speed, o2_setting, process,
                                update_datetime, interval_time, blower_speed_zone1_upper, blower_speed_zone1_lower, blower_speed_zone4_upper,
                                blower_speed_zone4_lower, blower_speed_zone9_upper, blower_speed_zone9_lower, blower_speed_cooling_upper, 
                                blower_speed_cooling_lower, create_time 
                            ) VALUES %s
                            ON CONFLICT (line_no, machine, program, process)
                            DO UPDATE
                            SET upper_heater_zone1 = EXCLUDED.upper_heater_zone1,
                                lower_heater_zone1 = EXCLUDED.lower_heater_zone1,
                                upper_heater_zone2 = EXCLUDED.upper_heater_zone2,
                                lower_heater_zone2 = EXCLUDED.lower_heater_zone2,
                                upper_heater_zone3 = EXCLUDED.upper_heater_zone3,
                                lower_heater_zone3 = EXCLUDED.lower_heater_zone3,
                                upper_heater_zone4 = EXCLUDED.upper_heater_zone4,
                                lower_heater_zone4 = EXCLUDED.lower_heater_zone4,
                                upper_heater_zone5 = EXCLUDED.upper_heater_zone5,
                                lower_heater_zone5 = EXCLUDED.lower_heater_zone5,
                                upper_heater_zone6 = EXCLUDED.upper_heater_zone6,
                                lower_heater_zone6 = EXCLUDED.lower_heater_zone6,
                                upper_heater_zone7 = EXCLUDED.upper_heater_zone7,
                                lower_heater_zone7 = EXCLUDED.lower_heater_zone7,
                                upper_heater_zone8 = EXCLUDED.upper_heater_zone8,
                                lower_heater_zone8 = EXCLUDED.lower_heater_zone8,
                                upper_heater_zone9 = EXCLUDED.upper_heater_zone9,
                                lower_heater_zone9 = EXCLUDED.lower_heater_zone9,
                                upper_heater_zone10 = EXCLUDED.upper_heater_zone10,
                                lower_heater_zone10 = EXCLUDED.lower_heater_zone10,
                                conveyor_speed = EXCLUDED.conveyor_speed,
                                o2_setting = EXCLUDED.o2_setting,
                                process = EXCLUDED.process,
                                update_datetime = EXCLUDED.update_datetime,
                                interval_time = EXCLUDED.interval_time,
                                update_by = EXCLUDED.update_by,
                                blower_speed_zone1_upper = EXCLUDED.blower_speed_zone1_upper,
                                blower_speed_zone1_lower = EXCLUDED.blower_speed_zone1_lower,
                                blower_speed_zone4_upper = EXCLUDED.blower_speed_zone4_upper,
                                blower_speed_zone4_lower = EXCLUDED.blower_speed_zone4_lower,
                                blower_speed_zone9_upper = EXCLUDED.blower_speed_zone9_upper,
                                blower_speed_zone9_lower = EXCLUDED.blower_speed_zone9_lower,
                                blower_speed_cooling_upper = EXCLUDED.blower_speed_cooling_upper,
                                blower_speed_cooling_lower = EXCLUDED.blower_speed_cooling_lower,
                                create_time = EXCLUDED.create_time
                        """
                        data_values = [(row['line_no'], row['machine'], row['program'], row['upper_heater_zone1'], row['lower_heater_zone1'],
                            row['upper_heater_zone2'], row['lower_heater_zone2'], row['upper_heater_zone3'], row['lower_heater_zone3'],
                            row['upper_heater_zone4'], row['lower_heater_zone4'], row['upper_heater_zone5'], row['lower_heater_zone5'],
                            row['upper_heater_zone6'], row['lower_heater_zone6'], row['upper_heater_zone7'], row['lower_heater_zone7'],
                            row['upper_heater_zone8'], row['lower_heater_zone8'], row['upper_heater_zone9'], row['lower_heater_zone9'], 
                            row['upper_heater_zone10'], row['lower_heater_zone10'], row['conveyor_speed'], row['o2_setting'], row['process'], 
                            row['update_datetime'], row['interval_time'], row['blower_speed_zone1_upper'], row['blower_speed_zone1_lower'], 
                            row['blower_speed_zone4_upper'], row['blower_speed_zone4_lower'], row['blower_speed_zone9_upper'], row['blower_speed_zone9_lower'], 
                            row['blower_speed_cooling_upper'], row['blower_speed_cooling_lower'], row['create_time'])
                        for _, row in pivot_df.iterrows()]
                        try:
                            with conn.cursor() as cursor:
                                execute_values(cursor, insert_query, data_values)
                            conn.commit()
                        except Exception as ex:
                            print(ex)
                        
                        logger.success(
                            f"commit smt_reflow_smic_set_log",
                            input_process=f"input {str(file_path)}",
                            output_process=f"database table smt_reflow_smic_set_log"
                        )
                # print(f"{timezone.now().strftime('%Y-%m-%d %H:%M:%S')} END smt_reflow_smic_set_log")
        pass
        self.analysis_data()

    def analysis_data(self):

        db = connections["10.17.66.121.iot.smt"].settings_dict
        with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"]) as conn:
            with conn.cursor() as cur:
                try:

                    check_query = """
                        SELECT
                            log.line_no,
                            log.machine,
                            log.program,
                            log.process,
                            log_master.update_by,
                            CASE
                                WHEN 
                                    log.line_no = log_master.line_no AND
                                    log.machine = log_master.machine AND
                                    log.program = log_master.program AND
                                    log.upper_heater_zone1 = log_master.upper_heater_zone1 AND
                                    log.lower_heater_zone1 = log_master.lower_heater_zone1 AND
                                    log.upper_heater_zone2 = log_master.upper_heater_zone2 AND
                                    log.lower_heater_zone2 = log_master.lower_heater_zone2 AND
                                    log.upper_heater_zone3 = log_master.upper_heater_zone3 AND
                                    log.lower_heater_zone3 = log_master.lower_heater_zone3 AND
                                    log.upper_heater_zone4 = log_master.upper_heater_zone4 AND
                                    log.lower_heater_zone4 = log_master.lower_heater_zone4 AND
                                    log.upper_heater_zone5 = log_master.upper_heater_zone5 AND
                                    log.lower_heater_zone5 = log_master.lower_heater_zone5 AND
                                    log.upper_heater_zone6 = log_master.upper_heater_zone6 AND
                                    log.lower_heater_zone6 = log_master.lower_heater_zone6 AND
                                    log.upper_heater_zone7 = log_master.upper_heater_zone7 AND
                                    log.lower_heater_zone7 = log_master.lower_heater_zone7 AND
                                    log.upper_heater_zone8 = log_master.upper_heater_zone8 AND
                                    log.lower_heater_zone8 = log_master.lower_heater_zone8 AND
                                    log.upper_heater_zone9 = log_master.upper_heater_zone9 AND
                                    log.lower_heater_zone9 = log_master.lower_heater_zone9 AND
                                    log.upper_heater_zone10 = log_master.upper_heater_zone10 AND
                                    log.lower_heater_zone10 = log_master.lower_heater_zone10 AND
                                    log.conveyor_speed = log_master.conveyor_speed AND
                                    log.o2_setting = log_master.o2_setting AND
                                    log.interval_time = log_master.interval_time AND 
                                    log.blower_speed_zone1_upper = log_master.blower_speed_zone1_upper AND 
                                    log.blower_speed_zone1_lower = log_master.blower_speed_zone1_lower AND 
                                    log.blower_speed_zone4_upper = log_master.blower_speed_zone4_upper AND 
                                    log.blower_speed_zone4_lower = log_master.blower_speed_zone4_lower AND 
                                    log.blower_speed_zone9_upper = log_master.blower_speed_zone9_upper AND 
                                    log.blower_speed_zone9_lower = log_master.blower_speed_zone9_lower AND 
                                    log.blower_speed_cooling_upper = log_master.blower_speed_cooling_upper AND 
                                    log.blower_speed_cooling_lower = log_master.blower_speed_cooling_lower
                                THEN  0
                                ELSE 1
                            END AS verify 
                        FROM
                            smt.smt_reflow_smic_set_log AS log
                        FULL JOIN smt.smt_reflow_smic_set_log_master AS log_master
                            ON log.line_no = log_master.line_no
                            AND log.machine = log_master.machine
                            AND log.program = log_master.program;
                    """
                    cur.execute(check_query)
                    result = cur.fetchall()
                    df = pd.DataFrame(result, columns=["line_no", "machine", "program", "process", "update_by", "verify"])
                    df = df[["line_no", "machine", "program", "process", "verify", "update_by"]]

                    for index, row in df.iterrows():
                        if row['line_no'] is not None:  
                            update_query = """
                            INSERT INTO smt.smt_reflow_smic_set_log (line_no, machine, program, process, verify, update_by)
                                VALUES (%(line_no)s, %(machine)s, %(program)s::text, %(process)s, %(verify)s, %(update_by)s)
                                ON CONFLICT (line_no, machine, program, process)
                                DO UPDATE SET verify = EXCLUDED.verify, update_by = EXCLUDED.update_by; """

                            cur.execute(update_query, row)
                            conn.commit()

                    logger.success(
                        f"commit smt_reflow_smic_set_log",
                        input_process=f"intput {str(row)}",
                        output_process="database smt.smt_reflow_smic_set_log"
                    )
                except Exception as ex:
                    logger.exception(ex)

    def handle(self, *args, **options):        
        self.run(INPUT_PATH1)
        schedule.every(1).minutes.do(self.jobqueue.put, lambda: self.run(INPUT_PATH1))
        self.run_schedule(schedule)