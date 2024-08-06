from datetime import datetime
from django.core.management.base import BaseCommand, CommandError
from django.db import connections
from psycopg2.extras import execute_values 
from server.settings import logger
from server.util.custom_base_command import CustomBaseCommand
import fnmatch
import os
import pandas as pd
import psutil
import psycopg2
import queue
import schedule
import sys

# Config
INPUT_PATH1 = "/home/mnt/10_17_72_74/g/SMT/Data_Refow_Tamura_set_master"
INPUT_PATH2 = "/home/mnt/10_17_72_74/g/SMT/Data_Reflow_Tamura"

class Command(CustomBaseCommand):

    @logger.catch
    def run(self, INPUT_PATH1):
        logger.log("START", None)
        
        # print(f"{timezone.now().strftime('%Y-%m-%d %H:%M:%S')} RUN smt_reflow_tamura_set_log_master")

        # db = connections["iot_joywatcher_data_log"].settings_dict
        # conn_db = psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"])

        start_time = datetime.now()
        memory = psutil.virtual_memory()

        input_list1 = os.listdir(INPUT_PATH1)
        result_list1= []  
        for folder1 in input_list1:
            os.path.exists(os.path.join(INPUT_PATH1, folder1))
            for folder2 in os.listdir(os.path.join(INPUT_PATH1, folder1)): 
                if not os.path.exists(os.path.join(INPUT_PATH1, folder1, folder2)):
                    continue
                file_list = fnmatch.filter(os.listdir(os.path.join(INPUT_PATH1, folder1, folder2)), "*.RPF")
                for file_name in file_list:  # Process the combined list
                    if file_name == "NoName.RPF" or file_name == "ECOMode.RPF" or file_name == "ECOMode.rpf":
                        # Handle the case where the latest file matches one of the specified names
                        pass
                    else:
                        file_path = os.path.join(INPUT_PATH1, folder1, folder2, file_name)
                        Modifi_date = (datetime.fromtimestamp(os.stat(os.path.join(file_path)).st_mtime))
                        time_file = Modifi_date.strftime('%Y-%m-%d %H:%M:%S')

                        # Extract "Line" and "Machine" from the file path
                        line_machine = file_path.split('/')[-3:-1]
                        line, machine = line_machine

                        # Extract "Program" from the file name
                        program_update_by = file_name.split('.')[0]
                        program = program_update_by.split('_')[0]
                        update_by = program_update_by.split('_')[1]

                        # Read the data from the file into a DataFrame
                        with open(file_path, 'r') as file:
                            raw_data = file.read()
                            lines = raw_data.strip().split('\n')

                        rows = lines[:17]

                        table = []
                        for row in rows:
                            data = row.split(',')
                            data = [item.strip('\"') for item in data]
                            if len(data) > 1 and '####' in data[0]:
                                data[0] = data[0].split('#### ')[1].rstrip('#').strip()
                            else:
                                continue
                            table.append(data)

                        reate_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        # Create a DataFrame
                        column_headers = [str(i) for i in range(len(table[0]))]
                        df = pd.DataFrame(table[1:], columns=column_headers)
                        # Create a new table with added "Line," "Machine," "Program," and other columns
                        clean_result = pd.DataFrame({
                            'line': [line],
                            'machine': [machine],
                            'program': [program],
                            'upper_heater_zone1': [df.loc[0, '1']],
                            'lower_heater_zone1': [df.loc[0, '2']],
                            'upper_heater_zone2': [df.loc[0, '3']],
                            'lower_heater_zone2': [df.loc[0, '4']],
                            'upper_heater_zone3': [df.loc[0, '5']],
                            'lower_heater_zone3': [df.loc[0, '6']],
                            'upper_heater_zone4': [df.loc[0, '7']],
                            'lower_heater_zone4': [df.loc[0, '8']],
                            'upper_heater_zone5': [df.loc[1, '1']],
                            'lower_heater_zone5': [df.loc[1, '2']],
                            'upper_heater_zone6': [df.loc[1, '3']],
                            'lower_heater_zone6': [df.loc[1, '4']],
                            'upper_heater_zone7': [df.loc[1, '5']],
                            'lower_heater_zone7': [df.loc[1, '6']],
                            'conveyor_m_per_min': [df.loc[1, '7']],
                            'o2_ppm': [df.loc[2, '1']],
                            'update_datetime': [reate_time],
                            'process':"ZREF",
                            'upper_heater_zone8': [df.loc[14, '1']],
                            'lower_heater_zone8': [df.loc[14, '2']],
                            'interval_time':[df.loc[2, '3']],
                            'update_by':[update_by],
                            'create_time':[time_file]
                        })
                        # Add the result to the list
                        result_list1.append(clean_result)
                        # Concatenate all the individual results into a single DataFrame
                        final_result = pd.concat(result_list1, ignore_index=True)
                    # Update DataFrame values
                    final_result['upper_heater_zone1'] = final_result['upper_heater_zone1'].astype(int) / 10
                    final_result['lower_heater_zone1'] = final_result['lower_heater_zone1'].astype(int) / 10
                    final_result['upper_heater_zone2'] = final_result['upper_heater_zone2'].astype(int) / 10
                    final_result['lower_heater_zone2'] = final_result['lower_heater_zone2'].astype(int) / 10
                    final_result['upper_heater_zone3'] = final_result['upper_heater_zone3'].astype(int) / 10
                    final_result['lower_heater_zone3'] = final_result['lower_heater_zone3'].astype(int) / 10
                    final_result['upper_heater_zone4'] = final_result['upper_heater_zone4'].astype(int) / 10
                    final_result['lower_heater_zone4'] = final_result['lower_heater_zone4'].astype(int) / 10
                    final_result['upper_heater_zone5'] = final_result['upper_heater_zone5'].astype(int) / 10
                    final_result['lower_heater_zone5'] = final_result['lower_heater_zone5'].astype(int) / 10
                    final_result['upper_heater_zone6'] = final_result['upper_heater_zone6'].astype(int) / 10
                    final_result['lower_heater_zone6'] = final_result['lower_heater_zone6'].astype(int) / 10
                    final_result['upper_heater_zone7'] = final_result['upper_heater_zone7'].astype(int) / 10
                    final_result['lower_heater_zone7'] = final_result['lower_heater_zone7'].astype(int) / 10
                    final_result['upper_heater_zone8'] = final_result['upper_heater_zone8'].astype(int) / 10
                    final_result['lower_heater_zone8'] = final_result['lower_heater_zone8'].astype(int) / 10
                    final_result['conveyor_m_per_min'] = final_result['conveyor_m_per_min'].astype(int) / 100

                    data_values = [row.to_dict() for _, row in final_result.iterrows()]
                    db = connections["10.17.66.121.iot.smt"].settings_dict
                    with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"]) as conn:
                        with conn.cursor() as cur:
                            insert_query = """
                                INSERT INTO smt.smt_reflow_tamura_set_log_master (
                                    line, machine, program, upper_heater_zone1, lower_heater_zone1, upper_heater_zone2, lower_heater_zone2,
                                    upper_heater_zone3, lower_heater_zone3, upper_heater_zone4, lower_heater_zone4, upper_heater_zone5,
                                    lower_heater_zone5, upper_heater_zone6, lower_heater_zone6, upper_heater_zone7, lower_heater_zone7,
                                    conveyor_m_per_min, o2_ppm, update_datetime, process, upper_heater_zone8, lower_heater_zone8,
                                    interval_time, update_by, create_time
                                ) VALUES %s
                                ON CONFLICT (line, machine, program, process)
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
                                    conveyor_m_per_min = EXCLUDED.conveyor_m_per_min,
                                    o2_ppm = EXCLUDED.o2_ppm,
                                    update_datetime = EXCLUDED.update_datetime,
                                    upper_heater_zone8 = EXCLUDED.upper_heater_zone8,
                                    lower_heater_zone8 = EXCLUDED.lower_heater_zone8,
                                    interval_time = EXCLUDED.interval_time,
                                    update_by = EXCLUDED.update_by,
                                    create_time = EXCLUDED.create_time
                            """
                            data_values = [(row['line'], row['machine'], row['program'], row['upper_heater_zone1'], row['lower_heater_zone1'],
                                row['upper_heater_zone2'], row['lower_heater_zone2'], row['upper_heater_zone3'], row['lower_heater_zone3'],
                                row['upper_heater_zone4'], row['lower_heater_zone4'], row['upper_heater_zone5'], row['lower_heater_zone5'],
                                row['upper_heater_zone6'], row['lower_heater_zone6'], row['upper_heater_zone7'], row['lower_heater_zone7'],
                                row['conveyor_m_per_min'], row['o2_ppm'], row['update_datetime'], row['process'], row['upper_heater_zone8'],
                                row['lower_heater_zone8'], row['interval_time'], row['update_by'], row['create_time'])
                            for _, row in final_result.iterrows()]
                            try:
                                with conn.cursor() as cursor:
                                    execute_values(cursor, insert_query, data_values)
                                conn.commit()
                            except Exception as ex:
                                print(ex)

                            logger.success(
                                f"commit smt_reflow_tamura_set_log_master",
                                input_process=f"filename {file_path}",
                                output_process="database table smt_reflow_tamura_set_log_master")
        
                            
        self.smt_reflow_tamura_set_log(INPUT_PATH2)
        
        logger.log("STOP", None)

    def smt_reflow_tamura_set_log(self, INPUT_PATH2):

        input_list2 = os.listdir(INPUT_PATH2)
        result_list2 = []  

        for folder1 in input_list2:
            os.path.exists(os.path.join(INPUT_PATH2, folder1))
            for folder2 in os.listdir(os.path.join(INPUT_PATH2, folder1)): 
                if not os.path.exists(os.path.join(INPUT_PATH2, folder1, folder2, "SET")):
                    continue
                file_list = fnmatch.filter(os.listdir(os.path.join(INPUT_PATH2, folder1, folder2, "SET")), "*.rpf")
                # # print(folder2)
                for file_name in file_list:  # Process the combined list
                    # # print(file_name)
                    if file_name == "NoName.RPF" or file_name == "ECOMode.RPF" or file_name == "ECOMode.rpf":
                        continue
                    else:
                        file_path = os.path.join(INPUT_PATH2, folder1, folder2, "SET", file_name)
                        Modifi_date = (datetime.fromtimestamp(os.stat(os.path.join(file_path)).st_mtime))
                        time_file = Modifi_date.strftime('%Y-%m-%d %H:%M:%S')

                        # Extract "Line" and "Machine" from the file path
                        line_machine = file_path.split('/')[-4:-2]
                        line, machine = line_machine

                        # Extract "Program" from the file name
                        program = file_name.split('.')[0]

                        # Read the data from the file into a DataFrame
                        with open(file_path, 'r') as file:
                            raw_data = file.read()
                            lines = raw_data.strip().split('\n')

                        rows = lines[:17]

                        table = []
                        for row in rows:
                            data = row.split(',')
                            data = [item.strip('\"') for item in data]
                            if len(data) > 1 and '####' in data[0]:
                                data[0] = data[0].split('#### ')[1].rstrip('#').strip()
                            else:
                                continue
                            table.append(data)

                        create_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        # Create a DataFrame
                        column_headers = [str(i) for i in range(len(table[0]))]
                        df = pd.DataFrame(table[1:], columns=column_headers)
                        # Create a new table with added "Line," "Machine," "Program," and other columns
                        clean_result = pd.DataFrame({
                            'line': [line],
                            'machine': [machine],
                            'program': [program],
                            'upper_heater_zone1': [df.loc[0, '1']],
                            'lower_heater_zone1': [df.loc[0, '2']],
                            'upper_heater_zone2': [df.loc[0, '3']],
                            'lower_heater_zone2': [df.loc[0, '4']],
                            'upper_heater_zone3': [df.loc[0, '5']],
                            'lower_heater_zone3': [df.loc[0, '6']],
                            'upper_heater_zone4': [df.loc[0, '7']],
                            'lower_heater_zone4': [df.loc[0, '8']],
                            'upper_heater_zone5': [df.loc[1, '1']],
                            'lower_heater_zone5': [df.loc[1, '2']],
                            'upper_heater_zone6': [df.loc[1, '3']],
                            'lower_heater_zone6': [df.loc[1, '4']],
                            'upper_heater_zone7': [df.loc[1, '5']],
                            'lower_heater_zone7': [df.loc[1, '6']],
                            'conveyor_m_per_min': [df.loc[1, '7']],
                            'o2_ppm': [df.loc[2, '1']],
                            'process':"ZREF",
                            'upper_heater_zone8': [df.loc[14, '1']],
                            'lower_heater_zone8': [df.loc[14, '2']],
                            'update_datetime': [create_time],
                            'interval_time':[df.loc[2, '3']],
                            'create_time': [time_file]
                        })

                        # Add the result to the list
                        result_list2.append(clean_result)
                        # Concatenate all the individual results into a single DataFrame
                        final_result = pd.concat(result_list2, ignore_index=True)
                        # Update DataFrame values
                final_result['upper_heater_zone1'] = final_result['upper_heater_zone1'].astype(int) / 10
                final_result['lower_heater_zone1'] = final_result['lower_heater_zone1'].astype(int) / 10
                final_result['upper_heater_zone2'] = final_result['upper_heater_zone2'].astype(int) / 10
                final_result['lower_heater_zone2'] = final_result['lower_heater_zone2'].astype(int) / 10
                final_result['upper_heater_zone3'] = final_result['upper_heater_zone3'].astype(int) / 10
                final_result['lower_heater_zone3'] = final_result['lower_heater_zone3'].astype(int) / 10
                final_result['upper_heater_zone4'] = final_result['upper_heater_zone4'].astype(int) / 10
                final_result['lower_heater_zone4'] = final_result['lower_heater_zone4'].astype(int) / 10
                final_result['upper_heater_zone5'] = final_result['upper_heater_zone5'].astype(int) / 10
                final_result['lower_heater_zone5'] = final_result['lower_heater_zone5'].astype(int) / 10
                final_result['upper_heater_zone6'] = final_result['upper_heater_zone6'].astype(int) / 10
                final_result['lower_heater_zone6'] = final_result['lower_heater_zone6'].astype(int) / 10
                final_result['upper_heater_zone7'] = final_result['upper_heater_zone7'].astype(int) / 10
                final_result['lower_heater_zone7'] = final_result['lower_heater_zone7'].astype(int) / 10
                final_result['upper_heater_zone8'] = final_result['upper_heater_zone8'].astype(int) / 10
                final_result['lower_heater_zone8'] = final_result['lower_heater_zone8'].astype(int) / 10
                final_result['conveyor_m_per_min'] = final_result['conveyor_m_per_min'].astype(int) / 100

                data_values = [row.to_dict() for _, row in final_result.iterrows()]
                db = connections["10.17.66.121.iot.smt"].settings_dict
                with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"]) as conn:
                    with conn.cursor() as cur:
                        insert_query = """
                            INSERT INTO smt.smt_reflow_tamura_set_log (
                                line, machine, program, upper_heater_zone1, lower_heater_zone1, upper_heater_zone2, lower_heater_zone2,
                                upper_heater_zone3, lower_heater_zone3, upper_heater_zone4, lower_heater_zone4, upper_heater_zone5,
                                lower_heater_zone5, upper_heater_zone6, lower_heater_zone6, upper_heater_zone7, lower_heater_zone7,
                                conveyor_m_per_min, o2_ppm, process, upper_heater_zone8, lower_heater_zone8, update_datetime,
                                interval_time, create_time
                            ) VALUES %s
                            ON CONFLICT (line, machine, program, process)
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
                                upper_heater_zone7 = EXCLUDED.upper_heater_zone7,
                                lower_heater_zone7 = EXCLUDED.lower_heater_zone7,
                                conveyor_m_per_min = EXCLUDED.conveyor_m_per_min,
                                o2_ppm = EXCLUDED.o2_ppm,
                                process = EXCLUDED.process,
                                upper_heater_zone8 = EXCLUDED.upper_heater_zone8,
                                lower_heater_zone8 = EXCLUDED.lower_heater_zone8,
                                update_datetime = EXCLUDED.update_datetime,
                                interval_time = EXCLUDED.interval_time,
                                create_time = EXCLUDED.create_time
                        """
                        data_values = [(row['line'], row['machine'], row['program'], row['upper_heater_zone1'], row['lower_heater_zone1'],
                        row['upper_heater_zone2'], row['lower_heater_zone2'], row['upper_heater_zone3'], row['lower_heater_zone3'],
                        row['upper_heater_zone4'], row['lower_heater_zone4'], row['upper_heater_zone5'], row['lower_heater_zone5'],
                        row['upper_heater_zone6'], row['lower_heater_zone6'], row['upper_heater_zone7'], row['lower_heater_zone7'],
                        row['conveyor_m_per_min'], row['o2_ppm'], row['process'], row['upper_heater_zone8'], row['lower_heater_zone8'],
                        row['update_datetime'], row['interval_time'], row['create_time']) for _, row in final_result.iterrows()]

                        with conn.cursor() as cursor:
                            execute_values(cursor, insert_query, data_values)
                        conn.commit()

                        logger.success(
                            f"commit smt_reflow_tamura_set_log",
                            input_process=f"filename {file_path}",
                            output_process="database table smt_reflow_tamura_set_log")
        self.analysis_data()

    def analysis_data(self):
        db = connections["10.17.66.121.iot.smt"].settings_dict
        with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"]) as conn:
            with conn.cursor() as cur:

                check_query = """
                    SELECT
                        log.line,
                        log.machine,
                        log.program,
                        log.process,
                        log_master.update_by,
                        case
                            when 
                        log.line = log_master.line AND
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
                        log.conveyor_m_per_min = log_master.conveyor_m_per_min AND
                        log.o2_ppm = log_master.o2_ppm AND
                        log.upper_heater_zone8 = log_master.upper_heater_zone8 AND
                        log.lower_heater_zone8 = log_master.lower_heater_zone8 AND
                        log.interval_time = log_master.interval_time
                            then 0
                            else 1
                        end as verify 
                    from
                        smt.smt_reflow_tamura_set_log as log
                    full join smt.smt_reflow_tamura_set_log_master as log_master
                    on log.line = log_master.line
                    and log.machine = log_master.machine
                    and log.program = log_master.program
                """
                
                cur.execute(check_query)
                result = cur.fetchall()
                df = pd.DataFrame(result, columns=["line", "machine", "program", "process", "update_by", "verify"])
                df = df[["line", "machine", "program", "process", "verify", "update_by"]]

                for index, row in df.iterrows():
                    check_query = """
                        SELECT id FROM smt.smt_reflow_tamura_set_log
                        WHERE line = %(line)s AND machine = %(machine)s AND program = %(program)s AND process = %(process)s
                    """
                    cur.execute(check_query, row)
                    existing_id = cur.fetchone()

                    if existing_id:
                        update_query = """
                            UPDATE smt.smt_reflow_tamura_set_log
                            SET verify = %(verify)s, update_by = %(update_by)s
                            WHERE line = %(line)s AND machine = %(machine)s AND program = %(program)s AND process = %(process)s
                        """
                        cur.execute(update_query, row)

                conn.commit()

                logger.success(
                    f"commit analysis_data smt_reflow_tamura_set_log",
                    input_process=f"database smt_reflow_tamura_set_log",
                    output_process="database table smt_reflow_tamura_set_log"
                )    

    def handle(self, *args, **options):                    
        schedule.every(1).minutes.do(self.jobqueue.put, lambda: self.run(INPUT_PATH1))
        self.run_schedule(schedule)
    