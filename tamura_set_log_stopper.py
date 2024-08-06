from datetime import datetime
from django.core.management.base import BaseCommand, CommandError
from django.db import connections
from psycopg2.extras import execute_values
from server.settings import logger
from server.util.custom_base_command import CustomBaseCommand
import pandas as pd
import psycopg2
import queue
import schedule
import sys

# Config

class Command(CustomBaseCommand):
 
    @logger.catch
    def run(self):
        
        logger.log("START", None)
        
        db = connections["10.17.66.121.iot.smt"].settings_dict
        with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"], options=db['OPTIONS']['options']) as conn:
            with conn.cursor() as cur:
                try:
                    analysis_query = """
                        WITH LatestSource AS (
                            SELECT DISTINCT ON (machine_code) machine_code, "source" AS program
                            FROM (
                                SELECT machine_code, "source", ROW_NUMBER() OVER (PARTITION BY machine_code ORDER BY create_at DESC) AS rn
                                FROM smt_reflow_tamura_temp_log
                            ) AS temp
                            WHERE rn = 1
                        )

                        SELECT DISTINCT ON (srtsl.machine) srtsl.line, srtsl.machine, 
                            COALESCE(ls.program, 'source') AS program,
                            CASE 
                                WHEN srtsl.program IS NULL THEN 1 
                                WHEN srtsl.program != COALESCE(ls.program, 'source') THEN 1 
                                ELSE COALESCE(srtsl.verify, 1)
                            END as verify,
                            'SET' as description, srtsl.process
                        FROM (
                            SELECT line, machine, program, verify, process
                            FROM smt_reflow_tamura_set_log 
                            WHERE machine IN (
                                SELECT machine_code
                                FROM smt_reflow_tamura_temp_log
                            )
                        ) srtsl
                        LEFT JOIN LatestSource ls
                        ON srtsl.machine = ls.machine_code
                        WHERE (srtsl.program IS NULL OR srtsl.program = ls.program) 
                        ORDER BY srtsl.machine, ls.program
                    """
                    cur.execute(analysis_query)
                    result = cur.fetchall()
                    df = pd.DataFrame(result, columns=["line", "machine", "program", "signal", "description", "process"])
                    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    df["update_date"] = current_timestamp
                    df = df[["line", "machine","description", "signal","update_date","process"]]
                
                    db = connections["10.17.72.65.iot.smt"].settings_dict
                    conn = psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"])
                    cur = conn.cursor()
                    insert_query = """
                        INSERT INTO smt.smt_lock_signal_dev (
                            line, machine, description, signal, update_date, process
                        ) VALUES %s
                        ON CONFLICT (line, machine, description, process)
                        DO UPDATE
                        SET signal = EXCLUDED.signal,
                            update_date = EXCLUDED.update_date;
                    """
                    # Ensure that data_values contains the correct number of values for each tuple
                    data_values = [tuple(row) for row in df.to_numpy()]
                    with conn.cursor() as cursor:
                        execute_values(cursor, insert_query, data_values)
                    conn.commit()

                    logger.success(
                        f"commit database table smt_reflow_tamura_set_log(121) to smt_lock_signal_dev(65)",
                        input_process=f"input database table smt_reflow_tamura_set_log(121)",
                        output_process=f"database table smt_lock_signal_dev(65)"
                    )
                    
                except Exception as ex:
                    logger.exception(ex)

        logger.log("STOP", None)
        pass

    def handle(self, *args, **options):
        schedule.every(1).minutes.do(self.jobqueue.put, self.run)
        self.run_schedule(schedule)