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

        db = connections["10.17.66.121.iot.smt"].settings_dict
        with psycopg2.connect(user=db["USER"], password=db["PASSWORD"], host=db["HOST"], port=db["PORT"], dbname=db["NAME"], options=db['OPTIONS']['options']) as conn:
            with conn.cursor() as cur:
                try:
                    analysis_query = """ 
                        SELECT DISTINCT
                            smic.line_no,
                            smic.machine,
                            smic."program",
                            smic.verify,
                            'SET' as description,
                            smic.process
                        FROM
                            smt_reflow_smic_set_log smic
                        INNER JOIN (
                            SELECT DISTINCT ON (mi.line, actv.machine_code)
                                mi.line,
                                actv.machine_code,
                                actv."recipe" as program,
                                actv.datetime
                            FROM
                                smt_reflow_smic_actv actv
                            INNER JOIN
                                smt_reflow_smic_actv mi ON actv.machine_code = mi.machine_code
                            INNER JOIN (
                                SELECT
                                    actv.machine_code,
                                    max(actv.id) as id
                                FROM
                                    smt_reflow_smic_actv actv
                                GROUP by actv.machine_code) mxid ON actv.id = mxid.id) tmact 
                                ON smic.line_no = tmact.line
                                AND smic.machine = tmact.machine_code
                                AND smic."program" = tmact.program;
                    """
                    cur.execute(analysis_query)
                    result = cur.fetchall()
                    df = pd.DataFrame(result, columns=["line", "machine", "program", "signal", "description", "process"])
                    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    df["update_date"] = current_timestamp
                    df = df[["line", "machine", "description", "signal", "update_date", "process"]]

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
                        f"commit signal database table smt_reflow_smic_set_log(121) to smt_lock_signal_dev(65)",
                        input_process=f"input database table smt_reflow_smic_set_log(121)",
                        output_process=f"database table smt_lock_signal_dev(65)"
                    )

                except Exception as ex:
                    logger.exception(ex)

        logger.log("STOP", None)
        pass

    def handle(self, *args, **options):           
        schedule.every(1).minutes.do(self.jobqueue.put, self.run)
        self.run_schedule(schedule)