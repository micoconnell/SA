import logging
import json
import datetime
import azure.functions as func
import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):
    timeout = context.current_utc_datetime + datetime.timedelta(minutes=1)
    column_apis = [
        [80, 79, 78, 322680, 322669, "Current1.csv"],
        [293674, 322687, 293675, 228, "Current2.csv"],
        [84, 322665, 294451, 294452, 294427, "Current3.csv"],
        [294428, 294418, 294417, "Current4.csv"],
        [294442, 294441, "Current5.csv"],
        [226, 225, 86, 85, 322684, "Current6.csv"],
        [87, 23695, 322677, 23694, "Current7.csv"],
        [224, 350464, 350466, 350465, 350463, "Current8.csv"],
        [337425, 337426, "Current9.csv"]
    ]

    current_db_results = []

    for column_api in column_apis:
        current_db_task = context.call_activity('CurrentPROD', column_api)
        timeout_task = context.create_timer(timeout)
        winning_task = yield context.task_any([current_db_task, timeout_task])

        if winning_task == timeout_task:
            raise Exception("Orchestration timed out.")
        
        current_db_result = current_db_task.result
        current_db_results.append(current_db_result)

        # Add a 1 second wait between each call to HistoricalDBPROD
        yield context.create_timer(context.current_utc_datetime + datetime.timedelta(seconds=4))

    Grouping_Task = context.call_activity('CurrentGrouping', "DONOW")
    Grouping_Results = yield context.task_all([Grouping_Task])

    

main = df.Orchestrator.create(orchestrator_function)