import logging
import json
import datetime
import azure.functions as func
import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):
    timeout = context.current_utc_datetime + datetime.timedelta(minutes=1.5)
    column_apis = [
        [80, 79, 78, 322680, 322669, "Historical1.csv"],
        [293674, 322687, 293675, 228, "Historical2.csv"],
        [84, 322665, 294451, 294452, 294427, "Historical3.csv"],
        [294428, 294418, 294417, "Historical4.csv"],
        [294442, 294441, "Historical5.csv"],
        [226, 225, 86, 85, 322684, "Historical6.csv"],
        [87, 23695, 322677, 23694, "Historical7.csv"],
        [224, 350464, 350466, 350465, 350463, "Historical8.csv"],
        [337425, 337426, "Historical9.csv"]
    ]

    historical_db_results = []

    for column_api in column_apis:
        historical_db_task = context.call_activity('HistoricalDBPROD', column_api)
        timeout_task = context.create_timer(timeout)
        winning_task = yield context.task_any([historical_db_task, timeout_task])

        if winning_task == timeout_task:
            raise Exception("Orchestration timed out.")
        
        historical_db_result = historical_db_task.result
        historical_db_results.append(historical_db_result)

        # Add a 1 second wait between each call to HistoricalDBPROD
        yield context.create_timer(context.current_utc_datetime + datetime.timedelta(seconds=4))

    Grouping_Task = context.call_activity('HistoricalGrouping', "DONOW")
    Grouping_Results = yield context.task_all([Grouping_Task])

    

main = df.Orchestrator.create(orchestrator_function)