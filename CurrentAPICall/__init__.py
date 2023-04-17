import datetime
import logging
import requests
import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    url = 'https://orchestrasuncorsupply.azurewebsites.net/api/orchestrators/DurableFunctionsOrchestratorCurrent'
    requests.get(url)
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)