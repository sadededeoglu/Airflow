import urllib
from airflow.utils.email import send_email
import logging


def send_mail_on_dag_failure(context):
    """
    Callback function to be executed when a DAG fails.

    Args:
        context (dict): The context dictionary containing information about the DAG run and task instance.

    The function performs the following actions:
    - Extracts task instance, DAG, and DAG run information from the context.
    - Constructs a URL link to the DAG run in the Airflow webserver.
    - Iterates through task instances to build an HTML body with task details.
    - Sends an email notification with the DAG failure information and task instances details.

    The email contains:
    - DAG ID and state.
    - Reason for failure.
    - Link to the DAG run in the Airflow webserver.
    - Details of each task instance including task ID, state, attempt number, and maximum attempts.
    """

    #logging.info(context)
    ti = context['task_instance']
    dag = context['dag']
    dag_run = context["dag_run"]
    dag_state = dag_run.get_state()
    execution_date = urllib.parse.quote(dag_run.execution_date.isoformat())
    task_ins = dag_run.task_instances
    base_url = context['conf'].get('webserver', 'BASE_URL')
    link = f'{base_url}/graph?dag_id={dag_run.dag_id}&root=&execution_date={execution_date}'

    task_body = ""
    # loop through the task instances
    for task in task_ins:
        attempt = task.try_number - 1
        max_attempts = task.max_tries + 1
        if task.state == 'failed':
            background_color = '#fff0f0'
            border_color = '#ff4444'
        elif task.state == 'success':
            background_color = '#f0f8f0'
            border_color = '#4CAF50'
        else: # task.state == 'skipped':
            background_color = '#f0f0f0'
            border_color = '#808080'

        task_body += f"<div style='max-width: 200px; padding: 20px; background-color: {background_color}; border-left: 4px solid {border_color}; margin-top: 8px;'>"
        task_body += f"Task:{task.task_id}<br>"
        task_body += f"State:{task.state}<br>"
        task_body += f"Attempt:{attempt}<br>"
        task_body += f"Max Attempts:{max_attempts}<br><br>"
        task_body += "</div>"


    body = "<h2> DAG Failure Info</h2>"
    body += "<div style='max-width: 200px; padding: 20px; background-color: #fff0f0; border-left: 4px solid #ff4444; margin-top: 16px;'>"
    body += f"Dag : {ti.dag_id} <br>"
    body += f"Task Sahibi: {dag.owner} <br>"
    body += f"Task AcÄ±klamasÄ±: {dag.description} <br>"
    body += f"State: {dag_state} <br>"
    body += f"Reason: {context['reason']} <br>"
    body += f"Link: <a href='{link}'>DAG RUN</a><br>"
    body += "</div>"
    body += "<h3>Task Instances Info</h3>"
    body += f"{task_body}"

    send_email(
        to=dag.default_args['email'],
        subject=f"Airflow Alert: DAG Failure: '{ti.dag_id}' has failed",
        html_content=body,
    )
