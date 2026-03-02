"""
monitoring/alert_hooks.py

Airflow on_failure_callback — logs pipeline failures.
Optionally posts to a Slack webhook (set SLACK_WEBHOOK_URL in .env).
"""

import logging
import os
from typing import Any, Dict

import requests

logger = logging.getLogger(__name__)


def notify_on_failure(context: Dict[str, Any]) -> None:
    """
    Airflow on_failure_callback.
    Called when any task in the DAG fails.

    Args:
        context: Airflow task execution context dict.
    """
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    exception = context.get("exception")
    log_url = context.get("task_instance").log_url

    message = (
        f":red_circle: *Airflow Task Failed*\n"
        f"• *DAG*: `{dag_id}`\n"
        f"• *Task*: `{task_id}`\n"
        f"• *Execution Date*: `{execution_date}`\n"
        f"• *Error*: `{exception}`\n"
        f"• *Logs*: {log_url}"
    )

    logger.error("TASK FAILURE — DAG: %s | Task: %s | Date: %s | Error: %s", dag_id, task_id, execution_date, exception)

    slack_webhook = os.getenv("SLACK_WEBHOOK_URL")
    if slack_webhook:
        try:
            resp = requests.post(slack_webhook, json={"text": message}, timeout=10)
            resp.raise_for_status()
            logger.info("Slack alert sent successfully.")
        except Exception as exc:
            logger.warning("Failed to send Slack alert: %s", exc)
    else:
        logger.info("SLACK_WEBHOOK_URL not set — skipping Slack notification. Full message:\n%s", message)
