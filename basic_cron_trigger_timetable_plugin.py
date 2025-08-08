""
BasicCronTriggerTimetable: An Airflow timetable plugin with modified catchup behavior
This module provides a custom Airflow timetable implementation that modifies how DAGs
are scheduled when catchup is disabled. Unlike the standard CronTriggerTimetable which
may trigger immediate execution when catchup is False, this implementation ensures that
DAGs only run at the next appropriate cron schedule point.
To register this plugin, place it in your Airflow plugins directory.
Key Features:
- Extends the standard CronTriggerTimetable
- Modifies scheduling behavior for catchup=False scenarios
- Prevents immediate execution when a DAG is enabled with catchup disabled
- Maintains all other standard cron scheduling capabilities
Usage:
    # In your DAG definition

    
    with DAG(
        dag_id="example_dag",
        timetable=BasicCronTriggerTimetable("0 0 * * *", timezone=UTC),  # Runs daily at midnight UTC
        catchup=False,
        ...
    ) as dag:
        # Your tasks here
Notes:
    - This plugin is automatically registered with Airflow upon startup
    - The timetable can be used with any cron expression supported by Airflow
    - Best used for DAGs where immediate backfilling is not desired when enabling with catchup=False
"""

import logging
from typing import TYPE_CHECKING
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction
from airflow.utils.timezone import coerce_datetime, utcnow
from airflow.timetables.trigger import CronTriggerTimetable


log = logging.getLogger(__name__)


class BasicCronTriggerTimetable(CronTriggerTimetable):
    """
    A modified implementation of CronTriggerTimetable that changes behavior when catchup is disabled.

    This timetable extends the standard CronTriggerTimetable but modifies how the next run time
    is calculated when catchup is disabled. Instead of potentially triggering an immediate run
    using _align_to_prev (as in the parent class), it uses _align_to_next to ensure the DAG runs
    at the next appropriate cron schedule point rather than immediately after being enabled.

    Example:
        ```python
        timetable = BasicCronTriggerTimetable("0 0 * * *", timezone=UTC) # Runs daily at midnight UTC
        ```
    """

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:

        log.info("Starting scheduling decision")

        parent_interval = super().next_dagrun_info(
            last_automated_data_interval=last_automated_data_interval,
            restriction=restriction,
        )
        log.info(
            f"Parent interval start and end: {parent_interval.data_interval.start}, {parent_interval.data_interval.end}"
        )
        start_time_candidates = (
            [parent_interval.data_interval.end] if parent_interval else []
        )

        if not restriction.catchup:
            log.info(
                "Catchup is disabled, calculating next start time based on current time and restrictions."
            )
            # In parent class behavior, if catchup is disabled, it returns the next run with _align_to_prev
            # causing it to run immediately after enabling the DAG.
            start_time_candidates.append(self._align_to_next(coerce_datetime(utcnow())))

            log.info(f"Start time candidates: {start_time_candidates}")
        next_start_time = max(start_time_candidates)
        # log the interval start, end

        interval_ = DagRunInfo.interval(
            coerce_datetime(next_start_time - self._interval),
            next_start_time,
        )
        log.info(
            f"Next DagRun interval: start={interval_.data_interval.start}, end={interval_.data_interval.end}"
        )

        return interval_


class BasicCronTriggerTimetablePlugin(AirflowPlugin):
    name = "BasicCronTriggerTimetable"
    timetables = [BasicCronTriggerTimetable]
