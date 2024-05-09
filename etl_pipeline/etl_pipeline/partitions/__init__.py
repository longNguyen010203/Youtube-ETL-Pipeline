from dagster import MonthlyPartitionsDefinition
from .. import constants


monthly_partitions = MonthlyPartitionsDefinition(
    start_date=constants.START_DATE,
    end_date=constants.END_DATE
)