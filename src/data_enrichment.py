from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def add_delay_category(
    df: DataFrame,
    delay_col: str = "ARRIVAL_DELAY",
    new_col: str = "DELAY_CATEGORY"
) -> DataFrame:
    """
    Adds a categorical delay label to the DataFrame based on arrival delay.
    Categories:
        - On-Time: delay <= 0
        - Short Delay: 0 < delay <= 15
        - Medium Delay: 15 < delay <= 60
        - Long Delay: delay > 60
        - Unknown: if delay is null
    Args:
        df: Input DataFrame
        delay_col: Name of the column with delay in minutes
        new_col: Name of the new column to create
    Returns:
        DataFrame with the new categorical delay column
    """
    return df.withColumn(
        new_col,
        F.when(F.col(delay_col).isNull(), "Unknown")
         .when(F.col(delay_col) <= 0, "On-Time")
         .when((F.col(delay_col) > 0) & (F.col(delay_col) <= 15), "Short Delay")
         .when((F.col(delay_col) > 15) & (F.col(delay_col) <= 60), "Medium Delay")
         .when(F.col(delay_col) > 60, "Long Delay")
         .otherwise("Unknown")
    )


