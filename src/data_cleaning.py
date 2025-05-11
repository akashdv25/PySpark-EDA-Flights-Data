from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, NumericType ,TimestampType


def clean_nulls(df: DataFrame, threshold_drop_col=0.2):
    """
    Cleans nulls in a DataFrame according to the following logic:
    - Drop columns with > threshold_drop_col (default 20%) nulls.
    - Else drop the rows with nulls in the columns.
        Returns a cleaned DataFrame.
    """
   
    total_rows = df.count()
    cols_to_drop = []
    cols_to_drop_rows = []

    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_ratio = null_count / total_rows

        if null_ratio > threshold_drop_col:
            cols_to_drop.append(col)
        else:            
            cols_to_drop_rows.append(col)

    print(f"\nColumns to drop (> {threshold_drop_col*100:.0f}% nulls): {cols_to_drop}")
    print(f"Columns to drop rows for (<20% nulls): {cols_to_drop_rows}\n")
    

    # Drop columns with too many nulls
    df = df.drop(*cols_to_drop)

    #  Drop rows with nulls in columns with moderate nulls
    if cols_to_drop_rows:
        df = df.dropna()
        print(f"Dropped rows with nulls in columns: {cols_to_drop_rows}")
    
    return df





#function to remove the duplicates in the data
def remove_duplicates(df, subset=None):
    num_of_duplicates = df.count() - df.dropDuplicates(subset).count()
    print(f"Number of duplicates removed: {num_of_duplicates}")
    return df.dropDuplicates(subset)


