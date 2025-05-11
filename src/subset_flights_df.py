def save_subset_csv(input_csv: str, limit: int, output_csv: str):
    """
    Copies the header and first `limit` rows from `input_csv` to `output_csv`.
    """
    with open(input_csv, 'r') as infile, open(output_csv, 'w') as outfile:
        for i, line in enumerate(infile):
            outfile.write(line)
            if i == limit:
                break
    print(f"Saved {limit} rows to {output_csv}")

if __name__ == "__main__":
    # Example usage:
    save_subset_csv("/home/new-user/Documents/PySpark-EDA-Flights-Data/data/flights.csv", 180000, "data/small_flights_df.csv")