import pandas as pd
import dask.dataframe as dd

def run_dask_csv_demo() -> None:
    # --- Loading from "Twitter Actual Data" Example ---
    
    # We found the real file name is 'twitter_actual_data.csv'
    csv_file = "twitter_actual_data.csv"
    print(f"Reading from {csv_file}...")

    # The dataset has no header, so we define columns manually
    # 0: target, 1: id, 2: date, 3: flag, 4: user, 5: text
    col_names = ["target", "id", "date", "flag", "user", "text"]

    def get_text_length(text: str) -> str:
        """Categorizes tweet length."""
        try:
            val = len(str(text))
            if val < 50:
                return "Short"
            elif val < 100:
                return "Medium"
            return "Long"
        except Exception:
            return "Error"

    # 1. Pandas
    print("--- Pandas ---")
    try:
        # Read a subset for Pandas (header=None means first row is data)
        df = pd.read_csv(csv_file, names=col_names, header=None, encoding="ISO-8859-1")
        # Just grab the last 5 rows to show we read it
        print(f"Pandas Data Sample:\n{df[['target', 'user', 'text']].tail(5)}")
    except FileNotFoundError:
        print(f"Error: {csv_file} not found.")
        return

    # 2. Dask
    print("\n--- Dask ---")
    # Read CSV with Dask
    ddf = dd.read_csv(csv_file, names=col_names, header=None, encoding="ISO-8859-1", blocksize="64MB")
    
    # Let's apply our custom function to the 'text' column
    print("Applying custom logic (Tweet Length Category)...")
    task = ddf['text'].apply(get_text_length, meta=('text', 'object'))
    
    print(f"Dask Task Object: {task}")
    print("Computing...")
    
    # We'll compute the head of the result
    result_dask = task.compute()
    print(f"Dask Result (Head):\n{result_dask.head(5)}")
    
    # Bonus: Let's count how many of each category (requires full scan)
    # This is where Dask shines on big data
    print("\nComputing Value Counts (full scan)...")
    
    # Compute counts for our custom text length
    length_counts = task.value_counts().compute()
    print(f"Length Category Counts:\n{length_counts}")

    print("-" * 30)
    print("Analyzing Sentiment Targets (0=Neg, 2=Neu, 4=Pos)...")
    
    # Map the numeric target to labels
    def map_target(val: int) -> str:
        if val == 0: return "Negative"
        if val == 2: return "Neutral"
        if val == 4: return "Positive"
        return "Unknown"

    sentiment_task = ddf['target'].apply(map_target, meta=('target', 'object'))
    sentiment_counts = sentiment_task.value_counts().compute()
    print(f"Sentiment Distribution:\n{sentiment_counts}")
