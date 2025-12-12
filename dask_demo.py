import pandas as pd
import dask.dataframe as dd

def run_dask_demo() -> None:
    # --- Custom Logic Example ---
    
    def my_custom_func(x: int) -> str:
        """A simple custom function that categorizes numbers."""
        return "High" if x > 50 else "Low"

    # 1. Pandas: Eager
    print("--- Pandas Custom Apply ---")
    df = pd.DataFrame({'a': range(100)})
    # Pandas runs this immediately on a single core
    result_pandas = df['a'].apply(my_custom_func)
    print(f"Pandas Result (Head):\n{result_pandas.head(10)}")
    print(f"Pandas Result (Tail):\n{result_pandas.tail(10)}")

    # 2. Dask: Lazy & Parallel
    print("\n--- Dask Custom Apply ---")
    ddf = dd.from_pandas(df, npartitions=2)
    
    # Dask needs to know what the output looks like (meta). 
    # Since our function returns a string/object, we specify it.
    task = ddf['a'].apply(my_custom_func, meta=('a', 'object'))
    
    print(f"Dask Task Object: {task}")
    print("Computing...")
    # This runs in parallel on the partitions
    result_dask = task.compute()
    print(f"Dask Result (Head):\n{result_dask.head(10)}")
    print(f"Dask Result (Tail):\n{result_dask.tail(10)}")
