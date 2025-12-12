from dask_demo import run_dask_demo
from dask_csv_loader import run_dask_csv_demo

def main() -> None:
    print("Hello from sentiment-analysis-tweet!")
    print("\nSelect a demo to run:")
    print("1. Basic Dask vs Pandas (In-Memory Custom Logic)")
    print("2. CSV Loading Example (Custom Logic on File)")
    
    choice = input("Enter 1 or 2: ")
    
    if choice == "1":
        print("\nRunning Basic Demo...")
        run_dask_demo()
    elif choice == "2":
        print("\nRunning CSV Demo...")
        run_dask_csv_demo()
    else:
        print("Invalid choice.")

if __name__ == "__main__":
    main()
