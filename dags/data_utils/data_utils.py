# utils/data_helpers.py
import pandas as pd

def batch_generator(df, batch_size):
    num_batches = len(df) // batch_size + 1
    total_batches = len(df) // batch_size + 1
    print("Total number of batches:", total_batches)
    for i in range(num_batches):
        yield df.iloc[i * batch_size: (i + 1) * batch_size], i + 1, total_batches