import pandas as pd

df = pd.read_csv("project2_testdata.csv", header=None)
df[3] += 1
df.to_csv("project2_testdata1.csv", header=None, index=False)