import pandas as pd

# preprocessing of Donation table
df = pd.read_csv("Donation.csv")
# remain all people whose statement is Active
df = df[df.Condition == 'Active']
# drop columns which is not useful
df = df.drop(columns = ['FIRST_N', 'LAST_N', 'Condition'])
# wirte processed dataframe into txt file
df.to_csv("Donation_updated.txt", sep = '\t',  index  = False, header=False)
