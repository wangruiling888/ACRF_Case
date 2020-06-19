import pandas as pd
import re

df_orgin = pd.read_csv('Customer.csv')
df_update = pd.DataFrame()
print("finish")
def clear(x):
  a = re.sub("\D", "", str(x))
  return a
def process(x):
  x = str(x)
  if len(x) == 2:
    return " " + x
  else:
    return x
def format_trans(x):
  a = x[:-3]
  b = x[-3:]
  print(b)
  if a[0:2] == '61':
    a = a[2:]
  if len(a) == 10:
    if a[0:2] == '02' or  a[0:2] == '03' or  a[0:2] == '07' or  a[0:2] == '08':
      a = "({}) {} {}".format(a[0:2], a[2:6], a[6:])
      return a
    elif a[0:2] == '04':
      a = "{} {} {}".format(a[0:4], a[4:7], a[7:])
      return a
  elif len(a) == 9:
    if a[0] == '2' or a[0] == '3' or a[0] == '7' or a[0] == '8':
      a = "(0{}) {} {}".format(a[0], a[1:5], a[5:])
      return a
    elif a[0] == '4':
      a = "0{} {} {}".format(a[0:3], a[3:6], a[6:])
      return a
  if len(a) == 8:
    state = b.strip()
    if  state == 'NSW' or state == 'ACT':
      a = "02 {} {}".format(a[0:4], a[4:])
    elif state == 'VIC':
      a = "03 {} {}".format(a[0:4], a[4:])
    elif state == "QS":
      a = "07 {} {}".format(a[0:4], a[4:])
    elif state == 'WA' or state == 'SA' or state == "NT":
      a = "08 {} {}".format(a[0:4], a[4:])
    else:
      a = "04 {} {}".format(a[0:4], a[4:])
    return a
  return ""
df_update["Donor Number"] = df_orgin["Donor Number"].apply(lambda x: str(int(x)))
df_orgin["Phone"] = df_orgin["Phone"].apply(lambda x: clear(x));
df_orgin["State"]  = df_orgin["State"].apply(lambda x: process(x))
df_orgin["add"] =  df_orgin["Phone"] + df_orgin["State"] 
df_update["Phone"] = df_orgin["add"].apply(lambda x:format_trans(x))
df_update = df_update[df_update.Phone!=""]
df_update.to_csv('new1.csv')  
