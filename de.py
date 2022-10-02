import requests
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas as pd
import datetime


# spark = SparkSession.builder.appName("Pratise").getOrCreate()
api = requests.get("https://api.dotreasury.com/polkadot/stats/weekly")
json = api.json()
CONSTANT = 10**10
data ={
    "date" : [],
    "income" : [],
    "treasuryBalance" : [],
    "output" : []
}

for value in json:
    data["date"].append(datetime.datetime.fromtimestamp(value["indexer"]["blockTime"]/1000.0))
    data["income"].append((int(value["income"]["inflation"])+ int(value["income"]["transfer"])+int(value["income"]["slash"])+int(value["income"]["others"]))/CONSTANT)
    data["treasuryBalance"].append(int(value["treasuryBalance"])/CONSTANT)
    data["output"].append((int(value["output"]["proposal"]) + int(value["output"]["tip"]) + int(value["output"]["bounty"]) + int(value["output"]["burnt"]))/CONSTANT )


y = pd.DataFrame(data,columns=["income","treasuryBalance","output"])
# x = pd.DataFrame(data,columns=["date"])
plt.plot(data["date"],y)
plt.show()