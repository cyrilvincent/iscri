import datetime

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import seaborn as sns
import geopandas as gpd
import json
import pandas as pd
import country_converter as coco
import requests



# Getting the data
# PAYLOAD = {'code': 'ALL'}
# URL = 'https://api.statworx.com/covid'
# RESPONSE = requests.post(url=URL, data=json.dumps(PAYLOAD))
# with open("data/covid.json", "w") as f:
#     f.write(RESPONSE.text)
# Convert the response to a data frame


# covid_df = pd.read_json("data/covid.json")
# print(covid_df.head(3))

# load the low resolution world map
world = gpd.read_file("../data/110m_cultural.zip")
print(list(world.columns))
world = world[['ADMIN', 'ADM0_A3', 'geometry']]
world = world[world.ADMIN != "Antarctica"]
print(world[['ADMIN', 'ADM0_A3']])
world["iso2"] = coco.convert(names=world['ADMIN'].to_list(), to='ISO2', not_found='NULL')
world = world[world.iso2 != "NULL"]

# world.plot()
# plt.show()

covid_df = pd.read_json("../data/covid.json")
date = datetime.date(2020,4,1)
covid_df = covid_df[covid_df['date'] == date.strftime("%Y-%m-%d")]
print(covid_df.head(3))

merged_df = pd.merge(left=world, right=covid_df, how='left', left_on='iso2', right_on='code')
df = merged_df.drop(['day', 'month', 'year', 'code'], axis=1)
df['case_growth_rate'] = round(df['cases']/df['cases_cum'], 2)
df['case_growth_rate']=df['case_growth_rate'].fillna(0)
print(df)

# Print the map
# Set the range for the choropleth
title = 'Daily COVID-19 Growth Rates'
col = 'cases'
source = 'Source: relataly.com \nGrowth Rate = New cases / All previous cases'
vmin = df[col].min()
vmax = df[col].max()
cmap = 'magma'

# Create figure and axes for Matplotlib
fig, ax = plt.subplots(1, figsize=(20, 8))

# Remove the axis
ax.axis('off')
df.plot(column=col, ax=ax, edgecolor='0.8', linewidth=1, cmap=cmap)

# Add a title
ax.set_title(title, fontdict={'fontsize': '25', 'fontweight': '3'})

# Create an annotation for the data source
ax.annotate(source, xy=(0.1, .08), xycoords='figure fraction', horizontalalignment='left',
            verticalalignment='bottom', fontsize=10)

# Create colorbar as a legend
sm = plt.cm.ScalarMappable(norm=plt.Normalize(vmin=vmin, vmax=vmax), cmap=cmap)

# Empty array for the data range
sm._A = []

# Add the colorbar to the figure
cbaxes = fig.add_axes([0.15, 0.25, 0.01, 0.4])
cbar = fig.colorbar(sm, cax=cbaxes)

print(df["cases"])
centroids = df.copy()
centroids.geometry = world.centroid
centroids['size'] = centroids['case_growth_rate'] * 1000
centroids.plot(markersize='size', ax=ax, cmap=cmap)

plt.show()

