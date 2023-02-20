# Airline On-Time Performance Data
## by Barnabas Ifebude


## Dataset

The data consists of 118,914,458 US commerical flights information betwee Oct 1988 and April 2008, with 29 features including the flight date, departure & arrival time, departure & arrival delays in minutes among other features. The dataset and description of each feature can be found [here](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/HG7NV7). The dataset size is huge about 12GB and couldn't be loaded into a dataframe. SQLite database was created, into which data was inserted in chunks and then needed data was queried and loaded into a dataframe on the fly.


## Summary of Findings

In the exploration phase, I found that majority of the flights departed or arrived within -50 and 50 minutes of schedule departure or arrival. To show all the bins in the distribution, I applied a log transform on the y-axis. Checking the average duration of delays in minutes reveals that yearly average is between 35 and 60 minutes. Isolating for the top 5 airport shows that median delay at Chicago O'Hare International airport is the highest, although the same airport doubles as the busiest airport.

Furthermore, a strong positive relationship was seen between arrival and departure delays. The timeseries trend reveals that delay arcoss the top 5 airports in the US has been on a steady increase, although there was dip after the 9/11 attacks, the trend resumed again at the beginning of 2003.


## Key Insights for Presentation

For the presentation, I focus on just the influence of airports on flight delays. I started by introducing the arrival and departure delay variables, followed by the transformed histograms of the arrival and departure delay variables. Then, I introduced the scatterplot of both variable to show the relationship between them.

Afterwards, I introduce the airport as a categorical variable. I used a boxplots of departure delay and airport to show the distribution of delay across the top 5 airports. I also used a timeseries plot to show the trend of airport delays across the top 5 airports. Finally, I use FacetGrid to show the relationship between departure and arrival delays across airports.
