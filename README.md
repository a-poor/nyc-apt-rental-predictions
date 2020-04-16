# Modeling NYC Apartment Rental Pricing on Craigslist

![map of nyc listing data](images/cl_apartments_map.png)

<img src="images/cl_apartments_map.png" align="center" width="500" height="auto"/>

by Austin Poor

I did this analysis as my second project for the [Metis Data Science Bootcamp](https://www.thisismetis.com/data-science-bootcamps). For this project we chose our own topics but were required to use gather our data by web-scraping and use a linear regression model.

As a New Yorker, I chose to model New York City apartment rental prices, using data scraped from Craigslist.

Data was collected from NYC area Craigslist ([newyork.craigslist.com](https://newyork.craigslist.com/search/apa)) with listings that were posted in the range `2019-12-24` to `2020-01-23`.

***

## Process

I used two python scripts to scrape and clean my data – [scrape.py](./scrape.py) and [clean.py](./clean.py) – which download apartment listing data to an sqlite database `data/craigslist_apts.db`.

From there, the notebook [craigslist_regression.ipynb](./craigslist_regression.ipynb) loads the data, further cleans it, and then models the data. There's an additional notebook, [geometry_conversion.ipynb](./geometry_conversion.ipynb), which is used to calculate apartment neighborhoods based on the _latitude_ and _longitude_ data from the Craigslist apartment listing.

## Results

After testing multiple types of linear models (linear regression, degree-2 polynomial regression, degree-3 polynomial regression, LASSO, and Ridge), my final model (degree 3 polynomial regression) was able to get an R^2 score of `0.768` on test data.

## Presentation

I've included a pdf of the slide deck used for my presentation, [here](./nyc-apt-rental-predictions-presentation.pdf).



