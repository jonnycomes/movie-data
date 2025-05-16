# Movie Data: Modeling, Analysis, and Engineering with TMDb and MovieLens

This project combines metadata from [TMDb (The Movie Database)](https://www.themoviedb.org/) with user ratings and tags from [MovieLens](https://grouplens.org/datasets/movielens/) to create a dataset for studying movies and building prediction models.

The repository includes:
- A custom pipeline for collecting and processing the data
- A SQLite database that integrates local files with data from the TMDb API
- Parquet exports used for analysis
- Jupyter notebooks that explore different aspects of the data

The dataset is designed to support a wide range of analyses and should be useful for exploring trends, patterns, and questions related to movies and user behavior.


---

## Project Highlights

### [Predicting Movie Success](notebooks/successful_movie_prediction/main_models_and_predictions.html)

This notebook builds models to predict which movies will be considered “successful” based on TMDb data.

The first model is a random forest using **frequency encoding** for categorical variables. We then improve the model by introducing **categorical scores**—numerical summaries of how often each director, actor, writer, and production company has been associated with well-rated movies.

What’s inside:
- Feature importance plots and sample trees
- Partial dependence plots to understand what the model is learning
- Predictions on recent releases
- Reflections on what patterns seem to matter most

**Main takeaway:** Adding context to the raw data—via historical performance scores—improves the model, especially in identifying strong candidates for success.

---

### [Movie Rating Volume and the S&P 500: Signal or Coincidence](notebooks/rating_volume_and_market/rating_volume_vs_sp500.html)

This notebook investigates the following question:
Is there a strong negative correlation between MovieLens rating volume and the S&P 500?

To explore this, we:

- Join timestamped movie ratings with historical market data

- Compute 4-week rolling rating counts

- Compare correlations before and after removing time trends

**Conclusion:**
The strong initial correlation (-0.74) turns out to be misleading. After detrending, it drops to -0.23, suggesting both series were changing over time—but not because one was driving the other.

This is a good reminder to be cautious with correlations that might just reflect shared structure, like time.

---

## How to Clone and Build the Dataset

This repository does **not** include the full dataset — it's too large to store in version control.  
Instead, you can recreate it locally by running the build script.

To do so, you'll need a TMDb API key, which you can request [here](https://developer.themoviedb.org/docs). Once you have a key, you can run the build script to fetch data from both MovieLens and TMDb.

**Note:**  
Building the dataset takes quite a while. Most of the time is spent downloading and processing data from TMDb — the script makes a large number of API calls. Expect the resulting SQLite database to be between 2.5 and 3 GB in size.

Once you’re ready, follow these steps:

1. Clone the repository:
   ```bash
   git clone https://github.com/jonnycomes/movie-data.git
   cd movie-data
   ```

2. Install the package and its dependencies in editable mode:
   ```bash
   pip install -e .
   ```

3. Run the dataset build script:
   ```bash
   python build_dataset.py
   ```

4. When prompted, enter your TMDB API key to fetch movie data from The Movie Database (TMDb).

---

## Data Sources and Attribution

This project uses data from the following sources:

- [MovieLens](https://grouplens.org/datasets/movielens/), a research project maintained by the GroupLens lab at the University of Minnesota. The dataset is available for non-commercial use and widely used for academic and educational purposes.

- [TMDb (The Movie Database)](https://www.themoviedb.org/), accessed via the public TMDb API.  
  This product uses the TMDb API but is **not endorsed or certified by TMDb**.

Please refer to each provider’s site for licensing and terms of use.

