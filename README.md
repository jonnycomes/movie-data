# Movie Data: Modeling, Analysis, and Engineering with TMDb and MovieLens

This project combines metadata from [TMDb](https://www.themoviedb.org/) (The Movie Database) with user ratings and tags from [MovieLens](https://grouplens.org/datasets/movielens/) to create a dataset for studying movies and building prediction models.

The repository includes:
- A custom pipeline for collecting and processing the data
- A SQLite database that integrates local files with data from the TMDb API
- Parquet exports used for analysis
- Jupyter notebooks that explore different aspects of the data

The dataset is designed to support a wide range of analyses and should be useful for exploring trends, patterns, and questions related to movies and user behavior.


---

## Project Highlights

### [Predicting Movie Success](https://jonnycomes.github.io/project_links/movie_data/main_models_and_predictions.html)

This notebook builds models to predict whether a movie will be considered “successful” based on TMDb data.

**What’s inside:**
- **Baseline model**: Random forest using frequency encoding for categorical variables  
  - Precision ~60%, very low recall  
- **Improved model**: Adds **categorical scores** that reflect historical success of directors, actors, writers, and production companies  
  - Precision improves to ~70%  
- **Predictions**: Applies the model to recent releases  
- **Interpretation**: Uses feature importance and partial dependence plots to identify key predictors of success

**Main takeaway:** Thoughtful feature engineering—especially contextual success scores—can substantially improve predictive performance.

---

### [Movie Rating Volume and the S&P 500: Signal or Coincidence](https://jonnycomes.github.io/project_links/movie_data/rating_volume_vs_sp500.html)

This notebook investigates the following question:  
_Does a drop in the stock market lead to more people rating movies?_

**What’s inside:**
- Found a strong negative correlation (-0.74) between the S&P 500 index and rating volume in the following month  
- Explored how changing the forward-looking window affected the correlation, revealing signs that time trends might be driving the relationship  
- Removed shared time trends and saw the correlation drop to -0.23  

**Main takeaway:**  
The initial correlation was misleading—both series were trending over time but not causally linked. Detrending revealed a much weaker relationship. This is a good reminder to be cautious with correlations that might just reflect shared structure, like time.

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

- [TMDb](https://www.themoviedb.org/) (The Movie Database), accessed via the public TMDb API.  
  This product uses the TMDb API but is **not endorsed or certified by TMDb**.

Please refer to each provider’s site for licensing and terms of use.

## License

This project is licensed under the [MIT License](LICENSE).

