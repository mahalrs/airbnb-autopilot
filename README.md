# Airbnb Autopilot

Short-term rentals are gaining popularity in recent years, with Airbnb leading the change. When listing properties, one of the biggest challenges is finding a competitive listing price. In this project, we conducted various experiments to perform comparative analysis of different machine learning models to predict listing price.

We used the Airbnb Listings Dataset as the data source to train our machine learning model. This dataset has a total number of 494,954 records of Airbnb rental information with 89 features. Features include categorical features such as city, country, and room type, numerical features like the number of bedrooms, beds, and baths, and descriptive text features like neighborhood overview and summary.

## ML techniques used

- Data cleaning, exploratory data analysis, and feature engineering.
- NLP techniques to transform descriptive text features into categorical features.
- Supervised machine learning: simple regression models, CatBoost, XGBoost, and simple deep learning models.

## Conclusion

Based on our observations, we found that all models outperform the linear regression model. We found that the CatBoost model attains the best performance with a MAE of 35.12 and a R2 score of 0.73.

We also found that tree-based models performed better in our use case. In terms of training and tuning time, tree-based models took significantly less time compared to deep learning models, again proving that tree-based models are better fit for this problem.

Finally, we found that zipcode, accommodates, and room type are the most important features which is usually the case in real estate industry.
