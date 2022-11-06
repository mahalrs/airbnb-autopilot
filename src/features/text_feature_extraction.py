import numpy as np
import pandas as pd
import numpy as np

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

df=pd.read_csv('airbnb-listings.csv',encoding='utf-8',engine='python',sep=';')

def sentiment_vader(sentence):
    # Create a SentimentIntensityAnalyzer object.
    sid_obj = SentimentIntensityAnalyzer()

    sentiment_dict = sid_obj.polarity_scores(sentence)
    negative = sentiment_dict['neg']
    neutral = sentiment_dict['neu']
    positive = sentiment_dict['pos']
    compound = sentiment_dict['compound']

    if sentiment_dict['compound'] >= 0.05:
        overall_sentiment = "Positive"

    elif sentiment_dict['compound'] <= - 0.05:
        overall_sentiment = "Negative"

    else:
        overall_sentiment = "Neutral"

    return negative, neutral, positive, compound, overall_sentiment

textCols = ["Summary",'Description', "Neighborhood Overview", "Access", "Transit", "House Rules"]
# Host response time can be directly splitted in decision tree models

textFeatures = df.loc[:, textCols].map(lambda x: sentiment_vader(str(x)) if x!= np.nan else x)

textFeatures.to_csv('textFeatures.csv')


textCompound = df.loc[:, textCols].applymap(lambda x: sentiment_vader(str(x)) if x!= np.nan else x)

textCompound.to_csv('textCompound.csv')