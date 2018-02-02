import re
import tweepy
import urllib3
from tweepy import OAuthHandler
from elasticsearch import Elasticsearch
import sys

urllib3.disable_warnings()


def CreateIndex(index_name, body_name):
    es.indices.create(index=index_name, body=body_name, ignore=400)


def DeleteIndex(index_name):
    es.indices.delete(index=index_name, ignore=[400, 404])


def checkifIndexExists(index_name):
    if es.indices.exists(index=index_name):
        return 1
    else:
        return 0


# Index name
ESVSC = 'trial_elasticsearch'

# Establishing ElasticSearch connection
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# Configuration of the index
ESVSC_INDEX_CONFIGURATION = {
    "settings": {
        "number_of_shards": "1",
        "number_of_replicas": "1"
    },
    "mappings": {
        "twitter": {
            "properties": {
                "text": {"type": "text"},
                "tweet_id": {"type": "long"},
                "id_str": {"type": "text"},
                "created_at": {"type": "text"},
            }
        }
    }
}


class TwitterClient(object):
    def __init__(self):
        # keys and tokens from the Twitter Dev Console
        consumer_key = 'your_consumer_key'
        consumer_secret = 'your_consumer_secret'

        access_token = 'your_access_token'
        access_token_secret = 'your_access_token_secret'

        # attempt authentication
        try:
            self.auth = OAuthHandler(consumer_key, consumer_secret)  # create OAuthHandler object
            self.auth.set_access_token(access_token, access_token_secret)  # set access token and secret
            self.api = tweepy.API(self.auth)  # create tweepy API object to fetch tweets

        except Exception as e:
            print("Error: " + str(e))

    def __getitem__(self, key):
        return self.book[key]

    def clean_tweet(self, tweet):  # function to clean tweets of special symbols and other unnecessary stuff
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])| (\w+:\ / \ / \S+)", " ", tweet).split())

    def get_tweets(self, query, count):
        tweets = []  # empty list to store parsed tweets

        try:
            maximum = -1

            for i in range(1, count, 100):
                fetched_tweets = self.api.search(q=query, count=count,
                                                 max_id=str(maximum - 1))  # call twitter api to fetch tweets
                non_bmp_map = dict.fromkeys(range(0x10000, sys.maxunicode + 1), 0xfffd)

                for tweet in fetched_tweets:  # parsing tweets one by one
                    parsed_tweet = {'text': tweet.text}  # empty dictionary to store required params of a tweet
                    print(tweet.id)
                    print(tweet.text.translate(non_bmp_map))
                    print(i)

                    es.index(index=ESVSC, doc_type="twitter", body={"text": tweet.text,
                                                                    "tweet_id": tweet.id,
                                                                    "id_str": tweet.id_str,
                                                                    "created_at": tweet.created_at
                                                                    })

                    print("\n")

                    # can save any other fields if needed

                    if tweet.retweet_count > 0:  # appending parsed tweet to tweets list
                        if parsed_tweet not in tweets:  # if tweet has retweets, ensure that it is appended only once
                            tweets.append(parsed_tweet)
                    else:
                        tweets.append(parsed_tweet)

                maximum = fetched_tweets[-1].id

            return tweets  # return parsed tweets

        except tweepy.TweepError as e:
            print("Error : " + str(e))  # print error (if any)


def main():
    if not checkifIndexExists(ESVSC):
        CreateIndex(ESVSC, ESVSC_INDEX_CONFIGURATION)

    else:
        DeleteIndex(ESVSC)
        CreateIndex(ESVSC, ESVSC_INDEX_CONFIGURATION)

    api = TwitterClient()  # creating object of TwitterClient Class
    tweets = api.get_tweets(query='Donald Trump', count=5000)  # calling function to get tweets


if __name__ == "__main__":
    main()  # calling main function
