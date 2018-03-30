import html
import logging
import json
import re
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from models import Tweet, Media, TelegramChat, Subscription, TwitterUser
from pprint import pprint
from collections import namedtuple
import threading


class TwitterStreamListener(StreamListener):
    def __init__(self, bot, tg_chat, tweet_parser):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.bot = bot
        self.tg_chat = tg_chat
        self.tweet_parser = tweet_parser
        super(TwitterStreamListener, self).__init__()

    def on_data(self, tweet):
        pprint(json.loads(tweet), indent=2)
        tweet = json.loads(tweet, object_hook=lambda d: namedtuple('X', d.keys())(*d.values()))
        tweet = self.tweet_parser.parse_tweet(tweet)
        self.logger.debug("- Got tweet #{} @{}: {}".format(tweet.id, tweet.user_name, tweet.text))
        self.bot.send_tweet(self.tg_chat, tweet)
        return True

    def on_error(self, status):
        print('error')
        print(status)


class FetchAndSendTweetsJob():
    def __init__(self, context=None):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.streams = {}
        self.threads = {}

    def stream_as_thread(self, bot, tg_chat):
        self.logger.debug('Starting stream for {}...'.format(tg_chat.chat_id))
        stream_listener = TwitterStreamListener(bot, tg_chat, self)
        bot_auth = bot.tw.auth
        auth = OAuthHandler(bot_auth.consumer_key, bot_auth.consumer_secret)
        auth.set_access_token(tg_chat.twitter_token, tg_chat.twitter_secret)
        stream = Stream(auth, stream_listener)
        subscriptions = list(Subscription.select().where(Subscription.tg_chat == tg_chat))
        user_ids = list(map((lambda x: str(x.tw_user.user_id)), subscriptions))
        self.streams[tg_chat.chat_id] = (stream, user_ids)
        stream.filter(follow=user_ids)

    def update(self):
        self.logger.debug("Cleaning up TelegramChats marked for deletion")
        for chat in TelegramChat.select().where(TelegramChat.delete_soon == True):
            chat.delete_instance(recursive=True)
            self.logger.debug("Deleting chat {}".format(chat.chat_id))

    def subscription_changed(self, bot, tg_chat):
        if tg_chat.chat_id in self.streams:
            self.streams[tg_chat.chat_id][0].disconnect()
            del self.streams[tg_chat.chat_id]
        thread = threading.Thread(target=self.stream_as_thread, args=(bot, tg_chat))
        thread.start()

    def run(self, bot):
        self.logger.debug("Fetching tweets...")
        tg_chats = list(TelegramChat.select().where(TelegramChat.twitter_secret is not None))

        for tg_chat in tg_chats:
            self.subscription_changed(bot, tg_chat)

    def parse_tweet(self, tweet) -> Tweet:
        t = Tweet(
            id=tweet.id,
            text=html.unescape(tweet.text),
            created_at=tweet.created_at,
            user_name=tweet.user.name,
            user_screen_name=tweet.user.screen_name,
        )

        if hasattr(tweet, 'retweeted_status'):
            t.text = u'\u267B' + ' @' + tweet.retweeted_status.user.screen_name + ': ' + html.unescape(
                tweet.retweeted_status.text)

        if hasattr(tweet, 'quoted_status'):
            t.text = re.sub(r' https://t\.co/[1-9a-zA-Z]+$', r'', t.text) + "\n"
            t.text += u'\u267B' + ' @' + tweet.quoted_status.user.screen_name + ': ' + html.unescape(
                tweet.quoted_status.text)
            tweet.entities.urls = []
            if 'extended_entities' in tweet.quoted_status:
                self.parse_tweet_media(t, tweet.quoted_status.extended_entities)

        if hasattr(tweet, 'extended_entities'):
            self.parse_tweet_media(t, tweet.extended_entities)
        elif tweet.entities.urls:
            t.link_url = tweet.entities.urls[0].expanded_url

        for url_entity in tweet.entities.urls:
            expanded_url = url_entity.expanded_url
            indices = url_entity.indices
            display_url = tweet.text[indices[0]:indices[1]]
            t.text = t.text.replace(display_url, expanded_url)

        return t

    def parse_tweet_media(self, tweet: Tweet, extended_entities: list):
        for entity in extended_entities.media:
            tweet.text = tweet.text.replace(entity.url, '')
            if 'video_info' in entity:
                video_urls = entity.video_info.variants
                video_url = max([video for video in video_urls if ('bitrate') in video], key=lambda x: x.bitrate).url
                tweet.media_list.append(Media('video', video_url))
                self.logger.debug("- - Found video URL in tweet: " + video_url)
            else:
                photo_url = entity.media_url_https
                tweet.media_list.append(Media('photo', photo_url))
                self.logger.debug("- - Found photo URL in tweet: " + photo_url)
