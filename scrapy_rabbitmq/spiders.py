from . import connection

from scrapy import Spider
from scrapy.http import Request
from scrapy import signals
from scrapy.exceptions import DontCloseSpider


class RabbitMQMixin(object):
    """ A RabbitMQ Mixin used to read URLs from a RabbitMQ queue.
    """

    """
        Overrided in subclass
    """
    rabbitmq_key = None 
    redis_batch_size = None
    redis_encoding = None

    def __init__(self):
        self.server = None

    def setup_rabbitmq(self):
        """ Setup RabbitMQ connection.

            Call this method after spider has set its crawler object.
        :return: None
        """

        if not self.rabbitmq_key:
            self.rabbitmq_key = '{}:start_urls'.format(self.name)

        self.server = connection.from_settings(self.crawler.settings)
        self.crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)
        self.crawler.signals.connect(self.item_scraped, signal=signals.item_scraped)

    def make_ackable_request(self, url, method, properties):
        return [Request(url = url, meta = {"delivery_tag": method.delivery_tag})]

    def start_requests(self):
        return self.next_requests()

    def next_requests(self):
        """ Provides a request to be scheduled.
        :return: Request object or None
        """

        method, properties, body = self.server.basic_get(queue=self.rabbitmq_key)

        # TODO(royce): Remove print
        print(body)

        if body:
            return self.make_ackable_request(body, method, properties)

        return []

    def schedule_next_requests(self):
        """ Schedules a request, if exists.

        :return:
        """
        req = self.next_requests()

        for req in self.next_requests():
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self):
        """ Waits for request to be scheduled.

        :return: None
        """
        self.schedule_next_requests()
        raise DontCloseSpider

    def item_scraped(self, *args, **kwargs):
        """ Avoid waiting for spider.
        :param args:
        :param kwargs:
        :return: None
        """
        self.schedule_next_requests()


class RabbitMQSpider(RabbitMQMixin, Spider):
    """ Spider that reads urls from RabbitMQ queue when idle.
    """

    @classmethod
    def from_crawler(self, crawler, *args, **kwargs):
        obj = super(RabbitMQSpider, self).from_crawler(crawler, *args, **kwargs)
        obj.setup_rabbitmq()
        return obj
 

    def set_crawler(self, crawler):
        super(RabbitMQSpider, self).set_crawler(crawler)
        self.setup_rabbitmq()
