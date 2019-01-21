# -*- coding: utf-8 -*-
from scrapy_redis.spiders import RedisSpider
import scrapy
import json
import re


class MeasurementSpider(RedisSpider):
    name = 'measurement'
    allowed_domains = ['atlas.ripe.net']
    redis_key = "measurement:start_urls"

    def parse(self, response):
        url_list_results = response.css('a[href*="results"]::attr(href)').extract()
        url_list_pages = response.css('a[href*="page="]::attr(href)').extract()
        for url in url_list_results:
            if re.search("https://atlas.ripe.net/api/v2/measurements/\d+/results/", url):
                yield scrapy.Request(url=url, callback=self.parse_result)
        for url in url_list_pages:
            if re.search("https://atlas.ripe.net/api/v2/measurements/\?page=.+", url):
                yield scrapy.Request(url=url, callback=self.parse)

    def parse_result(self, response):
        measurement_list = json.loads(response.body)
        for measurement in measurement_list:
            try:
                dst_addr = measurement["dst_addr"]
                from_addr = measurement["from"]
                prb_id = measurement["prb_id"]
                min_delay = measurement["min"]
            except:
                continue
            yield {
                "dst_addr": dst_addr,
                "from_addr": from_addr,
                "prb_id": prb_id,
                "min_delay": min_delay,
                "type": measurement["type"],
            }

