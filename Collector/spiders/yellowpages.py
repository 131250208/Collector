# -*- coding: utf-8 -*-
import scrapy
from bs4 import BeautifulSoup
from scrapy_redis.spiders import RedisSpider


class YellowpagesSpider(RedisSpider):
    name = 'yellowpages'
    allowed_domains = ['www.yellowpages.com']
    redis_key = "yellowpages:start_urls"

    def parse(self, response):
        soup = BeautifulSoup(response.body, "lxml")
        a_list = soup.select("a[href*=state]")
        for a in a_list:
            url = "https://www.yellowpages.com/{}".format(a["href"])
            state = a.get_text()
            yield scrapy.Request(url, callback=self.parse_state, meta={"state": state})

    def parse_state(self, response):
        state = response.meta["state"]

        soup = BeautifulSoup(response.body, "lxml")
        div = soup.select_one("div.row")
        a_list = div.select('section.column a')
        for a in a_list:
            city = a.get_text()
            url_1st_page = "https://www.yellowpages.com{}".format(a["href"])
            yield scrapy.Request(url_1st_page, callback=self.parse_1st_page, meta={"state": state, "city": city})
            url_additional = "https://www.yellowpages.com{}/business-listings/1".format(a["href"])
            yield scrapy.Request(url_additional, callback=self.parse_additonal, meta={"state": state, "city": city})
            url_recent = "https://www.yellowpages.com{}/recent/1".format(a["href"])
            yield scrapy.Request(url_recent, callback=self.parse_additonal, meta={"state": state, "city": city})

    def parse_additonal(self, response):
        state = response.meta["state"]
        city = response.meta["city"]

        soup = BeautifulSoup(response.body, "lxml")
        secs_yp = soup.select("section.local-yp")
        a_list = []
        for sec in secs_yp:
            a_list.extend(sec.select("a"))

        for a in a_list:
            name = a.get_text()
            url = "https://www.yellowpages.com{}".format(a["href"])
            yield {
                "name": name,
                "location": {
                    "state": state,
                    "city": city,
                },
                "url": url,
            }

        # other pages
        a_list = soup.select("div.paginator div.holder a")
        for a in a_list:
            url = "https://www.yellowpages.com{}".format(a["href"])
            yield scrapy.Request(url, callback=self.parse_additonal, meta={"state": state, "city": city})

    def parse_1st_page(self, response):
        state = response.meta["state"]
        city = response.meta["city"]

        soup = BeautifulSoup(response.body, "lxml")
        sec_yp = soup.select_one("section#popular-businesses")
        a_list = sec_yp.select("a")

        for a in a_list:
            name = a.get_text()
            if name == "Additional Businesses" or name == "Recently Added Businesses":
                continue
            url = "https://www.yellowpages.com{}".format(a["href"])

            yield {
                "name": name,
                "location": {
                    "state": state,
                    "city": city,
                },
                "url": url,
            }

