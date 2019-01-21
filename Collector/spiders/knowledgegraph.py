# -*- coding: utf-8 -*-
import scrapy
from bs4 import BeautifulSoup
import re
from scrapy_redis.spiders import RedisSpider
import logging


class KnowledgegraphSpider(RedisSpider):
    name = 'knowledgegraph'
    allowed_domains = ['www.google.com']
    redis_key = "knowledgegraph:start_urls"

    def parse(self, response):
        rel_org_name_set = set()
        text = response.body.decode("utf-8")
        soup = BeautifulSoup(text, "lxml")

        # it there an entity in google KG?
        div_kg_hearer = soup.select_one("div.kp-header")

        if div_kg_hearer is None:  # if there is no knowledge graph at the right, drop it
            return None

        enti_name = div_kg_hearer.select_one("div[role=heading] span")
        enti_name = enti_name.text if enti_name is not None else None
        if enti_name is None or "..." in enti_name:
            se = re.search('\["t-dhmk9MkDbvI",.*\[\["data",null,null,null,null,\[null,"\[\\\\"(.*)\\\\",', text)
            if se is not None:
                enti_name = se.group(1)
            else:
                return None

        # identify the type
        span_list = div_kg_hearer.select("span")
        enti_type = span_list[-1].text if len(span_list) > 1 else "unknown"

        # description from wikipedia
        des = soup.find("h3", text="Description")
        des_info = ""
        if des is not None:
            des_span = des.parent.select_one("span")
            des_info = des_span.text if des_span is not None else ""

        # extract attributes
        attr_tags = soup.select("div.Z1hOCe")
        attr_dict = {}
        for attr in attr_tags:
            attr_str = attr.get_text()
            se = re.search("(.*?)[:：](.*)", attr_str)
            if se is None:
                continue
            key_attr = se.group(1)
            val_attr = se.group(2)
            attr_dict[key_attr] = val_attr

        # relevant org name on current page
        a_reltype_list = soup.select("div.MRfBrb > a")
        for a in a_reltype_list:
            rel_org_name_set.add(a["title"].strip())

        # collect next urls e.g. : more x+
        div_list = soup.select("div.yp1CPe")
        next = []
        host = "https://www.google.com"
        for div in div_list:
            a_list = div.select("a.EbH0bb")
            for a in a_list:
                if "http" not in a["href"]:
                    next.append("%s%s" % (host, a["href"]))

        # crawl parent org
        a_parent_org = soup.find("a", text="Parent organization")
        if a_parent_org is not None:
            parent_str = a_parent_org.parent.parent.text.strip()
            parent_org = parent_str.split(":")[1]
            rel_org_name_set.add(parent_org.strip())

        # crawl subsidiaries
        a_subsidiaries = soup.find("a", text="Subsidiaries")
        if a_subsidiaries is not None:
            href = a_subsidiaries["href"]
            if "http" not in href:
                subsidiaries_str = a_subsidiaries.parent.parent.text.strip()
                subs = subsidiaries_str.split(":")[1].split(",")
                for sub in subs:
                    sub = sub.strip()
                    if sub == "MORE":
                        continue
                    rel_org_name_set.add(sub)
                next.append("%s%s" % (host, href))

        yield {"name": enti_name, "type": enti_type, "description": des_info, "attributes": attr_dict}

        # scrawl urls in list 'next'
        for url in next:
            yield scrapy.Request(url, callback=self.parse_relevant)

        rel_org_name_list = [org_name for org_name in rel_org_name_set if len(org_name) > 2]
        for q in rel_org_name_list:
            url = 'https://www.google.com/search?biw=1920&safe=active&hl=en&q=%s&oq=%s' % (q, q)
            yield scrapy.Request(url, callback=self.parse)

    def parse_relevant(self, response):
        soup = BeautifulSoup(response.body, "lxml")
        rel_org_name_set = set()
        # crawl items at the top
        a_list = soup.select("a.klitem")
        for a in a_list:
            rel_org_name = a["title"]
            rel_org_name_set.add(rel_org_name.strip())

        # crawl headings under the map if any
        heading_list = soup.select("div.VkpGBb")
        for heading in heading_list:
            heading_str = heading.select_one("div[role='heading']")
            rel_org_name_set.add(heading_str.get_text())

        rel_org_name_list = [org_name for org_name in rel_org_name_set if len(org_name) > 2]
        for q in rel_org_name_list:
            url = 'https://www.google.com/search?biw=1920&safe=active&hl=en&q=%s&oq=%s' % (q, q)
            yield scrapy.Request(url, callback=self.parse)

