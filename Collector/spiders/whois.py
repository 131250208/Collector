import scrapy
import re
import json
from scrapy_redis.spiders import RedisSpider


class WhoisSpider(RedisSpider):
    name = 'whois'
    redis_key = "whois:start_urls"
    # start_urls = ["https://whois.arin.net/rest/ip/8.8.8.8.json"]

    def parse(self, response):
        text = response.body.decode("utf-8")
        handle_json = json.loads(text)
        handle = handle_json["net"]["handle"]["$"]

        ori_url = response.request.url
        ip = re.search("https://whois.arin.net/rest/ip/(.*?).json", ori_url).group(1)

        url = "https://whois.arin.net/rest/net/%s/pft.json?s=%s" % (handle, ip)
        yield scrapy.Request(url, callback=self.parse_arin, meta={"ip": ip})

    def parse_arin(self, response):
        ip = response.meta["ip"]
        text = response.body.decode("utf-8")

        name = None
        start_address = None
        end_address = None

        json_whois = json.loads(text)["ns4:pft"]

        try:
            if "org" in json_whois:
                org = json_whois["org"]
                name = org["name"]["$"]
            if "customer" in json_whois:
                customer = json_whois["customer"]
                name = customer["name"]["$"]

            if "net" in json_whois:
                start_address = json_whois["net"]["startAddress"]["$"]
                end_address = json_whois["net"]["endAddress"]["$"]
        except KeyError:
            return None

        if name is None:
            return

        if name in ["Asia Pacific Network Information Centre",
                    "Latin American and Caribbean IP address Regional Registry",
                    "African Network Information Center"]: # the other four
            return

        yield {
            "start_address": start_address,
            "end_address": end_address,
            "org_name": name,
        }

        '''
        postpone
        '''
        # if "Asia Pacific Network Information Centre" in name:
        #     url = "http://wq.apnic.net/query?searchtext=%s" % ip
        #     yield scrapy.Request(url, callback=self.parse_apnic, meta={"ip": ip})
        #
        if "RIPE Network Coordination Centre" in name:
            url = "https://rest.db.ripe.net/search.json?source=ripe&query-string=%s" % ip # &source=apnic-grs
            yield scrapy.Request(url, callback=self.parse_ripe, meta={"ip": ip})
        #
        # if "Latin American and Caribbean IP address Regional Registry" in name:
        #     url = "https://rdap.registro.br/ip/%s" % ip
        #     yield scrapy.Request(url, callback=self.parse_ripe, meta={"ip": ip})
        #
        # if "African Network Information Center" in name:
        #     pass

    '''
    postpone
    '''
    # def parse_lanic(self, response):
    #     text = response.body.decode("utf-8")
    #     json_whois = json.loads(text)
    #     list_vcard = json_whois["entities"][0]["vcardArray"][1]
    #     for c in list_vcard:
    #         if c[0] == "fn":
    #             name = c[3]
    #             yield {
    #                 "ip": response.meta["ip"],
    #                 "name": name,
    #             }
    #
    def parse_ripe(self, response):
        text = response.body.decode("utf-8")
        try:
            json_res = json.loads(text)
            list_object = json_res["objects"]["object"]
            item = {}
            for ob in list_object:
                if ob["type"] == "organisation":
                    list_attr = ob["attributes"]["attribute"]
                    for attr in list_attr:
                        if attr["name"] == "org-name":
                            name = attr["value"]
                            item["org_name"] = name
                elif ob["type"] == "inetnum":
                    list_attr = ob["attributes"]["attribute"]
                    for attr in list_attr:
                        if attr["name"] == "inetnum":
                            se = re.search("(.*?) - (.*)", attr["value"])
                            item["start_address"] = se.group(1)
                            item["end_address"] = se.group(2)

            assert "org_name" in item and "start_address" in item and "end_address" in item
            yield item
        except Exception:
            return None
    #
    # def parse_apnic(self, response):
    #     text = response.body.decode("utf-8")
    #     json_whois = json.loads(text)
    #
    #     try:
    #         for entry in json_whois:
    #             if entry["type"] == "object" and entry["objectType"] == "inetnum":
    #                 attrs = entry["attributes"]
    #                 for attr in attrs:
    #                     if attr["name"] == "descr":
    #                         name = attr["values"][0]
    #                         yield {
    #                             "ip": response.meta["ip"],
    #                             "name": name
    #                         }
    #     except Exception:
    #         return None