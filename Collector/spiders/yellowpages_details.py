from bs4 import BeautifulSoup
from scrapy_redis.spiders import RedisSpider


class YellowpagesDetailsSpider(RedisSpider):
    name = 'yellowpages_details'
    allowed_domains = ['www.yellowpages.com']
    redis_key = "yellowpages_details:start_urls"

    def parse(self, response):
        soup = BeautifulSoup(response.body, "lxml")
        item = {}
        name = soup.select_one("div.sales-info > h1")
        if name:
            item["name"] = name.get_text()
        address = soup.select_one("div.contact h2.address")
        if address:
            item["address"] = address.get_text()

        phone = soup.select_one("div.contact p.phone")
        if phone:
            item["phone"] = phone.get_text()

        loc_div = soup.select_one("div#bpp-static-map")
        if loc_div:
            lon = float(loc_div["data-lng"])
            lat = float(loc_div["data-lat"])
            item["coordinate"] = {
                "longitude": lon,
                'latitude': lat,
            }

        website_link = soup.select_one("a.website-link")
        if website_link:
            item["website_link"] = website_link["href"]

        years_in_business = soup.select_one("div.years-in-business div.number")
        if years_in_business:
            item["years_in_business"] = int(years_in_business.get_text())

        business_detail_sec = soup.select_one("section#business-info")
        if business_detail_sec:
            dt_list = business_detail_sec.select("dt")
            dd_list = business_detail_sec.select("dd")
            key_list = [dt.get_text().strip() for dt in dt_list]
            val_list = []
            for dd in dd_list:
                if "class" in dd.attrs and "extra-phones" in dd.attrs["class"]:
                    phones = []
                    for p in dd.select("p"):
                        phones.append(p.get_text())
                    val_list.append(", ".join(phones))
                elif "class" in dd.attrs and "weblinks" in dd.attrs["class"]:
                    links = []
                    a_list = dd.select("p > a")
                    for a in a_list:
                        links.append({
                            "text": a.get_text(),
                            "url": a["href"]
                        })
                    val_list.append(links)
                else:
                    val_list.append(dd.get_text())

            business_details = dict(zip(key_list, val_list))
            item["details"] = business_details

        yield item
