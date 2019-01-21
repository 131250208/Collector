#@IgnoreInspection BashAddShebang
FROM python:3.6-onbuild

ENTRYPOINT ["scrapy"]
CMD ["crawl", "yellowpages_details"]
