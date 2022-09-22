# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class GitScrapperItem(scrapy.Item):
    url = scrapy.Field()
    page_url = scrapy.Field()
    next_url = scrapy.Field()
