
# Github scraper

[Scrapy](https://scrapy.org/) is an open source and collaborative framework for extracting the data you need from websites.

[Selenium](https://www.selenium.dev/) is browser-based regression automation.

### Aim of Project

We should give one url of GitHub domain. It could be of anything having `github.com` domain like user repository, user home page, search page, advanced search page, etc. 

This scraper will analyse the given url and fetch related urls. For example, if we give url `https://github.com/Parth971` then it will fetch all repositories url of `Parth971` user.

## Downloading repositories

After we have all urls of repositories, we can download it using [Selenium](https://www.selenium.dev/), which will guarantee 100% download of files. After downloading repositories, we can extract it using simple python script. 

## Install requirements (Python 3.8)

    pip install -r requirements.txt

## Steps to Run Scraper
1. Paste GitHub url into `./inputs/urls.txt` file. (**Only one link must be present.**)
2. Go into `main` directory.
3. Start Scraping: `scrapy crawl git_spider`
   - You can see collected links at `outputs/collected_links.txt` file.
4. Start Downloading Zip files: `python download_zip.py`
   - You can see repositories zips downloaded at `RepoDownloads/` directory.
   - You can see list of downloaded zips files at `outputs/downloaded_link.txt` file.
5. Start Unzipping: `python unzip.py`
   - You can see unzipped folder at `RepoDownloads/` directory.
   - You can see list of unzipped files at `outputs/unzipped_repositories.txt` file.


## Steps to Duplicate Project
To use this project for different GitHub link, follow as below:
- Copy main project folder.
- Paste new GitHub url into `./inputs/urls.txt` file.
- Delete all files/folders inside `outputs/` and `RepoDownloads/` directory.
- Done. Now you can follow : `Steps to Run Scraper`.

**Note: You must empty outputs and RepoDownloads folder before stating scraping.** <br>


## Set WAITING_TIME for Anti-Ban
- Set value for WAITING_TIME variable in `.env` file located at `main` directory.
- Value of WAITING_TIME variable must be integer.
- Default value is 60 (in seconds).

## Set NUMBER_OF_LINKS_TO_SCRAP
- Set value for `NUMBER_OF_LINKS_TO_SCRAP` variable in `.env` file located at `main` directory.
- Value of `NUMBER_OF_LINKS_TO_SCRAP` variable must be integer.
- By default, it will scrap all links.

## Count number of links collected
- Count urls: `python utils.py`
- For search links like `Topics`, it will show url counts for each `topics`.