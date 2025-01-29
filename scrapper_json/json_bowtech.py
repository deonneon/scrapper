import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
from typing import Dict, Any, Set
from urllib.parse import urljoin, urlparse
from collections import deque
import re
import time
from urllib.robotparser import RobotFileParser


class SmartProductScraper:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        self.visited_urls = set()
        self.product_urls = set()
        self.robot_parser = None

    def read_robots_txt(self, root_url: str):
        """Read and parse the robots.txt file."""
        parsed_url = urlparse(root_url)
        robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"

        self.robot_parser = RobotFileParser()
        try:
            self.robot_parser.set_url(robots_url)
            self.robot_parser.read()
            print(f"Loaded robots.txt from {robots_url}")
        except Exception as e:
            print(f"Failed to load robots.txt from {robots_url}: {str(e)}")

    def can_fetch(self, url: str) -> bool:
        """Check if a URL can be fetched according to robots.txt."""
        if not self.robot_parser:
            return True  # Assume permission if robots.txt couldn't be read
        return self.robot_parser.can_fetch(self.headers["User-Agent"], url)

    def is_same_domain(self, url: str, root_domain: str) -> bool:
        """Check if URL belongs to the same domain as root URL."""
        return urlparse(url).netloc == urlparse(root_domain).netloc

    def is_product_url(self, url: str) -> bool:
        """Check if URL is likely a product page."""
        product_patterns = [
            r"/products/",
            r"/collections/.*/products/",
        ]
        return any(re.search(pattern, url) for pattern in product_patterns)

    def get_all_links(self, soup: BeautifulSoup, root_url: str) -> Set[str]:
        """Extract all valid links from the page and avoid image links."""
        links = set()
        for a in soup.find_all("a", href=True):
            url = urljoin(root_url, a["href"])
            if self.is_same_domain(url, root_url):
                clean_url = url.split("#")[0]
                clean_url = clean_url.split("?")[0]
                if not re.search(
                    r"\b\.(jpg|jpeg|png|gif|bmp|svg|webp)(\b|(?=\?))",
                    clean_url,
                    re.IGNORECASE,
                ):
                    links.add(clean_url)
        return links

    def crawl_site(self, root_url: str, max_pages: int = 1000) -> Set[str]:
        """Crawl the website to find all product pages."""
        queue = deque([root_url])
        self.visited_urls.clear()
        self.product_urls.clear()

        self.read_robots_txt(root_url)
        print(f"Starting crawl from {root_url}")

        while queue and len(self.visited_urls) < max_pages:
            current_url = queue.popleft()

            if current_url in self.visited_urls or not self.can_fetch(current_url):
                continue

            try:
                print(f"Crawling: {current_url}")
                response = requests.get(current_url, headers=self.headers)
                response.raise_for_status()

                self.visited_urls.add(current_url)

                if self.is_product_url(current_url):
                    self.product_urls.add(current_url)
                    print(f"Found product: {current_url}")

                soup = BeautifulSoup(response.text, "html.parser")
                new_links = self.get_all_links(soup, root_url)

                for link in new_links:
                    if link not in self.visited_urls and self.can_fetch(link):
                        queue.append(link)

            except Exception as e:
                print(f"Error crawling {current_url}: {str(e)}")

            time.sleep(1)

        print(f"Crawl complete. Found {len(self.product_urls)} products")
        return self.product_urls

    def extract_schema_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        schema_data = {}
        json_ld = soup.find_all("script", type="application/ld+json")

        for script in json_ld:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and "@type" in data:
                    if data["@type"] in ["Product", "WebPage"]:
                        schema_data.update(self._flatten_dict(data))
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get("@type") in [
                            "Product",
                            "WebPage",
                        ]:
                            schema_data.update(self._flatten_dict(item))
            except json.JSONDecodeError:
                continue

        if not schema_data:
            schema_data = self._extract_fallback_data(soup)

        return schema_data

    def _flatten_dict(
        self, d: Dict[str, Any], parent_key: str = "", sep: str = "_"
    ) -> Dict[str, Any]:
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k

            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                if all(isinstance(x, (str, int, float)) for x in v):
                    items.append((new_key, ", ".join(str(x) for x in v)))
                else:
                    for i, item in enumerate(v):
                        if isinstance(item, dict):
                            items.extend(
                                self._flatten_dict(
                                    item, f"{new_key}{sep}{i}", sep=sep
                                ).items()
                            )
            else:
                items.append((new_key, v))
        return dict(items)

    def _extract_fallback_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        data = {}

        meta_tags = {
            "title": ["og:title", "twitter:title", "title"],
            "description": ["og:description", "description", "twitter:description"],
            "price": ["product:price:amount", "og:price:amount"],
            "currency": ["product:price:currency", "og:price:currency"],
            "availability": ["product:availability", "og:availability"],
            "image": ["og:image", "twitter:image"],
        }

        for key, meta_names in meta_tags.items():
            for name in meta_names:
                meta = soup.find("meta", {"property": name}) or soup.find(
                    "meta", {"name": name}
                )
                if meta and meta.get("content"):
                    data[key] = meta.get("content")
                    break

        selectors = {
            "price": [".price", ".product-price", "[data-product-price]"],
            "title": ["h1", ".product-title", ".product-name"],
            "sku": [".sku", "[data-product-sku]"],
            "description": [".product-description", "#product-description"],
        }

        for key, selector_list in selectors.items():
            if key not in data:
                for selector in selector_list:
                    element = soup.select_one(selector)
                    if element:
                        data[key] = element.get_text(strip=True)
                        break

        return data

    def scrape_product(self, url: str) -> Dict[str, Any]:
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            product_data = self.extract_schema_data(soup)
            product_data["url"] = url

            return product_data

        except Exception as e:
            print(f"Error scraping {url}: {str(e)}")
            return {}

    def scrape_all_products(
        self, root_url: str, output_file: str = "products_bowtech.json"
    ):
        product_urls = self.crawl_site(root_url)

        all_products = []
        count = 0
        limit = 10

        for url in product_urls:
            if count >= limit:  # Stop after reaching the limit
                break

            try:
                print(f"Scraping product: {url}")
                product_data = self.scrape_product(url)
                if product_data:
                    for key, value in product_data.items():
                        if isinstance(value, str):
                            product_data[key] = " ".join(value.split())
                    all_products.append(product_data)
                    count += 1

                time.sleep(1)

            except Exception as e:
                print(f"Error scraping product {url}: {str(e)}")

        if all_products:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(all_products, f, indent=4, ensure_ascii=False)
            print(f"Data saved to {output_file}")
        else:
            print("No products were collected")


if __name__ == "__main__":
    root_url = "https://bowtecharchery.com"

    scraper = SmartProductScraper()
    scraper.scrape_all_products(root_url)
