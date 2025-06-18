import asyncio
import glob
import json
import os
import sys

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright


async def scrape_page_content(page, url):
    await page.goto(url, timeout=60_000)

    previous_height = None
    while True:
        current_height = await page.evaluate("document.body.scrollHeight")
        if previous_height == current_height:
            break

        previous_height = current_height

        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1)

    data = {
        "title": await page.title(),
        "url": url,
        "email": None,
        "whatsapp": None,
        "instagram": None,
        "address": None,
        "map_url": None,
    }

    html = await page.content()

    soup = BeautifulSoup(html, "html.parser")

    for a in soup.find_all("a", href=True):
        if a["href"].startswith("mailto:"):
            data["email"] = a["href"].replace("mailto:", "")
        if a["href"].startswith("https://wa.me/"):
            data["whatsapp"] = a["href"].replace("https://wa.me/", "")
        if a["href"].startswith("https://www.instagram.com/"):
            data["instagram"] = a["href"]

    script_tag_schema_org_graph = soup.find(
        "script", {"type": "application/ld+json", "id": "schema-org-graph"}
    )
    if not script_tag_schema_org_graph:
        return data

    json_ld = json.loads(script_tag_schema_org_graph.string)
    extracted_data = extract_json_graph(json_ld)

    data["address"] = extracted_data.get("streetAddress")
    data["map_url"] = extracted_data.get("hasMap")

    return data


async def scrape_one_url(browser, url):
    page = await browser.new_page()
    try:
        data = await scrape_page_content(page, url)
        print(f"[OK] {url}")
        return data
    except Exception as e:
        print(f"[ERROR] {url} - {e}")
        return {"url": url, "error": str(e)}
    finally:
        await page.close()


async def run_scraper(urls: list[str], max_concurrent_requests=5):
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)

        # concurrent requests limit, handled by Semaphore gessss
        sem = asyncio.Semaphore(max_concurrent_requests)

        async def bounded_scrape(url):
            async with sem:
                return await scrape_one_url(browser, url)

        tasks = [bounded_scrape(url) for url in urls]
        results = await asyncio.gather(*tasks)

        await browser.close()

        filename = "outputs/result/scraped_result.csv"

        df = pd.DataFrame(results)
        df.to_csv(filename, index=False)

        print(f"Data saved to {filename}")


def load_urls_from_file(filename: str) -> list[str]:
    df = pd.read_csv(filename)
    return df["final_url"].tolist() if "final_url" in df.columns else []


def load_output_files(directory: str) -> list[str]:
    return glob.glob(os.path.join(directory, "*.csv"))


def extract_json_graph(json_ld):
    data = json.loads(json_ld) if isinstance(json_ld, str) else json_ld
    if not isinstance(data, dict):
        return {"streetAddress": None, "hasMap": None}

    graph = data.get("@graph", [])

    for item in graph:
        if "Hotel" in item.get("@type", []) and "address" in item:
            address = item["address"]
            street = address.get("streetAddress") if isinstance(address, dict) else None
            map_url = item.get("hasMap")

            return {"streetAddress": street, "hasMap": map_url}

    return {"streetAddress": None, "hasMap": None}


if __name__ == "__main__":
    urls = []

    output_files = load_output_files("outputs")
    for file in output_files:
        urls.extend(load_urls_from_file(file))

    urls = list(set(urls))
    if not urls:
        print("No URLs found to scrape.")
        sys.exit(1)

    print(f"Found {len(urls)} unique URLs to scrape.")

    # urls = urls[:1]
    # print(f"Scraping {len(urls)} URLs...")

    asyncio.run(run_scraper(urls))
