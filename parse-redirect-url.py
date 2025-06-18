import asyncio
import json

import pandas as pd
from playwright.async_api import async_playwright


async def get_final_url(browser, url, retries=3):
    page = await browser.new_page()

    final_result = None
    for attempt in range(retries):
        try:
            await page.goto(url, wait_until="load", timeout=60_000)
            final_url = page.url

            if url == final_url:
                print(f"[INFO] No redirection for {url}\n")
                final_result = {
                    "original_url": url,
                    "final_url": final_url,
                    "error": "No redirection",
                }
                break

            print(f"Original: {url}")
            print(f"Final:    {final_url}\n")
            final_result = {
                "original_url": url,
                "final_url": final_url,
                "error": None,
            }
            break
        except Exception as e:
            print(f"[RETRY {attempt+1}] {url} - {e}")
            await asyncio.sleep(2)

    await page.close()

    if final_result is None:
        print(f"[ERROR] Gagal mengambil URL: {url}")
        return {
            "original_url": url,
            "final_url": None,
            "error": f"Failed after {retries} retries",
        }
    return final_result


async def scrape_url_group(p, url_info):
    results = []

    browser = await p.chromium.launch(headless=True)

    start = url_info.get("start", 1)
    end = url_info.get("end", 1)
    if start > end:
        print(
            f"[ERROR] Invalid range for {url_info['title']}: start ({start}) > end ({end})"
        )
        return
    if start < 1 or end < 1:
        print(
            f"[ERROR] Invalid range for {url_info['title']}: start ({start}) or end ({end}) is less than 1"
        )
        return

    for number in range(start, end + 1):
        url = url_info["parent_url"].format(number=number)
        result = await get_final_url(browser, url)

        if result and result["error"] is not None:
            continue

        if result and result["final_url"] and "error" in result["final_url"]:
            continue

        results.append(result)

        await asyncio.sleep(1)

    await browser.close()

    # Save results
    filename = f"outputs/{url_info['title']}.csv"

    df = pd.DataFrame(results)
    df.to_csv(filename, index=False)
    print(f"[DONE] Saved to {filename}")


async def main():
    with open("urls.json", "r") as f:
        urls = json.load(f)

    if not urls:
        print("No URLs found to scrape.")
        return

    async with async_playwright() as p:
        tasks = [scrape_url_group(p, url_info) for url_info in urls]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
