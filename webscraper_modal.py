from dataclasses import dataclass
import modal

app = modal.App("webscraper")
playwright_image = modal.Image.debian_slim(python_version="3.13").run_commands(
    "apt-get update",
    "apt-get install -y software-properties-common",
    "apt-add-repository non-free",
    "apt-add-repository contrib",
    "pip install playwright boto3 Pillow",
    "playwright install-deps chromium",
    "playwright install chromium",
)


@dataclass
class PageData:
    response: bytes


@app.function(image=playwright_image, secrets=[modal.Secret.from_name("s3-secret")])
def getPageModal(url: str) -> dict[str, bool | str | dict[str, str] | int]:
    print(f"Starting web scrape for URL: {url}")
    from playwright.sync_api import sync_playwright, Route, Request
    from PIL import Image
    import boto3, os, time, urllib.parse, re, json

    cleanup = re.compile("([.]{2,}|[/]{2,})")

    print("Connecting to S3")
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["ENDPOINT_URL"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=None,
        config=boto3.session.Config(signature_version="s3v4"),  # type: ignore
    )
    out = {
        "type": "",
        "headers": {},
        "status": 0,
    }
    final_url = url

    def return_value(
        success: bool,
        url: str,
        final_url: str = "",
        path: str = "",
        status: int = 0,
        headers: dict[str, str] = {},
    ):
        return {
            "success": success,
            "url": url,
            "final_url": final_url,
            "path": path,
            "status": status,
            "headers": headers,
        }

    request_types: dict[str, str] = {}

    def requestFilter(route: Route, request: Request):
        print(f"Request filter: {request.resource_type} - {request.url}")
        request_types["request.url"] = request.resource_type

        match request.resource_type:
            case "image" | "media" | "font":
                print(f"Blocking resource: {request.resource_type} - {request.url}")
                route.abort()
            # case "document":
            # case "stylesheet":
            # case "script":
            # case "texttrack":
            # case "xhr":
            # case "fetch":
            # case "eventsource":
            # case "websocket":
            # case "manifest":
            # case "other":
            case _:
                print(f"Allowing resource: {request.resource_type} - {request.url}")
                route.fallback()

    print(f"Running scrape for URL: {url}")

    screenshot_file = "screenshot.webp"
    content_file = "content.html"
    body_file = "body"
    metadata_file = "metadata.json"

    with sync_playwright() as p:
        print("Launching browser")
        browser = p.chromium.launch()
        print("Creating new page")
        page = browser.new_page()
        print("Setting up request filtering")
        page.route("**/*", requestFilter)
        print(f"Navigating to URL: {url}")
        response = page.goto(url)
        if response is None:
            print("response is None")
            return return_value(success=False, url=url)
        print(f"Navigation completed, status: {response.status}")

        out["status"] = response.status
        out["headers"] = response.headers
        final_url = response.url
        print(f"Final URL after redirects: {final_url}")

        if final_url in request_types:
            out["type"] = request_types[final_url]
            print(f"Set main resource type: {out["type"]}")
        else:
            print(f"Main resource type not found")

        print(f"Extracted response status: {response.status}")
        if response.status < 200 or response.status > 299:
            print(f"Response status is not 2xx: {response.status}")
            browser.close()
            return return_value(
                success=False,
                url=url,
                final_url=final_url,
                status=response.status,
                headers=response.headers,
            )

        print(f"Extracted {len(out['headers'])} headers")

        try:
            print("Waiting for DOM content to load")
            page.wait_for_load_state("domcontentloaded", timeout=2000)
            print("Waiting for network idle state")
            page.wait_for_load_state("networkidle", timeout=2000)
            print("Page loaded...")
        except Exception as e:
            print(f"Wait timeout occurred for URL {url}: {e}")

        # links = page.eval_on_selector_all(
        #     "a[href]", "elements => elements.map(element => element.href)"
        # )
        # out["links"] = list(set(links))

        print(f"Taking screenshot")
        page.screenshot(path="/tmp/screenshot.png", full_page=False, type="png")

        img = Image.open("/tmp/screenshot.png", formats=["png"])
        img.save(f"/tmp/{screenshot_file}", "webp", optimize=True, quality=80)

        print(f"Saving response body")
        try:
            body_content = response.body()
            with open(f"/tmp/{body_file}", "wb") as f:
                f.write(body_content)
                f.close()
        except Exception as e:
            print(f"Failed to save response body: {e}")

        print(f"Extracting page content")
        try:
            with open(f"/tmp/{content_file}", "w") as f:
                f.write(page.content())
                f.close()
        except Exception as e:
            print(f"Failed to extract page content: {e}")

        print(f"Saving metadata")
        try:
            with open(f"/tmp/{metadata_file}", "w") as f:
                json.dump(out, f)
                f.close()
        except Exception as e:
            print(f"Failed to save metadata: {e}")

        print("Closing browser")
        browser.close()

    print(f"Processing URL path: {final_url}")
    url_s = urllib.parse.urlsplit(final_url, "https")
    path = url_s.geturl()
    path = path.removeprefix(url_s.scheme)
    path = path.removeprefix(":")
    path = path.removeprefix("//")
    path = urllib.parse.quote_plus(path, safe="/")
    old_path = ""
    while path != old_path:
        old_path = path
        path = cleanup.sub("", path)
    path = os.path.normpath(path)
    print(f"Normalized path: {path}")

    def saveToS3(file: str):
        print(f"Uploading file to S3: {path}/{file}")
        try:
            s3.upload_file(f"/tmp/{file}", "aletis", f"{path}/{file}")
        except Exception as e:
            print(f"Failed to upload file: {e}")

    saveToS3(screenshot_file)
    saveToS3(content_file)
    saveToS3(body_file)
    saveToS3(metadata_file)

    print(f"Web scrape completed successfully")
    return return_value(
        success=True, url=url, final_url=final_url, path=path, status=out["status"]
    )


@app.local_entrypoint()
def main():
    print("Starting local entrypoint for web scraper")
    urls = ["https://en.wikipedia.org/wiki/2023_in_film"]
    print(f"Processing {len(urls)} URLs: {urls}")
    for result in getPageModal.map(urls):
        print(f"Result: {result}")
    print("Local entrypoint completed")
