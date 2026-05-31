"""Module that provides utilities for interacting with the Bunkr API."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import aiohttp
from yarl import URL

from src.config import BUNKR_API, JS_VARS_REGEX

if TYPE_CHECKING:
    from bs4 import BeautifulSoup


def unescape_js_path(value: str) -> str:
    """Unescape common JavaScript-escaped URL fragments."""
    return value.replace(r"\/", "/")


def extract_js_vars(soup: BeautifulSoup) -> dict[str, str]:
    """Extract runtime variables embedded in Bunkr inline JavaScript."""
    for script in soup.find_all("script"):
        if script.string and "var jsCDN" in script.string:
            matches = re.compile(JS_VARS_REGEX, re.DOTALL).findall(script.string)
            return {key: unescape_js_path(value).strip("\"'") for key, value in matches}
    return {}


async def get_api_response(soup: BeautifulSoup | None = None) -> str | None:
    """Fetch encryption data from the Bunkr API."""
    # Extract JS vars
    js_vars = extract_js_vars(soup)
    if not js_vars:
        return None

    js_cdn = js_vars.get("jsCDN")
    if not js_cdn:
        return None

    async with (
        aiohttp.ClientSession() as session,
        session.get(URL(BUNKR_API), params={"path": URL(js_cdn).path}) as response,
    ):
        sign_data = await response.json()

    token = sign_data.get("token")
    ex = sign_data.get("ex")
    if token and ex:
        return f"{js_cdn}?token={token}&ex={ex}"

    return js_cdn
