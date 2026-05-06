"""Python-based downloader for Bunkr albums and files.

Usage:
    Ensure 'config.yaml' is configured correctly, then run:
        python3 downloader.py
"""

from __future__ import annotations

import asyncio
import sys
import yaml  # YAML 파싱을 위해 추가
import pprint # 디버깅 출력을 위해 추가
from types import SimpleNamespace
from typing import TYPE_CHECKING

from requests.exceptions import ConnectionError as RequestConnectionError
from requests.exceptions import RequestException, Timeout

from src.bunkr_utils import get_bunkr_status
from src.config import (
    AlbumInfo,
    DownloadInfo,
    SessionInfo,
    SkippedReason,
)
from src.crawlers.crawler_utils import (
    extract_all_album_item_pages,
    get_download_info,
)
from src.downloaders.album_downloader import AlbumDownloader, MediaDownloader
from src.file_utils import (
    create_download_directory,
    format_directory_name,
    write_on_session_log,
)
from src.general_utils import (
    check_disk_space,
    check_python_version,
    clear_terminal,
    fetch_page,
)
from src.managers.live_manager import initialize_managers
from src.url_utils import (
    add_https_prefix,
    check_url_type,
    get_album_id,
    get_album_name,
    get_host_page,
    get_identifier,
    log_unavailable_url,
)

if TYPE_CHECKING:
    from argparse import Namespace
    from bs4 import BeautifulSoup
    from src.managers.live_manager import LiveManager


async def handle_download_process(
    session_info: SessionInfo,
    url: str,
    initial_soup: BeautifulSoup,
    live_manager: LiveManager,
    max_retries: int,
) -> None:
    """Handle the download process for a Bunkr album or a single item."""
    host_page = get_host_page(url)
    identifier = get_identifier(url, soup=initial_soup)

    # Album download
    if check_url_type(url):
        item_pages = await extract_all_album_item_pages(initial_soup, host_page, url)
        album_downloader = AlbumDownloader(
            session_info=session_info,
            album_info=AlbumInfo(album_id=identifier, item_pages=item_pages),
            live_manager=live_manager,
        )
        await album_downloader.download_album(max_retries=max_retries)

    # Single item download
    else:
        download_link, filename = await get_download_info(url, initial_soup)
        live_manager.add_overall_task(identifier, num_tasks=1)
        task = live_manager.add_task()

        media_downloader = MediaDownloader(
            session_info=session_info,
            download_info=DownloadInfo(
                item_url=url,
                download_link=download_link,
                filename=filename,
                task=task,
            ),
            live_manager=live_manager,
        )
        media_downloader.download()


async def validate_and_download(
    bunkr_status: dict[str, str],
    url: str,
    live_manager: LiveManager,
    args: Namespace | None = None,
) -> None:
    """Validate the provided URL, and initiate the download process."""
    if not getattr(args, "disable_disk_check", False):
        check_disk_space(live_manager, custom_path=getattr(args, "custom_path", None))

    validated_url = add_https_prefix(url)
    soup = await fetch_page(validated_url)

    if soup is None:
        write_on_session_log(
            f"Request error for {url}", reason=SkippedReason.SERVICE_UNAVAILABLE,
        )
        log_unavailable_url(live_manager, validated_url)
        return

    album_id = get_album_id(validated_url) if check_url_type(validated_url) else None
    album_name = get_album_name(soup)

    directory_name = format_directory_name(album_name, album_id)
    download_path = create_download_directory(
        directory_name,
        custom_path=getattr(args, "custom_path", None),
        no_download_folder=getattr(args, "no_download_folder", False),
    )
    
    session_info = SessionInfo(
        args=args,
        bunkr_status=bunkr_status,
        download_path=download_path,
    )

    try:
        await handle_download_process(
            session_info,
            validated_url,
            soup,
            live_manager,
            getattr(args, "max_retries", 3),
        )

    except (RequestConnectionError, Timeout, RequestException) as err:
        error_message = f"Error downloading from {url}: {err}"
        raise RuntimeError(error_message) from err


def load_config_from_yaml(config_path: str = "config.yaml") -> Namespace:
    """Load configuration from YAML and convert to Namespace object."""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Error: Configuration file '{config_path}' not found.")
        sys.exit(1)
    except yaml.YAMLError as exc:
        print(f"Error parsing YAML file: {exc}")
        sys.exit(1)

    # config.yaml에서 urls 목록을 가져오기 (단일 url 하위호환 유지)
    raw_urls = config.get('urls', [])
    raw_url = config.get('url', None)
    
    url_list = []
    if isinstance(raw_urls, list):
        url_list.extend(raw_urls)
    if raw_url and isinstance(raw_url, str):
        url_list.append(raw_url)
        
    # 빈 값 제거
    url_list = [u.strip() for u in url_list if u and u.strip()]

    if not url_list:
        print("Error: 'urls' field is empty or missing in config.yaml")
        sys.exit(1)

    # 기존 argparse 구조에 맞게 누락된 변수까지 포함하여 매핑
    args = SimpleNamespace(
        urls=url_list,
        custom_path=config.get('download', {}).get('custom_path'),
        no_download_folder=config.get('download', {}).get('no_download_folder', False),
        exclude=config.get('filters', {}).get('ignore', []),
        include=config.get('filters', {}).get('include', []),
        disable_ui=config.get('system', {}).get('disable_ui', False),
        disable_disk_check=config.get('system', {}).get('disable_disk_check', False),
        max_retries=config.get('system', {}).get('max_retries', 3)
    )

    return args


async def main() -> None:
    """Initialize the download process (단일 테스트 용도)."""
    clear_terminal()
    check_python_version()

    args = load_config_from_yaml()

    print("----------- [Debug: Config Loaded] -----------")
    pprint.pprint(vars(args))
    print("----------------------------------------------\n")

    bunkr_status = get_bunkr_status()
    live_manager = initialize_managers(disable_ui=args.disable_ui)

    try:
        with live_manager.live:
            # downloader.py 직접 실행 시 yaml의 첫 번째 URL만 다운로드 (테스트용)
            await validate_and_download(
                bunkr_status,
                args.urls[0], 
                live_manager,
                args=args,
            )
            live_manager.stop()

    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())