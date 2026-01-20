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
from types import SimpleNamespace # argparse.Namespace와 호환되도록 객체 생성
from typing import TYPE_CHECKING

from requests.exceptions import ConnectionError as RequestConnectionError
from requests.exceptions import RequestException, Timeout

from src.bunkr_utils import get_bunkr_status
from src.config import (
    AlbumInfo,
    DownloadInfo,
    SessionInfo,
    # parse_arguments, # YAML 사용으로 제거
)
from src.crawlers.crawler_utils import (
    extract_all_album_item_pages,
    get_download_info,
)
from src.downloaders.album_downloader import AlbumDownloader, MediaDownloader
from src.file_utils import create_download_directory, format_directory_name
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
    # Check the available disk space on the download path before starting the download
    if not args.disable_disk_check:
        check_disk_space(live_manager, custom_path=args.custom_path)

    validated_url = add_https_prefix(url)
    soup = await fetch_page(validated_url)

    if soup is None:
        log_unavailable_url(live_manager, validated_url)
        return

    album_id = get_album_id(validated_url) if check_url_type(validated_url) else None
    album_name = get_album_name(soup)

    directory_name = format_directory_name(album_name, album_id)
    download_path = create_download_directory(
        directory_name,
        custom_path=args.custom_path,
    )
    session_info = SessionInfo(
        args=args,
        bunkr_status=bunkr_status,
        download_path=download_path,
    )

    try:
        await handle_download_process(
            session_info, validated_url, soup, live_manager, args.max_retries,
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

    if not config.get('url'):
        print("Error: 'url' field is required in config.yaml")
        sys.exit(1)

    # 기존 argparse.Namespace 구조에 맞춰 매핑
    # 기존 코드들이 args.custom_path 등으로 접근하기 때문에 구조를 맞춰줍니다.
    args = SimpleNamespace(
        url=config['url'],
        custom_path=config.get('download', {}).get('custom_path'),
        exclude=config.get('filters', {}).get('ignore', []),
        include=config.get('filters', {}).get('include', []),
        disable_ui=config.get('system', {}).get('disable_ui', False),
        disable_disk_check=config.get('system', {}).get('disable_disk_check', False)
    )

    return args


async def main() -> None:
    """Initialize the download process."""
    clear_terminal()
    check_python_version()

    # 1. Load Config from YAML
    args = load_config_from_yaml()

    # 2. Debug Log: 파싱된 설정 출력
    print("----------- [Debug: Config Loaded] -----------")
    pprint.pprint(vars(args))
    print("----------------------------------------------\n")

    bunkr_status = get_bunkr_status()
    # args = parse_arguments() # 기존 인수 파싱 제거
    
    # UI 비활성화 옵션 적용
    live_manager = initialize_managers(disable_ui=args.disable_ui)

    try:
        with live_manager.live:
            await validate_and_download(
                bunkr_status,
                args.url, # YAML에서 불러온 URL 사용
                live_manager,
                args=args, # YAML 설정 객체 전달
            )
            live_manager.stop()

    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())