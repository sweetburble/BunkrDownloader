"""Main module to read configuration and run the entire download process.

이 스크립트는 config.yaml 파일에 정의된 모든 URL(다중)을 순차적으로 
안전하게 다운로드합니다.
"""

import asyncio
import sys
from argparse import Namespace

# 이제 parse_arguments 대신 load_config_from_yaml을 가져옵니다.
from downloader import load_config_from_yaml, validate_and_download
from src.bunkr_utils import get_bunkr_status
from src.config import SESSION_LOG
from src.file_utils import read_file, write_file
from src.general_utils import check_python_version, clear_terminal
from src.managers.live_manager import initialize_managers


async def process_urls(urls: list[str], args: Namespace) -> None:
    """Validate and downloads items for a list of URLs."""
    bunkr_status = get_bunkr_status()
    live_manager = initialize_managers(disable_ui=args.disable_ui)

    with live_manager.live:
        for url in urls:
            await validate_and_download(bunkr_status, url, live_manager, args=args)
        live_manager.stop()


async def main() -> None:
    """Run the script and process URLs."""
    # 터미널 및 세션 로그 파일 초기화
    clear_terminal()
    write_file(SESSION_LOG)

    # 파이썬 버전 체크
    check_python_version()
    
    # 🌟 이제 모든 설정과 URL 리스트는 config.yaml에서 가져옵니다.
    args = load_config_from_yaml()

    if not args.urls:
        print("설정 파일(config.yaml)에 다운로드할 URL이 없습니다.")
        return

    # 가져온 모든 URL을 처리합니다.
    await process_urls(args.urls, args)


if __name__ == "__main__":
    try:
        asyncio.run(main())

    except KeyboardInterrupt:
        sys.exit(1)