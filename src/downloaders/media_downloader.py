"""Module that provides tools to manage the downloading of individual files from Bunkr.

It supports retry mechanisms, progress tracking, and error handling for a robust
download experience.
"""

from __future__ import annotations

import random
import time
from pathlib import Path
from typing import TYPE_CHECKING

import requests
from requests import RequestException

from src.bunkr_utils import mark_subdomain_as_offline, subdomain_is_offline
from src.config import (
    DOWNLOAD_HEADERS,
    MAX_RETRIES,
    CompletedReason,
    DownloadInfo,
    FailedReason,
    HTTPStatus,
    SessionInfo,
    SkippedReason,
)
from src.file_utils import truncate_filename, write_on_session_log

from .download_utils import save_file_with_progress

if TYPE_CHECKING:
    from src.managers.live_manager import LiveManager


class MediaDownloader:
    """Manage the downloading of individual files from Bunkr URLs."""

    def __init__(
        self,
        session_info: SessionInfo,
        download_info: DownloadInfo,
        live_manager: LiveManager,
        retries: int = MAX_RETRIES,
    ) -> None:
        """Initialize the MediaDownloader instance."""
        self.session_info = session_info
        self.download_info = download_info
        self.live_manager = live_manager
        self.retries = retries

    def attempt_download(self, final_path: str) -> tuple[bool, str]:
        """Attempt to download the file with retries and return (failed_status, error_details)."""
        last_error_msg = "알 수 없는 에러가 발생했습니다."
        
        for attempt in range(self.retries):
            try:
                # 타임아웃 설정: (연결 타임아웃, 읽기 타임아웃)
                response = requests.get(
                    self.download_info.download_link,
                    stream=True,
                    headers=DOWNLOAD_HEADERS,
                    timeout=(20, 60),
                )
                response.raise_for_status()

                failed = save_file_with_progress(
                    response,
                    final_path,
                    self.download_info.task,
                    self.live_manager,
                )
                
                # 다운로드가 성공적으로 끝나면 루프 종료 (failed가 False면 성공)
                if not failed:
                    return False, ""

            except RequestException as req_err:
                # 구체적인 HTTP 상태 코드와 에러 메시지를 캡처합니다.
                status_code = req_err.response.status_code if req_err.response else "No Response"
                last_error_msg = f"HTTP Error ({status_code}): {str(req_err)}"
                
                # Exit the loop if not retrying
                if not self._handle_request_exception(req_err, attempt):
                    break

        # Download failed
        return True, last_error_msg

    def download(self) -> dict | None:
        """Handle the download process."""
        is_final_attempt = self.retries == 1
        is_offline = subdomain_is_offline(
            self.download_info.download_link,
            self.session_info.bunkr_status,
        )

        if is_offline and is_final_attempt:
            self.live_manager.update_log(
                event="Non-operational subdomain",
                details=f"The subdomain for {self.download_info.filename} is offline. "
                "Check the log file.",
            )
            self._finalize_download(SkippedReason.DOMAIN_OFFLINE)
            return None

        formatted_filename = truncate_filename(self.download_info.filename)
        final_path = Path(self.session_info.download_path) / formatted_filename

        # Skip download if the file exists or is blacklisted
        if self._skip_file_download(final_path):
            return None

        error_details = "알 수 없는 에러"
        # Attempt to download the file with retries
        try:
            failed_download, error_details = self.attempt_download(final_path)

        except requests.exceptions.ConnectionError as err:
            error_details = f"ConnectionError: {str(err)}"
            self.live_manager.update_log(
                event="Connection error",
                # 버그 수정: 누락되었던 f-string 포맷팅을 복구하고 상세 에러를 추가했습니다.
                details=f"Read timed out for {self.download_info.filename}. Details: {error_details}",
            )
            failed_download = True

        # Handle failed download after retries
        if failed_download:
            return self._handle_failed_download(
                is_final_attempt=is_final_attempt, 
                error_details=error_details
            )

        self.live_manager.update_summary(CompletedReason.DOWNLOAD_SUCCESS)
        return None

    # Private methods
    def _skip_file_download(self, final_path: str) -> bool:
        """Determine whether a file should be skipped during download."""
        ignore_list = getattr(self.session_info.args, "ignore", [])
        include_list = getattr(self.session_info.args, "include", [])

        def log_and_skip_event(reason: str) -> bool:
            """Log the skip reason and updates the task before."""
            self.live_manager.update_log(event="Skipped download", details=reason)
            self.live_manager.update_task(
                self.download_info.task,
                completed=100,
                visible=False,
            )
            return True

        # Check if the file already exists
        if Path(final_path).exists():
            self.live_manager.update_summary(SkippedReason.ALREADY_DOWNLOADED)
            return log_and_skip_event(
                f"{self.download_info.filename} has already been downloaded.",
            )

        # Check if the file is in the ignore list
        if ignore_list and any(
            word in self.download_info.filename for word in ignore_list
        ):
            self.live_manager.update_summary(SkippedReason.IGNORE_LIST)
            return log_and_skip_event(
                f"{self.download_info.filename} matches the ignore list.",
            )

        # Check if the file is not in the include list
        if include_list and all(
            word not in self.download_info.filename for word in include_list
        ):
            self.live_manager.update_summary(SkippedReason.INCLUDE_LIST)
            return log_and_skip_event(
                f"No included words found for {self.download_info.filename}.",
            )

        # Check if the subdomain is marked as offline
        if subdomain_is_offline(
            self.download_info.download_link, self.session_info.bunkr_status,
        ):
            self._finalize_download(SkippedReason.DOMAIN_OFFLINE)
            return log_and_skip_event(
                f"The subdomain for {self.download_info.download_link} has been "
                "previously marked as offline.",
            )

        # If none of the skip conditions are met, do not skip
        return False

    def _retry_with_backoff(self, attempt: int, *, event: str) -> bool:
        """Log error, apply backoff, and return True if should retry."""
        self.live_manager.update_log(
            event=event,
            details=f"{event} for {self.download_info.filename} "
            f"({attempt + 1}/{self.retries})...",
        )

        if attempt < self.retries - 1:
            delay = 3 ** (attempt + 1) + random.uniform(1, 3)  # noqa: S311
            time.sleep(delay)
            return True

        return False

    def _handle_request_exception(
        self, req_err: RequestException, attempt: int,
    ) -> bool:
        """Handle exceptions during the request and manages retries."""
        is_server_down = (
            req_err.response is None
            or req_err.response.status_code in (
                HTTPStatus.SERVER_DOWN,
                HTTPStatus.SERVICE_UNAVAILABLE,
            )
        )

        # Mark the subdomain as offline and exit the loop
        if is_server_down:
            marked_subdomain = mark_subdomain_as_offline(
                self.session_info.bunkr_status,
                self.download_info.download_link,
            )
            self.live_manager.update_log(
                event="No response",
                details=f"Subdomain '{marked_subdomain}' has been marked as offline.",
            )
            return False

        if req_err.response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            return self._retry_with_backoff(attempt, event="Retrying download")

        if req_err.response.status_code == HTTPStatus.BAD_GATEWAY:
            self.live_manager.update_log(
                event="Server error",
                details=f"Bad gateway for {self.download_info.filename}.",
            )
            # Setting retries to 1 forces an immediate failure on the next check.
            self.retries = 1
            return False

        # Do not retry, exit the loop
        self.live_manager.update_log(event="Request error", details=str(req_err))
        return False

    def _handle_failed_download(
        self, *, is_final_attempt: bool, error_details: str = ""
    ) -> dict | None:
        """Handle a failed download after all retry attempts and log the reason."""
        if not is_final_attempt:
            self.live_manager.update_log(
                event="Exceeded retry attempts",
                details=f"Max retries reached for {self.download_info.filename}. "
                f"Reason: {error_details}. "
                "It will be retried one more time after all other tasks.",
            )
            return {
                "id": self.download_info.task,
                "filename": self.download_info.filename,
                "download_link": self.download_info.download_link,
                "item_url": self.download_info.item_url,
                "last_error": error_details,  # 앨범 다운로더에 상세 에러 이유를 함께 넘깁니다.
            }

        self.live_manager.update_log(
            event="Download failed",
            details=f"Failed to download {self.download_info.filename}. "
            f"Reason: {error_details}. Check the log file.",
        )
        self._finalize_download(FailedReason.MAX_RETRIES_REACHED)
        return None

    def _finalize_download(
        self,
        reason: FailedReason | SkippedReason,
        *,
        completed: int | None = None,
    ) -> None:
        outcome = reason.__class__.__name__.replace("Reason", "")

        write_on_session_log(
            self.download_info,
            reason=reason,
            outcome=outcome,
        )

        self.live_manager.update_task(
            self.download_info.task,
            completed=completed,
            visible=False,
        )
        self.live_manager.update_summary(reason)