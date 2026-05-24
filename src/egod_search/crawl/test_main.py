from datetime import datetime, timezone
from importlib.resources import files
from os import getenv
from typing import TypedDict
from anyio import Path
from asyncio import Lock, create_subprocess_exec, gather, sleep, to_thread
from asyncio.subprocess import DEVNULL
from re import MULTILINE, compile
from sys import executable, stderr, stdout
from tempfile import TemporaryDirectory
from unittest import main, SkipTest
from pathlib import Path as SyncPath

from yarl import URL

from .. import PACKAGE_NAME
from .._util import AsyncTestCase
from .main import PARSER_OPTION_DEFAULTS, main as main_main

if __name__ == "__main__":
    main()


class MainTestCase(AsyncTestCase):
    __slots__ = ("_lock", "_server_process")

    class _CommonOptions(TypedDict):
        pass

    _COMMON_OPTIONS = _CommonOptions()

    _MODIFICATION_DATE_AND_SIZE_REGEX = compile(
        r"^(?:(?:\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+(?:[+-][0-2]\d:[0-5]\d|Z))|(?:\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d(?:[+-][0-2]\d:[0-5]\d|Z))|(?:\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d(?:[+-][0-2]\d:[0-5]\d|Z))), \d+$",
        MULTILINE,
    )  # https://stackoverflow.com/a/3143231
    _SERVER_DIRECTORY = (
        Path(__file__).parent / "../../../examples/comp4321-hkust.github.io/testpages/"
    )
    _SERVER_START_TIME = 2
    _SERVER_URL = URL("http://localhost:8000/testpage.htm")
    _DATABASE_FILENAME = "database.db"
    _SUMMARY_FILENAME = "summary.txt"

    maxDiff = None

    # @override
    async def asyncSetUp(self) -> None:
        ret = await super().asyncSetUp()

        # Skip integration tests if test data is not available
        if not SyncPath(str(self._SERVER_DIRECTORY)).is_dir():
            raise SkipTest("Test data not available")

        ci = getenv("CI") == "true"
        self._lock = Lock()
        self._server_process = await create_subprocess_exec(
            executable,
            "-m",
            "http.server",
            str(self._SERVER_URL.port),
            "--bind",
            str(self._SERVER_URL.host),
            "--directory",
            str(self._SERVER_DIRECTORY),
            stdin=DEVNULL,
            stdout=stdout if ci else DEVNULL,
            stderr=stderr if ci else DEVNULL,
        )
        await sleep(self._SERVER_START_TIME)  # wait for the server to start

        return ret

    async def asyncTearDown(self) -> None:
        if hasattr(self, "_server_process"):
            self._server_process.kill()
        return await super().asyncTearDown()

    @classmethod
    async def read_expected_summary(cls, filename: str) -> str:
        """
        Read the expected summary file.
        """
        return await to_thread(
            (files(PACKAGE_NAME) / "res/tests/output_summary" / filename).read_text
        )

    @classmethod
    def normalize_summary(cls, summary: str) -> str:
        """
        Normalize summary for comparison.
        """
        return cls._MODIFICATION_DATE_AND_SIZE_REGEX.sub(
            f"{datetime.fromtimestamp(0, tz=timezone.utc).isoformat()}, 42", summary
        )

    async def test_output_summary_30_mp(self):
        with TemporaryDirectory() as tmp_dir:
            summary_path = Path(tmp_dir) / self._SUMMARY_FILENAME

            options = PARSER_OPTION_DEFAULTS.copy()
            options.update(
                summary_path=summary_path,
                page_count=30,
                **self._COMMON_OPTIONS,
            )
            async with self._lock:
                await main_main(
                    (self._SERVER_URL,),
                    **options,
                    database_path=Path(tmp_dir) / self._DATABASE_FILENAME,
                )

            self.assertEqual(
                *map(
                    self.normalize_summary,
                    await gather(
                        self.read_expected_summary("summary 30.txt"),
                        summary_path.read_text(),
                    ),
                )
            )

    async def test_output_summary_500_mp(self):
        with TemporaryDirectory() as tmp_dir:
            summary_path = Path(tmp_dir) / self._SUMMARY_FILENAME

            options = PARSER_OPTION_DEFAULTS.copy()
            options.update(
                summary_path=summary_path,
                page_count=500,
                **self._COMMON_OPTIONS,
            )
            async with self._lock:
                await main_main(
                    (self._SERVER_URL,),
                    **options,
                    database_path=Path(tmp_dir) / self._DATABASE_FILENAME,
                )

            self.assertEqual(
                *map(
                    self.normalize_summary,
                    await gather(
                        self.read_expected_summary("summary 500.txt"),
                        summary_path.read_text(),
                    ),
                )
            )
