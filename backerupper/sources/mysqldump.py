import logging
import subprocess

from backerupper.sources.abstract import AbstractSource

_LOGGER = logging.getLogger()

class MySQLSource(AbstractSource):

    SYSTEM_DATABASES = [
        "information_schema",
        "performance_schema",
        "mysql",
        "sys"
    ]

    def __init__(
            self,
            min_chunk_size: int,
            hostname: str,
            port: int,
            username: str,
            password: str,
            ignored_databases: list[str] | None = None
        ) -> None:
        super().__init__(min_chunk_size)

        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.ignored_databases = ignored_databases or self.SYSTEM_DATABASES

        self._process: subprocess.Popen | None = None
        self._completed = False

    def start(self):
        self._process = subprocess.Popen(
            ["mysqldump"]
        )
    
    def abort(self):
        ...

    def join(self):
        ...
    
    def read_data(self):
        return b"0x00" * self.min_chunk_size
    
    def _list_databases(self):
        cmd = [
            "mysql",
            f"-u{self.username}",
            f"-p{self.password}",
            "-N",   # skip column names
            "-e", "SHOW DATABASES;"
        ]
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        all_dbs = result.stdout.strip().split("\n")
        # Filter out system databases
        user_dbs = [db for db in all_dbs if db not in self.ignored_databases]

        _LOGGER.debug("Found databases: %s", user_dbs)

        return user_dbs

    @property
    def name(self):
        return f"MySQLSource {self.username}@{self.hostname}:{self.port}"
    
    @property
    def completed(self):
        return self._completed
