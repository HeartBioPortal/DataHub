"""GitHub bot that reviews PR metadata files and posts suggestions.

This module provides a very small proof-of-concept bot which inspects
``metadata.json`` and ``provenance.json`` files modified in a pull request and
comments on any missing required fields according to the bundled JSON schemas.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List

from github import Github


SCHEMA_DIR = Path(__file__).resolve().parents[1] / "schemas"


def _load_schema(path: Path) -> dict:
    with path.open() as fh:
        return json.load(fh)


METADATA_SCHEMA = _load_schema(SCHEMA_DIR / "metadata.schema.json")
PROV_SCHEMA = _load_schema(SCHEMA_DIR / "prov.schema.json")


@dataclass
class BotConfig:
    """Configuration required to operate on a pull request."""

    token: str
    repo_name: str
    pr_number: int


class MetadataAssistBot:
    """Analyse a pull request and comment on metadata issues."""

    def __init__(self, config: BotConfig) -> None:
        self.config = config
        self._gh = Github(config.token)
        self._repo = self._gh.get_repo(config.repo_name)
        self._pr = self._repo.get_pull(config.pr_number)

    # ------------------------------------------------------------------
    @staticmethod
    def _missing_fields(data: dict, schema: dict) -> List[str]:
        required = schema.get("required", [])
        return [field for field in required if field not in data]

    # ------------------------------------------------------------------
    def _analyse_files(self) -> List[str]:
        comments: List[str] = []
        for pr_file in self._pr.get_files():
            name = Path(pr_file.filename).name
            if name not in {"metadata.json", "provenance.json"}:
                continue
            raw = self._repo.get_contents(pr_file.filename, ref=self._pr.head.sha)
            data = json.loads(raw.decoded_content.decode())
            schema = METADATA_SCHEMA if name == "metadata.json" else PROV_SCHEMA
            missing = self._missing_fields(data, schema)
            if missing:
                comments.append(
                    f"{pr_file.filename}: missing fields: {', '.join(sorted(missing))}"
                )
        return comments

    # ------------------------------------------------------------------
    def run(self) -> None:
        """Run the bot and post the analysis as an issue comment."""

        issues = self._analyse_files()
        if issues:
            body = "Metadata Assist Report:\n" + "\n".join(f"- {i}" for i in issues)
        else:
            body = "Metadata Assist: all metadata files look good."

        self._pr.create_issue_comment(body)


def main(env: Iterable[str] | None = None) -> None:
    env = env or os.environ
    token = env.get("GITHUB_TOKEN")
    pr_number = env.get("PR_NUMBER")
    repo_name = env.get("GITHUB_REPOSITORY")

    if not (token and pr_number and repo_name):
        print("Missing environment variables")
        return

    config = BotConfig(token=token, repo_name=repo_name, pr_number=int(pr_number))
    MetadataAssistBot(config).run()


if __name__ == "__main__":
    main()
