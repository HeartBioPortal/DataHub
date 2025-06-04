"""Skeleton GitHub App that comments on pull requests with metadata suggestions."""
import os
from github import Github


def main():
    token = os.getenv("GITHUB_TOKEN")
    pr_number = os.getenv("PR_NUMBER")
    repo_name = os.getenv("GITHUB_REPOSITORY")
    if not token or not pr_number or not repo_name:
        print("Missing environment variables")
        return

    gh = Github(token)
    repo = gh.get_repo(repo_name)
    pr = repo.get_pull(int(pr_number))

    # TODO: add LLM logic here. For now, just post a placeholder comment.
    pr.create_issue_comment("Metadata Assist: please ensure provenance.json includes all required fields.")


if __name__ == "__main__":
    main()
