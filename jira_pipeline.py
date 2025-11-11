import dlt

from jira import jira, jira_issues


def main() -> None:
    """Run the jira pipeline.

    This function is exported as an entry point so tooling like `uv run`
    can execute the pipeline by the name `jira_pipeline`.
    """
    pipeline = dlt.pipeline(
        pipeline_name="jira_pipeline"
    )

    load_info = pipeline.run(
        jira().with_resources("issues", "issue_histories", "issue_comments", "issue_sprints", "all_sprints")
    )
    print(load_info)


if __name__ == "__main__":
    main()
