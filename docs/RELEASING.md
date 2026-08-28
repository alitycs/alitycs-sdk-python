# Releasing

1. Update `project.version` in `pyproject.toml` and move changelog entries into a dated release
   section in a pull request.
2. Run lint, tests, coverage, build, and governance checks.
3. Merge after CI and CodeRabbit review.
4. Create and push an annotated tag matching the package version.
5. The release workflow verifies reviewed `main`, builds the wheel and source distribution,
   rechecks immutable tag identity, attests the artifacts, creates the GitHub Release, and
   publishes to PyPI using trusted publishing.

The active `Immutable release tags` ruleset matches `refs/tags/v*`, blocks tag updates and
deletions, and has no bypass actors.

The `pypi` GitHub environment and PyPI trusted publisher must be configured before the first tag.
