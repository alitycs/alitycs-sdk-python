#!/usr/bin/env ruby

require "pathname"
require "yaml"

root = Pathname.new(__dir__).parent
failures = []
read = ->(path) { root.join(path).read }
policy_data = YAML.safe_load(read.call(".coderabbit.yaml"), aliases: false)
docs = read.call("docs/coderabbit.md")
ci_data = YAML.safe_load(read.call(".github/workflows/ci.yml"), aliases: false)
release_data = YAML.safe_load(read.call(".github/workflows/release.yml"), aliases: false)
owner_entries = read.call(".github/CODEOWNERS").each_line.filter_map do |line|
  content = line.sub(/\s+#.*$/, "").strip
  next if content.empty? || content.start_with?("#")

  fields = content.split
  [fields.first, fields.drop(1)]
end.to_h

dig = lambda do |*keys|
  keys.reduce(policy_data) { |node, key| node.is_a?(Hash) ? node[key] : nil }
end

{
  %w[reviews request_changes_workflow] => true,
  %w[reviews review_progress] => true,
  %w[reviews fail_commit_status] => true,
  %w[reviews auto_review enabled] => true,
  %w[reviews auto_review auto_incremental_review] => true,
  %w[reviews tools github-checks enabled] => true,
}.each do |keys, expected|
  actual = dig.call(*keys)
  next if actual == expected

  failures << "policy setting #{keys.join('.')} must be #{expected}, found #{actual.inspect}"
end

auto_review = dig.call("reviews", "auto_review")
if auto_review.is_a?(Hash) && auto_review.key?("ignore_usernames")
  failures << "bot exclusions are forbidden"
end
failures << "custom gate workflow is forbidden" if root.join(".github/workflows/coderabbit-gate.yml").exist?
failures << "custom review signal is forbidden" if root.join(".github/workflows/coderabbit-review-event.yml").exist?
failures << "native status must not be documented as approval" unless docs.include?("completion signal, not an approval proxy")
failures << "custom gate must stay absent" unless docs.include?("No shared policy repository or custom GitHub App is required")

{
  "/.coderabbit.yaml" => "@bulanovdm",
  "/.github/" => "@bulanovdm",
  "/scripts/validate-coderabbit.sh" => "@bulanovdm",
  "/scripts/verify-workflow-pins.rb" => "@bulanovdm",
  "/scripts/test-coderabbit-policy.rb" => "@bulanovdm",
  "/requirements-dev.txt" => "@bulanovdm",
}.each do |path, expected_owner|
  actual_owners = owner_entries[path] || []
  failures << "missing CODEOWNER: #{path} #{expected_owner}" unless actual_owners.include?(expected_owner)
end

workflow_steps = lambda do |workflow|
  jobs = workflow["jobs"]
  next [] unless jobs.is_a?(Hash)

  jobs.values.flat_map do |job|
    steps = job.is_a?(Hash) ? job["steps"] : nil
    next [] unless steps.is_a?(Array)

    steps.filter_map { |step| step.is_a?(Hash) ? [job, step] : nil }
  end
end

shell_lines = lambda do |run|
  run.each_line.filter_map do |line|
    stripped = line.strip
    next if stripped.empty? || stripped.start_with?("#")

    stripped
  end
end

unconditional_exact_step = lambda do |job, step, expected_lines|
  job.is_a?(Hash) &&
    step.is_a?(Hash) &&
    !job.key?("if") &&
    !step.key?("if") &&
    !job.key?("continue-on-error") &&
    !step.key?("continue-on-error") &&
    step["run"].is_a?(String) &&
    shell_lines.call(step["run"]) == expected_lines
end

probe_command = "./scripts/test-coderabbit-policy.rb"
probe_job = {}
{
  "commented policy commands" => "# #{probe_command}",
  "echoed policy commands" => "echo #{probe_command}",
  "ignored policy failures" => "#{probe_command} || true",
  "commands in inactive functions" => "inactive() {\n  #{probe_command}\n}",
  "commands in false conditionals" => "if false; then\n  #{probe_command}\nfi",
}.each do |description, run|
  if unconditional_exact_step.call(probe_job, { "run" => run }, [probe_command])
    failures << "#{description} must not count"
  end
end
if unconditional_exact_step.call(probe_job, { "if" => "false", "run" => probe_command }, [probe_command])
  failures << "conditionally disabled policy steps must not count"
end

ci_steps = workflow_steps.call(ci_data)
{
  "./scripts/verify-workflow-pins.rb" => "CI must verify workflow pins",
  "./scripts/validate-coderabbit.sh" => "CI must validate CodeRabbit policy",
  "./scripts/test-coderabbit-policy.rb" => "CI must run governance tests",
}.each do |command, message|
  present = ci_steps.any? do |job, step|
    unconditional_exact_step.call(job, step, [command])
  end
  failures << "#{message} in a dedicated unconditional step" unless present
end

failures << "release permissions must default closed" unless release_data["permissions"] == {}
release_jobs = release_data["jobs"]
build_job = release_jobs["build"] if release_jobs.is_a?(Hash)
release_job = release_jobs["release"] if release_jobs.is_a?(Hash)
build_steps = build_job["steps"] if build_job.is_a?(Hash)
release_steps = release_job["steps"] if release_job.is_a?(Hash)
verify_step = build_steps&.find { |step| step.is_a?(Hash) && step["id"] == "verify_tag" }
recheck_step = release_steps&.find do |step|
  step.is_a?(Hash) && step["name"] == "Recheck immutable release tag"
end
create_step = release_steps&.find { |step| step.is_a?(Hash) && step["name"] == "Create GitHub Release" }
verify_lines = [
  'git fetch --no-tags --force origin "+refs/heads/main:refs/remotes/origin/main"',
  'if [[ "$(git cat-file -t "$GITHUB_REF")" != "tag" ]]; then',
  'echo "error: releases require an annotated tag" >&2',
  "exit 1",
  "fi",
  'tag_object="$(git rev-parse "${GITHUB_REF}^{tag}")"',
  'tag_commit="$(git rev-parse "${GITHUB_REF}^{commit}")"',
  '[[ "$tag_commit" == "$GITHUB_SHA" ]]',
  'git merge-base --is-ancestor "$tag_commit" refs/remotes/origin/main',
  'version="$(python -c \'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["project"]["version"])\')"',
  '[[ "$GITHUB_REF_NAME" == "v${version}" ]]',
  'PYTHONPATH=src python -c \'import alitycs, alitycs.client; assert alitycs.__version__ == alitycs.client.__version__ == "\'"$version"\'"\'',
  'printf \'tag_commit=%s\\ntag_object=%s\\n\' "$tag_commit" "$tag_object" >> "$GITHUB_OUTPUT"',
]
unless unconditional_exact_step.call(build_job, verify_step, verify_lines)
  failures << "release must require annotated tags"
end
recheck_lines = [
  'git fetch --force origin "+refs/tags/${GITHUB_REF_NAME}:refs/tags/${GITHUB_REF_NAME}"',
  '[[ "$(git rev-parse "refs/tags/${GITHUB_REF_NAME}^{tag}")" == "$EXPECTED_TAG_OBJECT" ]]',
  '[[ "$(git rev-parse "refs/tags/${GITHUB_REF_NAME}^{commit}")" == "$EXPECTED_TAG_COMMIT" ]]',
  '[[ "$GITHUB_SHA" == "$EXPECTED_TAG_COMMIT" ]]',
]
unless unconditional_exact_step.call(release_job, recheck_step, recheck_lines)
  failures << "release must recheck immutable tag"
end
create_lines = recheck_lines + [
  'gh release create "$GITHUB_REF_NAME" release/* --verify-tag --generate-notes',
]
unless unconditional_exact_step.call(release_job, create_step, create_lines)
  failures << "release must recheck immutable tag immediately before creation"
end
failures << "release workflow must not use concurrency" if release_data.key?("concurrency")

abort failures.join("\n") unless failures.empty?
puts "CodeRabbit governance policy passed."
