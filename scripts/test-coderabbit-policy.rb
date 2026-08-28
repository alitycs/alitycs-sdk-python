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
}.each do |path, expected_owner|
  actual_owners = owner_entries[path] || []
  failures << "missing CODEOWNER: #{path} #{expected_owner}" unless actual_owners.include?(expected_owner)
end

workflow_runs = lambda do |workflow|
  jobs = workflow["jobs"]
  next [] unless jobs.is_a?(Hash)

  jobs.values.flat_map do |job|
    steps = job.is_a?(Hash) ? job["steps"] : nil
    next [] unless steps.is_a?(Array)

    steps.filter_map { |step| step.is_a?(Hash) && step["run"].is_a?(String) ? step["run"] : nil }
  end
end

shell_lines = lambda do |run|
  run.each_line.filter_map do |line|
    stripped = line.strip
    next if stripped.empty? || stripped.start_with?("#")

    stripped
  end
end

executable_reference = lambda do |runs, command|
  invocation = /\A(?:command\s+|exec\s+)?#{Regexp.escape(command)}\z/
  runs.any? { |run| shell_lines.call(run).any? { |line| line.match?(invocation) } }
end

probe_command = "./scripts/test-coderabbit-policy.rb"
failures << "commented policy commands must not count" if executable_reference.call(["# #{probe_command}"], probe_command)
failures << "echoed policy commands must not count" if executable_reference.call(["echo #{probe_command}"], probe_command)
failures << "ignored policy failures must not count" if executable_reference.call(["#{probe_command} || true"], probe_command)

ci_runs = workflow_runs.call(ci_data)
{
  "./scripts/verify-workflow-pins.rb" => "CI must verify workflow pins",
  "./scripts/validate-coderabbit.sh" => "CI must validate CodeRabbit policy",
  "./scripts/test-coderabbit-policy.rb" => "CI must run governance tests",
}.each do |command, message|
  failures << message unless executable_reference.call(ci_runs, command)
end

failures << "release permissions must default closed" unless release_data["permissions"] == {}
release_jobs = release_data["jobs"]
build_steps = release_jobs.dig("build", "steps") if release_jobs.is_a?(Hash)
release_steps = release_jobs.dig("release", "steps") if release_jobs.is_a?(Hash)
verify_step = build_steps&.find { |step| step.is_a?(Hash) && step["id"] == "verify_tag" }
recheck_step = release_steps&.find do |step|
  step.is_a?(Hash) && step["name"] == "Recheck immutable release tag"
end
verify_run = verify_step.is_a?(Hash) ? verify_step["run"].to_s : ""
recheck_run = recheck_step.is_a?(Hash) ? recheck_step["run"].to_s : ""
verify_lines = shell_lines.call(verify_run)
recheck_lines = shell_lines.call(recheck_run)
tag_guard = 'if [[ "$(git cat-file -t "$GITHUB_REF")" != "tag" ]]; then'
guard_start = verify_lines.index(tag_guard)
guard_end = guard_start && verify_lines.each_index.find { |index| index > guard_start && verify_lines[index] == "fi" }
guard_fails = guard_start && guard_end && verify_lines[(guard_start + 1)...guard_end].any? do |line|
  line.match?(/\Aexit\s+[1-9][0-9]*\z/)
end
unless guard_fails &&
    verify_lines.include?('tag_object="$(git rev-parse "${GITHUB_REF}^{tag}")"') &&
    verify_lines.include?('tag_commit="$(git rev-parse "${GITHUB_REF}^{commit}")"') &&
    verify_lines.include?('[[ "$tag_commit" == "$GITHUB_SHA" ]]')
  failures << "release must require annotated tags"
end
unless recheck_lines.include?(
  '[[ "$(git rev-parse "refs/tags/${GITHUB_REF_NAME}^{tag}")" == "$EXPECTED_TAG_OBJECT" ]]',
) && recheck_lines.include?(
  '[[ "$(git rev-parse "refs/tags/${GITHUB_REF_NAME}^{commit}")" == "$EXPECTED_TAG_COMMIT" ]]',
) && recheck_lines.include?('[[ "$GITHUB_SHA" == "$EXPECTED_TAG_COMMIT" ]]')
  failures << "release must recheck immutable tag"
end
failures << "release workflow must not use concurrency" if release_data.key?("concurrency")

abort failures.join("\n") unless failures.empty?
puts "CodeRabbit governance policy passed."
