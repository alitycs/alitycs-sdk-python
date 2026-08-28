#!/usr/bin/env ruby

require "pathname"

root = Pathname.new(__dir__).parent
failures = []
read = ->(path) { root.join(path).read }
policy = read.call(".coderabbit.yaml")
docs = read.call("docs/coderabbit.md")
ci = read.call(".github/workflows/ci.yml")
release = read.call(".github/workflows/release.yml")
owners = read.call(".github/CODEOWNERS")

[
  "request_changes_workflow: true",
  "review_progress: true",
  "fail_commit_status: true",
  "enabled: true",
  "auto_incremental_review: true",
  "github-checks:",
].each { |value| failures << "missing policy setting: #{value}" unless policy.include?(value) }

failures << "bot exclusions are forbidden" if policy.include?("ignore_usernames:")
failures << "custom gate workflow is forbidden" if root.join(".github/workflows/coderabbit-gate.yml").exist?
failures << "custom review signal is forbidden" if root.join(".github/workflows/coderabbit-review-event.yml").exist?
failures << "native status must not be documented as approval" unless docs.include?("completion signal, not an approval proxy")
failures << "custom gate must stay absent" unless docs.include?("No shared policy repository or custom GitHub App is required")

[
  "/.coderabbit.yaml @bulanovdm",
  "/.github/ @bulanovdm",
  "/scripts/validate-coderabbit.sh @bulanovdm",
  "/scripts/verify-workflow-pins.rb @bulanovdm",
  "/scripts/test-coderabbit-policy.rb @bulanovdm",
].each { |value| failures << "missing CODEOWNER: #{value}" unless owners.include?(value) }

failures << "CI must verify workflow pins" unless ci.include?("./scripts/verify-workflow-pins.rb")
failures << "CI must validate CodeRabbit policy" unless ci.include?("./scripts/validate-coderabbit.sh")
failures << "CI must run governance tests" unless ci.include?("./scripts/test-coderabbit-policy.rb")
failures << "release permissions must default closed" unless release.include?("permissions: {}")
failures << "release must require annotated tags" unless release.include?("releases require an annotated tag")
failures << "release must recheck immutable tag" unless release.include?("Recheck immutable release tag")
failures << "release workflow must not use concurrency" if release.match?(/^concurrency:/)

abort failures.join("\n") unless failures.empty?
puts "CodeRabbit governance policy passed."
