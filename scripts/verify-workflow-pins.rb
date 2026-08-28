#!/usr/bin/env ruby
# frozen_string_literal: true

require "open3"
require "optparse"
require "psych"
require "set"

GIT_ACTION_REFERENCE = /\A[^@\s]+@[0-9a-f]{40}\z/
DOCKER_ACTION_REFERENCE = /\Adocker:\/\/[^@\s]+@sha256:[0-9a-f]{64}\z/
WORKFLOW_IMAGE_REFERENCE = %r{
  \A
  (?:
    [a-z0-9](?:[a-z0-9.-]*[a-z0-9])?(?::[0-9]+)?/
  )?
  [a-z0-9]+(?:[._-]+[a-z0-9]+)*(?:/[a-z0-9]+(?:[._-]+[a-z0-9]+)*)*
  (?::[a-z0-9_][a-z0-9_.-]{0,127})?
  @sha256:[0-9a-f]{64}
  \z
}x
VERSIONED_GITHUB_RUNNER_LABEL = %r{
  \A
  (?!.*-latest\z)
  (?:ubuntu-[0-9]{2}\.[0-9]{2}|windows-[0-9]+|macos-[0-9]+)
  (?:-[a-z0-9]+)*
  \z
}x
SAME_COMMIT_REFERENCE = %r{\A(?:\./|\$/)[^@\s]+\z}
WORKFLOW_PATH = %r{\A\.github/workflows/.+\.ya?ml\z}
ACTION_METADATA_PATH = %r{(?:\A|/)action\.ya?ml\z}
REGULAR_FILE_MODES = Set.new(%w[100644 100755]).freeze

options = {}
parser =
  OptionParser.new do |flags|
    flags.banner = "Usage: verify-workflow-pins.rb [--git-ref SHA | --stdin LABEL]"
    flags.on("--git-ref SHA", "Read tracked workflow and action files from this commit") do |value|
      options[:git_ref] = value
    end
    flags.on("--stdin LABEL", "Validate one YAML document read from standard input") do |value|
      options[:stdin] = value
    end
  end
parser.parse!

abort parser.to_s unless ARGV.empty?
if options[:git_ref] && options[:stdin]
  abort "--git-ref and --stdin are mutually exclusive"
end
if options[:git_ref] && !options[:git_ref].match?(/\A[0-9a-f]{40}\z/)
  abort "--git-ref must be a full lowercase commit SHA"
end

def git_output(*arguments)
  stdout, stderr, status = Open3.capture3("git", *arguments)
  return stdout if status.success?

  raise "git #{arguments.first} failed: #{stderr.strip}"
end

def policy_path?(path)
  path.match?(WORKFLOW_PATH) || path.match?(ACTION_METADATA_PATH)
end

def tracked_entries_for(options)
  entries = Hash.new { |paths, path| paths[path] = [] }
  if options[:git_ref]
    git_output("ls-tree", "-rz", options[:git_ref]).split("\0").each do |record|
      metadata, path = record.split("\t", 2)
      mode, type, object = metadata.to_s.split(" ", 3)
      raise "could not parse git tree entry #{record.inspect}" if path.nil? || object.nil?

      entries[path] << { mode: mode, stage: nil, type: type }
    end
  else
    git_output("ls-files", "--stage", "-z").split("\0").each do |record|
      metadata, path = record.split("\t", 2)
      mode, object, stage = metadata.to_s.split(" ", 3)
      raise "could not parse git index entry #{record.inspect}" if path.nil? || stage.nil?

      entries[path] << { mode: mode, stage: stage, type: object.nil? ? nil : "blob" }
    end
  end
  entries
end

def regular_tracked_file?(entries, path, options)
  records = entries.fetch(path, [])
  return false unless records.one?

  entry = records.first
  return false unless REGULAR_FILE_MODES.include?(entry[:mode])
  return entry[:type] == "blob" if options[:git_ref]

  entry[:stage] == "0" && File.file?(path) && !File.symlink?(path)
end

def sources_for(options)
  return [{ options[:stdin] => $stdin.read }, nil] if options[:stdin]

  tracked_entries = tracked_entries_for(options)
  paths = tracked_entries.keys.select { |path| policy_path?(path) }.sort
  raise "no tracked workflow or action metadata files were found" if paths.empty?

  sources =
    paths.to_h do |path|
      unless regular_tracked_file?(tracked_entries, path, options)
        raise "#{path} is not a regular tracked workflow or action metadata file"
      end
      content =
        if options[:git_ref]
          git_output("show", "#{options[:git_ref]}:#{path}")
        else
          File.binread(path)
        end
      [path, content]
    end
  [sources, tracked_entries]
end

def document_context(document)
  anchors = {}
  positions = {}
  next_position = 0
  visit = nil
  visit = lambda do |node|
    # Psych retains source order in children for block and flow collections. Indexing the parsed
    # document therefore distinguishes same-line definitions without allowing another document's
    # anchors to leak into this one.
    positions[node.object_id] = next_position
    next_position += 1
    if node.respond_to?(:anchor) && !node.is_a?(Psych::Nodes::Alias) && !node.anchor.nil?
      (anchors[node.anchor] ||= []) << node
    end
    node.children.each { |child| visit.call(child) } if node.respond_to?(:children) && node.children
  end
  visit.call(document)
  { anchors: anchors, positions: positions }
end

def resolve_alias(node, context)
  seen = Set.new
  while node.is_a?(Psych::Nodes::Alias)
    return nil unless seen.add?(node.object_id)

    alias_position = context[:positions].fetch(node.object_id)
    node = context[:anchors].fetch(node.anchor, []).reverse.find do |candidate|
      context[:positions].fetch(candidate.object_id) < alias_position
    end
    return nil if node.nil?
  end
  node
end

def scalar_value(node, context)
  resolved = resolve_alias(node, context)
  resolved.value if resolved.is_a?(Psych::Nodes::Scalar)
end

def mapping_entries(node, context)
  resolved = resolve_alias(node, context)
  return [] unless resolved.is_a?(Psych::Nodes::Mapping)

  resolved.children.each_slice(2).to_a
end

def mapping_values(node, name, context)
  mapping_entries(node, context).filter_map do |key, value|
    [key, value] if scalar_value(key, context) == name
  end
end

def sequence_items(node, context)
  resolved = resolve_alias(node, context)
  return [] unless resolved.is_a?(Psych::Nodes::Sequence)

  resolved.children
end

def each_mapping_reference(node, name, context, reference_kind)
  mapping_values(node, name, context).each do |key, value|
    location = "line #{key.start_line + 1}, column #{key.start_column + 1}"
    yield scalar_value(value, context), location, reference_kind
  end
end

def each_steps_uses(node, context, &block)
  sequence_items(node, context).each do |step|
    each_mapping_reference(step, "uses", context, :action, &block)
  end
end

def node_location(node)
  "line #{node.start_line + 1}, column #{node.start_column + 1}"
end

def each_yaml_merge_key(node, context, &block)
  if node.is_a?(Psych::Nodes::Mapping)
    node.children.each_slice(2) do |key, value|
      block.call(node_location(key)) if scalar_value(key, context) == "<<"
      each_yaml_merge_key(key, context, &block)
      each_yaml_merge_key(value, context, &block)
    end
  elsif node.respond_to?(:children) && node.children
    node.children.each { |child| each_yaml_merge_key(child, context, &block) }
  end
end

def each_document_merge_key(stream, &block)
  stream.children.each do |document|
    context = document_context(document)
    document.children.each do |root|
      each_yaml_merge_key(root, context, &block)
    end
  end
end

def each_workflow_container_image(job, context)
  mapping_values(job, "container", context).each do |container_key, container|
    resolved_container = resolve_alias(container, context)
    if resolved_container.is_a?(Psych::Nodes::Scalar)
      yield resolved_container.value, node_location(container_key), :workflow_container_image
      next
    end

    unless resolved_container.is_a?(Psych::Nodes::Mapping)
      yield nil, node_location(container_key), :workflow_container_image
      next
    end

    images = mapping_values(resolved_container, "image", context)
    if images.empty?
      yield nil, node_location(container_key), :workflow_container_image
      next
    end

    images.each do |image_key, image|
      yield scalar_value(image, context), node_location(image_key), :workflow_container_image
    end
  end
end

def each_workflow_service_image(job, context)
  mapping_values(job, "services", context).each do |services_key, services|
    resolved_services = resolve_alias(services, context)
    unless resolved_services.is_a?(Psych::Nodes::Mapping)
      yield nil, node_location(services_key), :workflow_service_image
      next
    end

    mapping_entries(resolved_services, context).each do |service_key, service|
      resolved_service = resolve_alias(service, context)
      unless resolved_service.is_a?(Psych::Nodes::Mapping)
        yield nil, node_location(service_key), :workflow_service_image
        next
      end

      images = mapping_values(resolved_service, "image", context)
      if images.empty?
        yield nil, node_location(service_key), :workflow_service_image
        next
      end

      images.each do |image_key, image|
        yield scalar_value(image, context), node_location(image_key), :workflow_service_image
      end
    end
  end
end

def each_workflow_uses(root, context, &block)
  job_mappings = mapping_values(root, "jobs", context)
  if job_mappings.empty?
    block.call(nil, node_location(root), :workflow_jobs_missing)
    return
  end

  job_mappings.drop(1).each do |jobs_key, _jobs|
    block.call(nil, node_location(jobs_key), :workflow_jobs_duplicate)
  end

  job_mappings.each do |jobs_key, jobs|
    resolved_jobs = resolve_alias(jobs, context)
    unless resolved_jobs.is_a?(Psych::Nodes::Mapping)
      block.call(nil, node_location(jobs_key), :workflow_jobs_non_mapping)
      next
    end

    seen_job_names = Set.new
    mapping_entries(resolved_jobs, context).each do |job_name_node, job|
      job_name = scalar_value(job_name_node, context)
      if job_name.nil?
        block.call(nil, node_location(job_name_node), :workflow_job_name_non_scalar)
      elsif !seen_job_names.add?(job_name)
        block.call(job_name, node_location(job_name_node), :workflow_job_duplicate)
      end

      resolved_job = resolve_alias(job, context)
      unless resolved_job.is_a?(Psych::Nodes::Mapping)
        block.call(job_name, node_location(job_name_node), :workflow_job_non_mapping)
        next
      end

      workflow_references = mapping_values(resolved_job, "uses", context)
      workflow_references.drop(1).each do |uses_key, _uses|
        block.call(job_name, node_location(uses_key), :workflow_uses_duplicate)
      end

      runner_labels = mapping_values(resolved_job, "runs-on", context)
      if workflow_references.empty? && runner_labels.empty?
        block.call(job_name, node_location(job_name_node), :workflow_runner_missing)
      end
      runner_labels.drop(1).each do |runner_key, _runner|
        block.call(job_name, node_location(runner_key), :workflow_runner_duplicate)
      end
      runner_labels.each do |runner_key, runner|
        block.call(
          scalar_value(runner, context),
          node_location(runner_key),
          :workflow_runner_label,
        )
      end

      each_mapping_reference(job, "uses", context, :workflow, &block)
      each_workflow_container_image(job, context, &block)
      each_workflow_service_image(job, context, &block)
      mapping_values(job, "steps", context).each do |_steps_key, steps|
        each_steps_uses(steps, context, &block)
      end
    end
  end
end

def each_action_metadata_reference(root, context, &block)
  mapping_values(root, "runs", context).each do |_runs_key, runs|
    each_mapping_reference(runs, "image", context, :docker_image, &block)
    mapping_values(runs, "steps", context).each do |_steps_key, steps|
      each_steps_uses(steps, context, &block)
    end
  end
end

def each_action_uses(stream, path, &block)
  stream.children.each do |document|
    context = document_context(document)
    document.children.each do |root|
      if path.match?(ACTION_METADATA_PATH)
        each_action_metadata_reference(root, context, &block)
      else
        each_workflow_uses(root, context, &block)
      end
    end
  end
end

def same_commit_candidates(reference, reference_kind)
  return [] unless reference.match?(SAME_COMMIT_REFERENCE)

  path = reference.sub(%r{\A(?:\./|\$/)}, "").sub(%r{/+\z}, "")
  return [] if path.empty?

  if reference_kind == :workflow
    path.match?(WORKFLOW_PATH) ? [path] : []
  elsif path.match?(WORKFLOW_PATH)
    []
  else
    ["#{path}/action.yml", "#{path}/action.yaml"]
  end
end

def local_dockerfile_path(metadata_path, reference)
  path = reference.sub(%r{\A\./}, "")
  segments = path.split("/", -1)
  return nil if segments.empty?
  return nil if segments.any? { |segment| segment.empty? || segment == "." || segment == ".." }
  return nil unless segments.last == "Dockerfile"

  directory = File.dirname(metadata_path)
  relative_path = segments.join("/")
  directory == "." ? relative_path : File.join(directory, relative_path)
end

begin
  sources, tracked_entries = sources_for(options)
  errors = []
  local_count = 0
  pinned_count = 0

  sources.each do |path, content|
    begin
      stream = Psych.parse_stream(content, filename: path)
      each_document_merge_key(stream) do |location|
        errors << "#{path}: #{location} YAML merge keys (<<) are not supported"
      end
      each_action_uses(stream, path) do |reference, location, reference_kind|
        if reference_kind == :workflow_jobs_missing
          errors << "#{path}: #{location} workflow must declare exactly one jobs mapping"
        elsif reference_kind == :workflow_jobs_duplicate
          errors << "#{path}: #{location} workflow must not declare jobs more than once"
        elsif reference_kind == :workflow_jobs_non_mapping
          errors << "#{path}: #{location} workflow jobs must be a mapping"
        elsif reference_kind == :workflow_job_name_non_scalar
          errors << "#{path}: #{location} workflow job name must be a scalar string"
        elsif reference_kind == :workflow_job_duplicate
          errors << "#{path}: #{location} workflow job #{reference.inspect} is declared more than once"
        elsif reference_kind == :workflow_job_non_mapping
          errors << "#{path}: #{location} workflow job #{reference.inspect} must be a mapping"
        elsif reference_kind == :workflow_uses_duplicate
          errors << "#{path}: #{location} workflow job #{reference.inspect} declares uses more than once"
        elsif reference_kind == :workflow_runner_missing
          errors <<
            "#{path}: #{location} workflow job #{reference.inspect} must declare exactly one " \
              "scalar explicit versioned GitHub-hosted runs-on label"
        elsif reference_kind == :workflow_runner_duplicate
          errors <<
            "#{path}: #{location} workflow job #{reference.inspect} declares runs-on more than once"
        elsif reference_kind == :workflow_runner_label
          if reference.nil?
            errors <<
              "#{path}: #{location} workflow runs-on must be a scalar explicit versioned " \
                "GitHub-hosted label"
          elsif !reference.match?(VERSIONED_GITHUB_RUNNER_LABEL)
            errors <<
              "#{path}: #{location} workflow runs-on must be a literal explicit versioned " \
                "GitHub-hosted label, got #{reference.inspect}"
          end
        elsif %i[workflow_container_image workflow_service_image].include?(reference_kind)
          image_type = reference_kind == :workflow_container_image ? "container" : "service"
          if reference.nil?
            errors <<
              "#{path}: #{location} workflow #{image_type} image must be a scalar literal"
          elsif reference.match?(WORKFLOW_IMAGE_REFERENCE)
            pinned_count += 1
          else
            errors <<
              "#{path}: #{location} workflow #{image_type} image must be a literal lowercase " \
                "SHA-256 digest-pinned registry reference, got #{reference.inspect}"
          end
        elsif reference_kind == :docker_image
          if reference.nil?
            errors << "#{path}: #{location} image must be a scalar string"
          elsif reference.start_with?("docker://")
            if reference.match?(DOCKER_ACTION_REFERENCE)
              pinned_count += 1
            else
              errors << "#{path}: #{location} uses an unpinned Docker image #{reference.inspect}"
            end
          elsif (dockerfile_path = local_dockerfile_path(path, reference))
            if options[:stdin] || regular_tracked_file?(tracked_entries, dockerfile_path, options)
              local_count += 1
            else
              errors <<
                "#{path}: #{location} does not resolve to a regular tracked Dockerfile " \
                  "#{reference.inspect}"
            end
          else
            errors << "#{path}: #{location} has an invalid Docker action image #{reference.inspect}"
          end
        elsif reference.nil?
          errors << "#{path}: #{location} uses must be a scalar string"
        elsif reference.start_with?("docker://")
          if reference.match?(DOCKER_ACTION_REFERENCE)
            pinned_count += 1
          else
            errors << "#{path}: #{location} uses an unpinned Docker image #{reference.inspect}"
          end
        elsif reference.start_with?("./") || reference.start_with?("$/")
          candidates = same_commit_candidates(reference, reference_kind)
          if !candidates.empty? &&
              (options[:stdin] || candidates.any? { |candidate| sources.key?(candidate) })
            local_count += 1
          else
            errors <<
              "#{path}: #{location} has an invalid same-commit action or workflow reference " \
              "#{reference.inspect}"
          end
        elsif reference.match?(GIT_ACTION_REFERENCE)
          pinned_count += 1
        else
          errors << "#{path}: #{location} uses a mutable action reference #{reference.inspect}"
        end
      end
    rescue Psych::Exception => error
      errors << "#{path}: invalid YAML: #{error.message.lines.first&.strip}"
    end
  end

  unless errors.empty?
    errors.each { |error| warn "error: #{error}" }
    exit 1
  end

  puts(
    "Workflow action references verified: #{pinned_count} immutable third-party, " \
      "#{local_count} local",
  )
rescue StandardError => error
  warn "error: #{error.message}"
  exit 1
end
