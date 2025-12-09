import os
import re
import yaml

ACTIONS_DIR = ".github/actions"
WORKFLOWS_DIR = ".github/workflows"
TEMP_ACTIONS_DIR = ".temporary/actions"
TEMP_WORKFLOWS_DIR = ".temporary/workflows"

os.makedirs(TEMP_ACTIONS_DIR, exist_ok=True)
os.makedirs(TEMP_WORKFLOWS_DIR, exist_ok=True)


def load_yaml_file(filepath):
    with open(filepath, "r") as f:
        return yaml.safe_load(f)


def extract_runs_from_workflow(data):
    """Extract run commands from a workflow YAML structure."""
    runs = []
    if not data or "jobs" not in data:
        return runs

    for job_id, job_data in data["jobs"].items():
        steps = job_data.get("steps", [])
        if steps and isinstance(steps, list):
            for step in steps:
                if "run" in step:
                    runs.append(step["run"])
    return runs


def extract_runs_from_action(data):
    """Extract run commands from a composite action YAML structure."""
    runs = []
    if not data or "runs" not in data:
        return runs

    runs_data = data["runs"]
    if runs_data.get("using") == "composite":
        steps = runs_data.get("steps", [])
        if steps and isinstance(steps, list):
            for step in steps:
                if "run" in step:
                    runs.append(step["run"])
    return runs


def parse_command_blocks(run_content):
    """
    Parse the run content into command blocks.

    Handles:
    - Line continuations using backslash
    - Heredocs starting with <<EOF and ending with EOF
    - Bash arrays starting with name=( and ending with )
    """
    lines = run_content.splitlines()
    command_blocks = []
    current_block = []
    in_heredoc = False
    in_array = False

    for line in lines:
        stripped = line.strip()

        if in_heredoc:
            current_block.append(line)
            if stripped == "EOF":
                command_blocks.append(current_block)
                current_block = []
                in_heredoc = False
            continue

        if in_array:
            current_block.append(line)
            if stripped == ")":
                command_blocks.append(current_block)
                current_block = []
                in_array = False
            continue

        # Detect start of heredoc
        if "<<" in line and "EOF" in line:
            if current_block:
                command_blocks.append(current_block)
                current_block = []
            in_heredoc = True
            current_block.append(line)
            continue

        # Detect start of bash array: var=( or var = (
        if re.match(r"^\s*\w+\s*=\s*\($", line):
            if current_block:
                command_blocks.append(current_block)
                current_block = []
            in_array = True
            current_block.append(line)
            continue

        # Default multiline via backslashes
        current_block.append(line)
        if not stripped.endswith("\\"):
            command_blocks.append(current_block)
            current_block = []

    if current_block:
        command_blocks.append(current_block)

    return command_blocks


def write_runs_to_files(runs, output_dir, prefix):
    """
    Write run commands to files.

    Write each run command to a .sh file with template {action name}-{index inside of action file}
    in the given output_dir.

    Also adds a #!/usr/bin/env bash shebang at the start of each script.

    For each run:
      - Parse into command blocks.
      - If any block contains '${{ ... }}', insert shellcheck disable instructions
        before that block.
    """
    for i, run_content in enumerate(runs, 1):
        file_id = f"{prefix}-{i}.sh"
        filepath = os.path.join(output_dir, file_id)

        command_blocks = parse_command_blocks(run_content)

        with open(filepath, "w") as f:
            f.write("#!/usr/bin/env bash\n\n")
            for block in command_blocks:
                # Check if this block contains GitHub variables
                if any("${{" in line for line in block):
                    # SC2296 is about that github variables are not valid shell variables
                    # SC1083 about basically the same thing
                    f.write("# shellcheck disable=SC2296,SC1083\n")
                for line in block:
                    f.write(line + "\n")


def process_workflows():
    for root, dirs, files in os.walk(WORKFLOWS_DIR):
        for file in files:
            if file.endswith((".yml", ".yaml")):
                filepath = os.path.join(root, file)
                data = load_yaml_file(filepath)
                runs = extract_runs_from_workflow(data)
                if runs:
                    base_name = os.path.splitext(file)[0]
                    write_runs_to_files(runs, TEMP_WORKFLOWS_DIR, base_name)


def process_actions():
    # For actions, we assume each action directory under .github/actions contains an action.yml or action.yaml
    for action_dir in os.listdir(ACTIONS_DIR):
        full_path = os.path.join(ACTIONS_DIR, action_dir)
        if os.path.isdir(full_path):
            # Look for action.yml or action.yaml
            action_file = None
            for candidate in ["action.yml", "action.yaml"]:
                candidate_path = os.path.join(full_path, candidate)
                if os.path.exists(candidate_path):
                    action_file = candidate_path
                    break

            if action_file:
                data = load_yaml_file(action_file)
                runs = extract_runs_from_action(data)
                if runs:
                    write_runs_to_files(runs, TEMP_ACTIONS_DIR, action_dir)


if __name__ == "__main__":
    process_workflows()
    process_actions()
    print("Run commands have been extracted and saved in .temporary/")
