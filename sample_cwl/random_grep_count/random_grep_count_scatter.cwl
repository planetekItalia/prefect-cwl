cwlVersion: v1.2
$graph:
  - class: Workflow
    id: "#random_grep_count"
    label: random_grep_count
    doc: Generate random strings in file, grep them, then count
    requirements:
      ScatterFeatureRequirement: {}
      StepInputExpressionRequirement: {}
    inputs:
      random_string_number:
        type: int
      grep_string:
        type: string[]
      augment_lines:
        type: int
    outputs:
      output_dir:
        type: Directory
        outputSource: counter/counter_output
    steps:
      randomizer:
        run: "#randomizer"
        in:
          number_of_random_strings: random_string_number
        out: [randomizer_output]

      grepper:
        run: "#grepper"
        scatter: grep_string
        in:
          grepper_input: randomizer/randomizer_output
          grep_string: grep_string
        out: [grepper_output]

      augmenter:
        run: "#augmenter"
        scatter: augmenter_input
        in:
          augmenter_input: grepper/grepper_output
          extra_lines: augment_lines
        out: [augmenter_output]

      counter:
        run: "#counter"
        in:
          counter_input: augmenter/augmenter_output
        out: [counter_output]

  - class: CommandLineTool
    id: "randomizer"
    label: randomizer
    doc: Create a file with N random base64 lines
    requirements:
      DockerRequirement:
        dockerPull: python:3.11-alpine
        dockerOutputDirectory: /out
      InitialWorkDirRequirement:
        listing:
          - entryname: /cwl_job/randomizer.py
            entry: |
              import argparse
              import base64
              import os

              import logging
              import sys

              logging.basicConfig(
                  level=logging.INFO,
                  format="%(asctime)s | %(levelname)s | %(message)s",
                  handlers=[logging.StreamHandler(sys.stdout)],
              )

              logger = logging.getLogger(__name__)

              def main():
                  logger.info("Hello stdout")
                  logger.error("Still stdout")
                  p = argparse.ArgumentParser()
                  p.add_argument("--n", type=int, required=True)
                  args = p.parse_args()

                  outdir = "/out"
                  output_dir = os.path.join(outdir, "randomizer_output")
                  logger.info(f"Writing to {output_dir}")
                  os.makedirs(output_dir, exist_ok=True)

                  output_file = os.path.join(output_dir, "random.txt")
                  logger.info(f"Writing to {output_file}")
                  with open(output_file, "ab") as f:
                      for _ in range(args.n):
                          encoded = base64.b64encode(os.urandom(16))
                          f.write(encoded + b"\n")

              if __name__ == "__main__":
                  main()

    baseCommand: [python, /cwl_job/randomizer.py]
    inputs:
      number_of_random_strings:
        type: int
        inputBinding:
          prefix: --n
    outputs:
      randomizer_output:
        type: Directory
        outputBinding:
          glob: randomizer_output

  - class: CommandLineTool
    id: "grepper"
    label: grepper
    doc: Grep lines matching a pattern from the randomizer output
    requirements:
      DockerRequirement:
        dockerPull: python:3.11-alpine
        dockerOutputDirectory: /out
      InitialWorkDirRequirement:
        listing:
          - entryname: /cwl_job/grepper.py
            entry: |
              import argparse
              import os

              def main():
                  p = argparse.ArgumentParser()
                  p.add_argument("--input-dir", required=True)
                  p.add_argument("--pattern", required=True)
                  args = p.parse_args()

                  outdir = "/out"
                  output_dir = os.path.join(outdir, "grepper_output")
                  os.makedirs(output_dir, exist_ok=True)

                  in_file = os.path.join(args.input_dir, "random.txt")
                  out_file = os.path.join(output_dir, "grep.txt")

                  pat = args.pattern.encode("utf-8")
                  try:
                      with open(in_file, "rb") as fin, open(out_file, "wb") as fout:
                          for line in fin:
                              if pat in line:
                                  fout.write(line)
                  except FileNotFoundError:
                      # If input doesn't exist, produce empty output
                      with open(out_file, "wb") as fout:
                          pass

              if __name__ == "__main__":
                  main()
    baseCommand: [python, /cwl_job/grepper.py]
    arguments:
      - prefix: --pattern
        valueFrom: $(inputs.grep_string)
    inputs:
      grepper_input:
        type: Directory
        inputBinding:
          prefix: --input-dir
      grep_string:
        type: string
    outputs:
      grepper_output:
        type: Directory
        outputBinding:
          glob: grepper_output

  - class: CommandLineTool
    id: "augmenter"
    label: augmenter
    doc: Add random lines to each grepped file
    requirements:
      DockerRequirement:
        dockerPull: python:3.11-alpine
        dockerOutputDirectory: /out
      InitialWorkDirRequirement:
        listing:
          - entryname: /cwl_job/augmenter.py
            entry: |
              import argparse
              import base64
              import os

              def main():
                  p = argparse.ArgumentParser()
                  p.add_argument("--input-dir", required=True)
                  p.add_argument("--n", type=int, required=True)
                  args = p.parse_args()

                  outdir = "/out"
                  output_dir = os.path.join(outdir, "augmenter_output")
                  os.makedirs(output_dir, exist_ok=True)

                  in_file = os.path.join(args.input_dir, "grep.txt")
                  out_file = os.path.join(output_dir, "grep.txt")

                  with open(out_file, "wb") as fout:
                      try:
                          with open(in_file, "rb") as fin:
                              for line in fin:
                                  fout.write(line)
                      except FileNotFoundError:
                          pass

                      for _ in range(args.n):
                          encoded = base64.b64encode(os.urandom(16))
                          fout.write(encoded + b"\n")

              if __name__ == "__main__":
                  main()
    baseCommand: [python, /cwl_job/augmenter.py]
    inputs:
      augmenter_input:
        type: Directory
        inputBinding:
          prefix: --input-dir
      extra_lines:
        type: int
        inputBinding:
          prefix: --n
    outputs:
      augmenter_output:
        type: Directory
        outputBinding:
          glob: augmenter_output

  - class: CommandLineTool
    id: "counter"
    label: counter
    doc: Count all lines across all augmented files
    requirements:
      DockerRequirement:
        dockerPull: python:3.11-alpine
        dockerOutputDirectory: /out
      InitialWorkDirRequirement:
        listing:
          - entryname: /cwl_job/counter.py
            entry: |
              import argparse
              import os

              def main():
                  p = argparse.ArgumentParser()
                  p.add_argument("--input-dirs", nargs="+", required=True)
                  args = p.parse_args()

                  outdir = "/out"
                  output_dir = os.path.join(outdir, "counter_output")
                  os.makedirs(output_dir, exist_ok=True)
                  out_file = os.path.join(output_dir, "count.txt")

                  total = 0
                  for d in args.input_dirs:
                      fpath = os.path.join(d, "grep.txt")
                      try:
                          with open(fpath, "rb") as fin:
                              total += sum(1 for _ in fin)
                      except FileNotFoundError:
                          continue

                  with open(out_file, "w", encoding="utf-8") as fout:
                      fout.write(f"{total}\n")

              if __name__ == "__main__":
                  main()
    baseCommand: [python, /cwl_job/counter.py]
    inputs:
      counter_input:
        type: Directory[]
        inputBinding:
          prefix: --input-dirs
    outputs:
      counter_output:
        type: Directory
        outputBinding:
          glob: counter_output
