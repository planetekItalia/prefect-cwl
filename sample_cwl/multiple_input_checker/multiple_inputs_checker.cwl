cwlVersion: v1.2
$graph:
  - class: Workflow
    id: "#multiple_inputs_checker"
    label: multiple_inputs_checker
    doc: Simply check various type of inputs
    inputs:
      val_int:
        type: int
      val_int_a:
        type: int[]
      val_flt:
        type: float
      val_flt_a:
        type: float[]
      val_str:
        type: string
      val_str_a:
        type: string[]
    outputs:
      output_dir:
        type: Directory
        outputSource: multiple_checker/check_output
    steps:
      multiple_checker:
        run: "#multiple_checker"
        in:
          val_int: val_int
          val_int_a: val_int_a
          val_flt: val_flt
          val_flt_a: val_flt_a
          val_str: val_str
          val_str_a: val_str_a
        out: [check_output]

  - class: CommandLineTool
    id: "multiple_checker"
    label: multiple_inputs_checker
    doc: Check multiple type of inputs type
    requirements:
      DockerRequirement:
        dockerPull: python:3.11-alpine
        dockerOutputDirectory: /out
      InlineJavascriptRequirement: {}
      EnvVarRequirement:
        envDef:
          OUTDIR: /out
          WRK_DIR: /app
      InitialWorkDirRequirement:
        listing:
          - entryname: /cwl_job/app.py
            entry: |
              import argparse
              import base64
              import os

              def main():
                  outdir = os.environ["OUTDIR"]
                  output_dir = os.path.join(outdir, "check_output")
                  os.makedirs(output_dir, exist_ok=True)

                  print(f"I'm a input Checker!")

                  p = argparse.ArgumentParser()
                  p.add_argument("--i", type=int, required=True)
                  p.add_argument("--ia", type=int, nargs='+', required=True)
                  p.add_argument("--f", type=float, required=True)
                  p.add_argument("--fa", type=float, nargs='+', required=True)
                  p.add_argument("--s", type=str, required=True)
                  p.add_argument("--sa", type=str, nargs='+', required=True)

                  args = p.parse_args()

                  print(f"These are inputs received: {args}")
                  abs_path = os.path.abspath(f"{output_dir}/out.txt")
                  print("Absolute output path:", abs_path)
                  with open(f"{output_dir}/out.txt", 'w') as f:
                    print('These are inputs', file=f)
                    print(args, file=f)

              if __name__ == "__main__":
                  main()
    baseCommand: [python, /cwl_job/app.py]
    inputs:
      val_int:
        type: int
        inputBinding:
            prefix: --i
      val_int_a:
        type: int[]
        inputBinding:
            prefix: --ia
      val_flt:
        type: float
        inputBinding:
            prefix: --f
      val_flt_a:
        type: float[]
        inputBinding:
            prefix: --fa
      val_str:
        type: string
        inputBinding:
            prefix: --s
      val_str_a:
        type: string[]
        inputBinding:
            prefix: --sa
    outputs:
      check_output:
        type: Directory
        outputBinding:
          glob: check_output