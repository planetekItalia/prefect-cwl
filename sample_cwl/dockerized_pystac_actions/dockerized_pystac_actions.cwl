cwlVersion: v1.2
$namespaces:
  s: https://schema.org/
s:softwareVersion: 4.0.0
$graph:
 
  - class: Workflow
    id: "#cwl2prefect"
    label: CWL for cwl2prefect POC
    doc: Documentation for cwl2prefect is available at
    inputs:
        downloader_catalog_url:
            type: string
        downloader_collection_name:
            type: string
        downloader_date_start:
            type: string
        downloader_date_end:
            type: string
        downloader_bbox:
            type: float[]
    outputs:
      output_downloader:
        type: Directory
        outputSource: downloader/downloader_output
      output_cropper:
        type: Directory
        outputSource: cropper/cropper_output
    steps:
      downloader:
        run: "#downloader"
        in:
            downloader_catalog_url: downloader_catalog_url
            downloader_collection_name: downloader_collection_name
            downloader_date_start: downloader_date_start
            downloader_date_end: downloader_date_end
            downloader_bbox: downloader_bbox

        out: [downloader_output]
      cropper:
        run: "#cropper"
        in:
          cropper_input: downloader/downloader_output
          downloader_bbox: downloader_bbox
        out: [cropper_output]

  - class: CommandLineTool
    id: downloader
    requirements:
      DockerRequirement:
        dockerPull: monacopkt/pystac-downloader:1.4.0
        dockerOutputDirectory: /out
      NetworkAccess:
        networkAccess: true
    baseCommand: python
    arguments:
      - /app/main.py
      - --folder
      - /out/data
    inputs:
        downloader_catalog_url:
            type: string
            inputBinding:
                prefix: --catalog
        downloader_collection_name:
            type: string
            inputBinding:
                prefix: --collection
        downloader_date_start:
            type: string
            inputBinding:
                prefix: --start
        downloader_date_end:
            type: string
            inputBinding:
                prefix: --end
        downloader_bbox:
            type: float[]
            inputBinding:
                prefix: --bbox
    outputs:
      downloader_output:
        type: Directory
        outputBinding:
          glob: data
  - class: CommandLineTool
    id: cropper
    requirements:
      DockerRequirement:
        dockerPull: docker.io/monacopkt/pystac-cropper:1.5.0
        dockerOutputDirectory: /out
      NetworkAccess:
        networkAccess: true
    baseCommand: python
    arguments:
      - /app/main.py
      - -o
      - /out/output
    inputs:
      cropper_input:
        type: Directory
        inputBinding:
          prefix: --input
      downloader_bbox:
        type: float[]
        inputBinding:
          prefix: --bbox
    outputs:
      cropper_output:
        type: Directory
        outputBinding:
          glob: output