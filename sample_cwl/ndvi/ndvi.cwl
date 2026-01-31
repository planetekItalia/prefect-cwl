cwlVersion: v1.2
$namespaces:
  s: https://schema.org/
s:softwareVersion: 0.1.2
schemas:
  - http://schema.org/version/9.0/schemaorg-current-http.rdf
$graph:

  - class: Workflow
    id: "#ndvi"
    label: workflow_label
    doc: workflow_doc
    inputs:
      spatial_extent:
        type: string[]
        label: Spatial extent bounding box [minLon, minLat, maxLon, maxLat]
    outputs:
      execution_result_assets:
        type: Directory
        outputSource: process/item_results
    steps:
      process:
        run: "#process"
        in:
          spatial_extent: spatial_extent
        out: [item_results, assets_results]

  - class: CommandLineTool
    id: process
    baseCommand: python
    arguments:
      - /app/run.py
      - --spatial_extent
      - $(inputs.spatial_extent[0])
      - $(inputs.spatial_extent[1])
      - $(inputs.spatial_extent[2])
      - $(inputs.spatial_extent[3])
    requirements:
      ResourceRequirement:
        coresMax: 1
        ramMax: 4096
      EnvVarRequirement:
        envDef:
          WRK_DIR: /app
      DockerRequirement:
        dockerPull: brunifrancesco/ndvi
        dockerOutputDirectory: /out
      NetworkAccess:
        networkAccess: true
    inputs:
      spatial_extent:
        type: string[]
    outputs:
      item_results:
        type: Directory
        outputBinding:
          glob: items
      assets_results:
        type: Directory
        outputBinding:
          glob: assets

