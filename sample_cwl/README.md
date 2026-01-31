# Some samples

This directory contains some sample CWLs aimed at demonstrating how this library works.
You can analyze how this CWL has been created to know what this library supports, in practice.
Move to the specific directory, then run the *python* script inside.
If you pick the *K8s* version, be sure you got your *KUBECONFIG* env variable correctly pointing to your cluster.

At the current time we have:

- *Random Grepper Counter*: Generate a file with *N* random string, then grep it using a *predicate*, then count how many lines have been preserved
  - To run workflow cwl execute `cwltool random_grep_count/random_grep_count.cwl#random_grep_count --grep_string <grep_string> --random_string_number <random_string_number>`
- *Multiple Input Checker*: Test all the inputs supported by the adapter and persist them to a file
  - To run workflow cwl execute 
  ```bash
    cwltool multiple_input_checker/multiple_inputs_checker.cwl#multiple_inputs_checker multiple_input_checker/inputs.yml
  ```
- *Dockerized PySTAC Action:* Two step composed workflow witch use docker images that download and item from STAC catalogue and crop it
  - To run workflow cwl execute 
  ```bash
    cwltool dockerized_pystac_actions/dockerized_pystac_actions.cwl#cwl2prefect dockerized_pystac_actions/inputs.yml
  ```
  - To run workflow cwl execute 
  ```bash
    cwltool dockerized_pystac_actions/dockerized_pystac_actions_parallel_crop.cwl#cwl2prefect dockerized_pystac_actions/inputs_parallel_crop.yml
  ```
- *NDVI*: Generate a NDVI image from Sentinel-2 data
  - To run workflow cwl execute 
  ```bash
    cwltool ndvi/ndvi.cwl#ndvi ndvi/inputs.yml
  ``` 