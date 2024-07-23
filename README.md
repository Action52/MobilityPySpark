# MobilitySpark
***
Repository to extend support of MEOS and MobilityDB data types into Pyspark, and experimentation on different partition techniques for trajectory data.
This repository was used as codebase for the thesis "MobilitySpark: Big Mobility Data Management with PySpark and PyMEOS".

## Install

### Through Docker

The project can be run locally using Docker. This is method is preferred if you have a
Mac with M-Series Chip.

1. Download this repository.
2. <code>cd</code> into the *container* folder.
3. Build the image, name it as you prefer, for example *mobilityspark*:

```
docker build -t <LOCAL-NAME-OF-THE-IMAGE> <PATH-TO-Dockerfile>
```

Example:

```
docker build -t mobilityspark ./
```

4. Run the container:

```
docker run -v <VOLUME-PATH> -p <JUPYTERLAB-PORT-FORWARD> -p <SPARK-UI-PORT-FORWARD> <CONTAINER-NAME>
```

Example:

```
docker run -v $(pwd)/../../../data:/data -p 8887:8888 -p 4040:4040 -p 4041:4041 -p 4042:4042 pyrtitionfinal
```

This example, for instance, runs the project mapping the volume ../../../data to the /data folder inside the container.
This is useful if you want to use local files inside your container. Also, you need to specify:
- Ports for the JupyterLab (recommended <code>-p 8887:8888 </code>)
- Ports for the Spark UI (recommended <code>-p 4040:4040 </code>)

You can reserve any number of ports for different Spark sessions, as in the example.

This command will run the container and will end up by printing a token to start the JupyterLab. Copy the token.

5. Go to your browser, and go to localhost:8887, or the port corresponding to your JupyterLab.
6. Insert the generated token where asked. The JupyterLab will prompt.
7. Now you can run the notebooks of the project by going to /notebooks inside the JupyterLab.


### Locally

To install locally:

1. Download this repository.
2. <code>cd</code> into the main folder of the repository.
3. In a clean Python environment, run:

```
pip install .
```

This will install MobilitySpark and the required packages in your environment.

4. If you desire to run the notebooks, now you can do, with your env activated:
```
jupyter notebook
```

5. Now you can run the notebooks of the project by going to /notebooks inside your local Jupyter server.

## Examples

There are three example notebooks located at the <code>/notebooks/</code> folder:
1. *UDTDemo.ipynb* - Provides an introduction to MobilitySpark, partitioning mobility data, and reading/writing these files using PySpark.
2. *PartitioningSchemesWithPyMEOS.ipynb* - Recreates the OpenSky experiments performed in the thesis.
3. *BerlinMODQueries.ipynb* - Recreates the BerlinMOD experiments performed in the thesis.

## Sample plots

![](sample_charts/jette_exp2.png)
![](sample_charts/jette_auderguem_exp2.png)
![](sample_charts/dots_subplot_4_large.svg)

## License

MIT License
Copyright (c) 2024 Luis Alfredo León Villapún, see LICENSE