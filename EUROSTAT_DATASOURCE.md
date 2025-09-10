# Eurostat Data Source Documentation

This document provides an overview of the Eurostat data source, focusing on the concepts and terminology relevant to users of the `py-load-eurostat` package.

## Introduction to Eurostat

Eurostat is the statistical office of the European Union, responsible for publishing high-quality, harmonised statistics on Europe. It provides a vast range of data covering the economy, population, society, and environment of the EU and its member states.

The data provided by Eurostat is:
- **Comprehensive**: Covering a wide array of topics.
- **Harmonised**: Data is collected and processed in a consistent manner across all member states, allowing for meaningful comparisons.
- **Freely available**: Most of the data is accessible free of charge for public use.

## Finding Eurostat Data

The easiest way to explore the available data is through the [Eurostat Data Browser](https://ec.europa.eu/eurostat/databrowser/explore/all/all_themes). The data is organized in a thematic tree.

### Identifying the Dataset ID

To download a dataset with `py-load-eurostat`, you need to provide its `dataset_id`. This is a unique code assigned by Eurostat to each dataset.

You can find the `dataset_id` in the Data Browser. When you have selected a dataset, the ID is displayed in the information panel. For example, the dataset "GDP and main components" has the ID `nama_10_gdp`.

### Programmatic Discovery: The Table of Contents (TOC)

For programmatic discovery of datasets, Eurostat provides a Table of Contents (TOC) file. This file, available in TSV format, lists all available datasets with their IDs and descriptions. The `py-load-eurostat` tool uses this TOC to get a list of all available datasets.

## Structure of Eurostat Datasets

Eurostat datasets are structured as multi-dimensional cubes. To understand the data, it's important to be familiar with the following concepts:

### Dimensions and Attributes

- **Dimensions**: These are the variables that define the structure of the dataset. Common dimensions include `GEO` (geolocation), `TIME_PERIOD` (time), and `INDIC_NA` (indicator). Each cell in the data cube is a unique combination of dimension values.
- **Attributes**: These provide additional information about the data, such as the unit of measure or observation flags (e.g., 'c' for confidential).

### Codelists

The values for most dimensions are represented by codes. For example, in the `GEO` dimension, 'DE' represents Germany and 'FR' represents France. A **codelist** is a file that contains the mapping between these codes and their human-readable labels. The `py-load-eurostat` tool can automatically download and store these codelists.

### Data Structure Definition (DSD)

The entire structure of a dataset—its dimensions, attributes, and their associated codelists—is defined in a **Data Structure Definition (DSD)** file. This is a metadata file in SDMX-ML (XML) format. The `py-load-eurostat` tool fetches and parses this file to understand how to process the data.

## Data Formats

The `py-load-eurostat` tool interacts with two primary file formats:

### TSV (Tab-Separated Values)

This is the format used for the actual statistical data. It's a text file where values are separated by tabs. Eurostat's TSV files have a specific structure:
- The first line contains a header with the dimension names.
- Each subsequent line represents a unique combination of dimension values (except for the time dimension).
- The columns represent the time periods (e.g., years, quarters).
- The values in the cells are the observations, often followed by flags.

The `py-load-eurostat` tool is designed to parse this wide format and unpivot it into a tidy, long format suitable for relational databases.

### SDMX-ML (XML)

SDMX (Statistical Data and Metadata eXchange) is an international standard for exchanging statistical information. Eurostat uses the XML implementation of this standard (SDMX-ML) for its metadata files, including the DSD and codelists. You don't need to interact with these files directly, as `py-load-eurostat` handles them automatically.

## Data Representation

When downloading data, Eurostat offers two representations, which can be selected in `py-load-eurostat` with the `--representation` option:

- **`Standard` (coded)**: This is the default and most common representation. The dimension values in the data are represented by their short codes (e.g., `DE`, `FR`). This is efficient for storage and processing.
- **`Full` (labeled)**: In this representation, the codes are replaced by their full, human-readable labels (e.g., `Germany`, `France`). This can be useful for quick analysis or for users who are not familiar with the codes, but it results in larger files and may not be ideal for loading into a database.

For most use cases with `py-load-eurostat`, the `Standard` representation is recommended. The tool will download the codelists separately, allowing you to join the codes with their labels within your database.
