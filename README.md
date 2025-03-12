# S3 Specs

A growing collection of **executable specifications** designed to document and test
**S3-compatible Object Storage implementations**. These specifications serve as both
human-readable documentation and actionable end-to-end tests.

## Objectives

The main purposes of this project are (in this order):

1. **Provide clear documentation of features** for S3-compatible cloud **Object Storage** products.  
   - Each executable specification combines human-readable explanations with examples
   (the executable part of the spec, written as Pytest tests).

2. **Offer a comprehensive set of end-to-end tests** to check compatibility and functionality
across Object Storage providers.  
   - The primary sponsor of this open-source project is [Magalu Cloud](https://console.magalu.cloud).
   These tests are used in development environments before introducing new features and in existing
   regions as smoke tests and regular health checks.

## Usage

### Browse Documentation Online

Navigate the latest documentation generated from executed specs at:  
https://magalucloud.github.io/s3-specs/

### Run and Browse Locally

Install dependencies and launch a local Jupyter Lab server to view the specs interactively:

```bash
just browse
```

Then, right-click a `_test.py` file and choose "Open With Notebook."

### Run Specs as Tests

To execute a single spec using pytest, run the following command on a machine with Python and `uv` installed:

```bash
cd docs
uv run pytest {spec_path} --config ../{config_yaml_file}
```

## Contributing

This is an open project, and we welcome contributions in the form of new or improved specifications.

### Suggesting Specifications

Open an issue on the [issues page](https://github.com/magalucloud/s3-specs/issues), explaining the
feature, scenarios, and steps you'd like to see described as an executable specification.

### Writing Specifications

Specifications are written in Python (using pytest) and natural language (Markdown comments).
The goal is to create a document-like structure that tells a story, combining headings, paragraphs,
and examples. For more on this approach, see [Literate programming](https://en.wikipedia.org/wiki/Literate_programming).

Key guidelines for writing specs:
- Write **natural language descriptions** in Markdown (in **Portuguese**).
- Create pytest `test_` functions for the examples.
- Use pytest fixtures to minimize boilerplate and improve readability.
- Check your assertions by running `pytest`.
- Use Jupyter's "Open With Notebook" context menu to verify that the cells print clear, useful documentation.
- Parametrize your tests using YAML files when necessary.
- Add your name to the [AUTHORS](./AUTHORS) file.
- Open a pull request.

    **::: note**
    Web pages will be generated using a renderer that interprets the source as Jupyter notebooks,
    with the help of **jupytext** and **nbconvert**.

## License

MIT

## Sponsors and Acknowledgements

- [MagaluCloud](https://magalu.cloud)

