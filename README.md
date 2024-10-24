# S3 Specs

A growing collection of **executable specifications** that can be used to test
**Object Storage** implementations similar to AWS's S3, for example, cloud products
that are "S3-compatible".

## Contribute

This is an open project that and you can contribute with it by suggesting or writing 
new specifications.

### Suggesting Specifications

Just open a new issue on the [issues page](), explaining what feature, scenarios and steps you are
interesting in see described as an executable specification on this collection.

### Writing Specifications

Specifications are written in the format of Notebooks, we use **jupyter**, parametrized with 
**papermill** and exported to markdown (to become pages) by **nbconvert**.

Follow these steps to submit a contribution:

- install the dependencies
  - `poetry install`
- launch the interactive environment
  - `poetry run jupyter docs`
- write a new .ipynb document, or duplicate an existing one
- run your cells and check that the expected results are ok
  - parametrize accordingly, secrets and other arguments should be variables to be filled by **papermill**
- include a link to the new spec on the main page (index.md)
- add your name to the AUTHORS file
- open a pull request

