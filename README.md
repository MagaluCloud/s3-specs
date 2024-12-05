# S3 Specs

A growing collection of **executable specifications** that can be used to test
**Object Storage** implementations similar to AWS's S3, for example, cloud products
that are "S3-compatible".

## Use S3-Specs as a testing tool on a terminal

Since we aim at having specs to be pytest compliant, running a single spec might be as simple as:

```
cd docs
pytest -n auto <spec path>

# Or if you need specific config to the test you are running:
# pytest -n auto <spec path> --config ../<params file.yaml>
```

But the specs are also readable pages, so if you want to use the generation of a page to run a test
use the page generation shell script example:

```
./run-spec.sh docs/list-buckets_test.py params/br-ne1.yaml markdown
```

## Browse S3 sample library online

All the specs are published online as execution pages at: https://marmotitude.github.io/s3-specs/

## Execute specifications as Jupyter Notebooks

If you have a local copy of this repo, another possibility is to play with the examples interactively
on a Jupyter Lab environment, to install the dependencies and launch it you will need to have
an environment with Python and Poetry:

```
poetry install
poetry shell
jupyter lab docs
```

Then right-click on a .py file ending with _test.py and choose "Open With Notebook".


## Contributing

This is an open project and you can contribute with it by suggesting or writing 
new specifications.

### Suggesting Specifications

Just open a new issue on the [issues page](https://github.com/marmotitude/s3-specs/issues),
explaining what feature, scenarios and steps you are interesting in see described as an
executable specification on this collection.

### Writing Specifications

Specifications are written in python, following the pytest patterns, and mixed with markdown comments.
The idea is to make the specification to be mostly excecutable pytest code but with a document
structure, with headings and text paragraphs.


Web pages will be generated by a renderer that interprets the source as jupyter notebooks, with the
help of **jupytext**, and **nbconvert**.

Keeping both pytest tests and jupyter notebooks on the same specs might sound strange, but
it is really not that bad. The extra effort is worth it in order to gain reliable documentation 
along with end-to-end test scenarios.

Follow these steps to submit a contribution:

- install the dependencies
- write a new "_test.py" document, or duplicate an existing one
- write the examples as pytest test_ functions
  - write new fixtures if needed
- run `pytest` to check the assertions
- use Jupyter "Open With Notebook" context menu to run your cells and check that it prints useful
stuff for humans (tests are documentation)
  - parametrize accordingly, using yaml files
- generate new pages with the `./run_spec.sh` script
  - include a link to the new pages on the main page (index.md)
- add your name to the [AUTHORS](./AUTHORS) file
- open a pull request

## License

MIT

## Sponsors and Acknowledgements

- [MagaluCloud](https://magalu.cloud)

