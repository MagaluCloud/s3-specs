# Running S3 Specs from OCI image (Docker or Podman)

## 1. Create a config file with your endpoints and credentials,
following the yaml format in profiles.example.yaml

```bash
wget https://raw.githubusercontent.com/MagaluCloud/s4-specs/refs/heads/main/params.example.yaml -O params.yaml
vim params.yaml
# Edit to add this values on the number of profiles your tests needs
# (policy and acl tests for example needs 2 different profiles):
#    region_name: "br-ne1"
#    endpoint_url: "https://br-se1.magaluobjects.com/"
#    aws_access_key_id: "YOUR-KEY-ID-HERE"
#    aws_secret_access_key: "YOUR-SECRET-KEY-HERE"
```

### 1.1. Optional: build the image locally

If you cloned the repo and want to build the image locally instead of using the published ones,
use the Containerfiles under the `oci` folder:

```bash
IMAGE_NAME=myimage podman build -t "$IMAGE_NAME" -f ./oci/full.Containerfile .
```

## 2. Run all tests (this might take several minutes)

```bash
# $IMAGE_NAME is the local tag name, if you build the Containerfile yourself,
# or the published one like https://ghcr.io/magalucloud/s3-specs:full
#
# all examples can be run with docker as well
podman run -t \
  -v $(realpath params.yaml):/app/params.example.yaml \
  "$IMAGE_NAME" \
  -n auto
```

> ::note:: Whatever comes after the image name (`-n auto` in the example above)
are pytest arguments, see `oci/entrypoint.sh` for details. The next examples are
variations of test filters using the markers feature of pytest (`-m` argument).

## 3. Run all tests, excluding specific categories

In following example we use the `-m` arg with the `not` operator to exclude two categories:

```bash
podman run -t \
  -v $(realpath params.yaml):/app/params.example.yaml \
  "$IMAGE_NAME" \
  -n auto -m "not locking not slow"
```

## 4. Run only some specific categories

You can pass pytest arguments to the image entrypoint, for example, you can filter tests of
specific categories, using the `-m` arg for markers, like:

```bash
podman run -t \
  -v $(realpath params.yaml):/app/params.example.yaml \
  "$IMAGE_NAME" \
  -n auto -m "basic cold_storage"
```
