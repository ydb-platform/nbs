You can use [act](https://github.com/nektos/act) as a debugging tool for pipelines it acts as a GitHub runner of some sort, using docker.

It is not 100% replacement for GitHub actions altogether (i.e. you can't run self-hosted GitHub runners), but you can use it to debug your changes before committing

To set it up follow the instructions on the GitHub page and

To use it you need to set up a few files in separate directories, from where you will call `act`:

* `.secrets` - get it here https://console.nebius.ai/folders/yc.nbs.nbs-secrets/lockbox/secret/cd4pqp4a1mt3hcshllf6/overview
* `.vars` - example below:

```bash
AWS_BUCKET=github-actions-s3
AWS_ENDPOINT=https://storage.eu-north1.nebius.cloud
REMOTE_CACHE_URL_YA=http://195.242.17.155:9090
AWS_WEBSITE_SUFFIX=website.nemax.nebius.cloud
```

Also, you will need `~/.actrc`:

Here are a few examples of how to use it:

```bash
act -W .github/workflows/pr-github-actions.yaml workflow_dispatch
```

You can add input values in `.input` file:

```bash
echo <<EOF
test_size=small
test_type=unittest
test_threads=32
#run_tests=false
#run_build=false
EOF
act -W .github/workflows/build_and_test_act.yaml workflow_dispatch
```
