You can use [act](https://github.com/nektos/act) as a debugging tool for pipelines it acts as a GitHub runner of some sort, using docker.

It is not 100% replacement for GitHub actions altogether (i.e. you can't run self-hosted GitHub runners), but you can use it to debug some of your changes before committing

Here are a few examples of how to use it:

```bash
cat <<EOF > /tmp/act-pr-event.json 
{
  "action": "synchronize",
  "pull_request": {
    "number": 123,
    "head": {
      "sha": "HEADSHA"
    },
    "base": {
      "ref": "main"
    },
    "user": {
      "login": "local-user"
    },
    "labels": []
  },
  "repository": {
    "name": "nbs",
    "owner": {
      "login": "local-org"
    }
  }
}
EOF
act pull_request --bind   -W .github/workflows/pr-github-actions.yaml   -j python   -e /tmp/act-pr-event.json   -P self-hosted=ghcr.io/catthehacker/ubuntu:act-latest   -P runner_light=ghcr.io/catthehacker/ubuntu:act-latest
```

