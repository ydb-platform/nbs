For validating syntax use action-validator

```bash
find .github/workflows .github/actions -type f \( -iname \*.yaml -o -iname \*.yml \) -print | while read path; do echo Checking $path; action-validator --verbose $path; done
```

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
      "ref": "main",
      "sha": "BASESHA"
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
act pull_request --bind \
  -W .github/workflows/pr-github-actions.yaml \
  -j check-trigger-label \
  -e /tmp/act-pr-event.json \
  -P self-hosted=ghcr.io/catthehacker/ubuntu:act-latest \
  -P runner_light=ghcr.io/catthehacker/ubuntu:act-latest \
  --pull=false
```

The allowed-label case should also set `allowed=true`:

```bash
cat <<EOF > /tmp/act-pr-event-large-tests.json
{
  "action": "labeled",
  "pull_request": {
    "number": 123,
    "head": {
      "sha": "HEADSHA"
    },
    "base": {
      "ref": "main",
      "sha": "BASESHA"
    },
    "user": {
      "login": "local-user"
    },
    "labels": []
  },
  "label": {
    "name": "large-tests"
  },
  "repository": {
    "name": "nbs",
    "owner": {
      "login": "local-org"
    }
  }
}
EOF
act pull_request --bind \
  -W .github/workflows/pr-github-actions.yaml \
  -j check-trigger-label \
  -e /tmp/act-pr-event-large-tests.json \
  -P self-hosted=ghcr.io/catthehacker/ubuntu:act-latest \
  -P runner_light=ghcr.io/catthehacker/ubuntu:act-latest \
  --pull=false
```

The ignored-label case should set `allowed=false`:

```bash
cat <<EOF > /tmp/act-pr-event-doc-label.json
{
  "action": "labeled",
  "pull_request": {
    "number": 123,
    "head": {
      "sha": "HEADSHA"
    },
    "base": {
      "ref": "main",
      "sha": "BASESHA"
    },
    "user": {
      "login": "local-user"
    },
    "labels": []
  },
  "label": {
    "name": "documentation"
  },
  "repository": {
    "name": "nbs",
    "owner": {
      "login": "local-org"
    }
  }
}
EOF
act pull_request --bind \
  -W .github/workflows/pr-github-actions.yaml \
  -j check-trigger-label \
  -e /tmp/act-pr-event-doc-label.json \
  -P self-hosted=ghcr.io/catthehacker/ubuntu:act-latest \
  -P runner_light=ghcr.io/catthehacker/ubuntu:act-latest \
  --pull=false
```
