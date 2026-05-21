1. Install packer https://developer.hashicorp.com/packer/tutorials/docker-get-started/get-started-install-cli
2. Obtain token:
```
export NEBIUS_IAM_TOKEN=$(nebius iam get-access-token)
export GITHUB_TOKEN=$(gh auth token)
```
3. Set environmental variables:
```
export ORG=ydb-platform
export TEAM=nbs
export PARENT_ID="project-e02gfsnkpr00d7kw1dw8jw"
export SUBNET_ID="vpcsubnet-e02dsth0aw7vwxzn77"
export VM_USER_PASSWD=$(openssl passwd -6 -salt xyz  yourpass)
```
4. Init packer
```
packer init .
```
