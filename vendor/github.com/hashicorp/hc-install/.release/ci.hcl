# Copyright (c) HashiCorp, Inc.
# SPDX-License-Identifier: MPL-2.0

schema = "1"

project "hc-install" {
  // team is currently unused and has no meaning
  // but is required to be non-empty by CRT orchestrator
  team = "_UNUSED_"
  slack {
    notification_channel = "C01QDH3Q37W" // #feed-terraform-exec
  }
  github {
    organization     = "hashicorp"
    repository       = "hc-install"
    release_branches = ["main"]
  }
}

event "build" {
  action "build" {
    organization = "hashicorp"
    repository   = "hc-install"
    workflow     = "build"
  }
}

event "prepare" {
  depends = ["build"]

  action "prepare" {
    organization = "hashicorp"
    repository   = "crt-workflows-common"
    workflow     = "prepare"
    depends      = ["build"]
  }

  notification {
    on = "fail"
  }
}

event "trigger-staging" {
  // This event is dispatched by the bob trigger-promotion command
  // and is required - do not delete.
}

event "promote-staging" {
  depends = ["trigger-staging"]
  action "promote-staging" {
    organization = "hashicorp"
    repository   = "crt-workflows-common"
    workflow     = "promote-staging"
    config       = "release-metadata.hcl"
  }

  notification {
    on = "fail"
  }
}

event "trigger-production" {
  // This event is dispatched by the bob trigger-promotion command
  // and is required - do not delete.
}

event "promote-production" {
  depends = ["trigger-production"]
  action "promote-production" {
    organization = "hashicorp"
    repository   = "crt-workflows-common"
    workflow     = "promote-production"
  }

  notification {
    on = "always"
  }
}

// promote Linux packages to production repo
event "promote-production-packaging" {
  depends = ["promote-production"]
  action "promote-production-packaging" {
    organization = "hashicorp"
    repository   = "crt-workflows-common"
    workflow     = "promote-production-packaging"
  }

  notification {
    on = "always"
  }
}
