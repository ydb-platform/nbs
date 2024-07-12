// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package build

import (
    "context"
    "fmt"
    "io/ioutil"
    "log"
    "os"
    "time"

    "github.com/go-git/go-git/v5"
    "github.com/go-git/go-git/v5/plumbing"
    isrc "github.com/hashicorp/hc-install/internal/src"
    "github.com/hashicorp/hc-install/internal/validators"
    "github.com/hashicorp/hc-install/product"
)

var (
    defaultPreCloneCheckTimeout = 1 * time.Minute
    defaultCloneTimeout         = 5 * time.Minute
    defaultBuildTimeout         = 15 * time.Minute

    discardLogger = log.New(ioutil.Discard, "", 0)
)

// GitRevision installs a particular git revision by cloning
// the repository and building it per product BuildInstructions
type GitRevision struct {
    Product      product.Product
    InstallDir   string
    Ref          string
    CloneTimeout time.Duration
    BuildTimeout time.Duration

    logger        *log.Logger
    pathsToRemove []string
}

func (*GitRevision) IsSourceImpl() isrc.InstallSrcSigil {
    return isrc.InstallSrcSigil{}
}

func (gr *GitRevision) SetLogger(logger *log.Logger) {
    gr.logger = logger
}

func (gr *GitRevision) log() *log.Logger {
    if gr.logger == nil {
        return discardLogger
    }
    return gr.logger
}

func (gr *GitRevision) Validate() error {
    if !validators.IsProductNameValid(gr.Product.Name) {
        return fmt.Errorf("invalid product name: %q", gr.Product.Name)
    }
    if !validators.IsBinaryNameValid(gr.Product.BinaryName()) {
        return fmt.Errorf("invalid binary name: %q", gr.Product.BinaryName())
    }

    bi := gr.Product.BuildInstructions
    if bi == nil {
        return fmt.Errorf("no build instructions")
    }
    if bi.GitRepoURL == "" {
        return fmt.Errorf("missing repository URL")
    }
    if bi.Build == nil {
        return fmt.Errorf("missing build instructions")
    }

    return nil
}

func (gr *GitRevision) Build(ctx context.Context) (string, error) {
    bi := gr.Product.BuildInstructions

    if bi.PreCloneCheck != nil {
        preCloneCheckTimeout := defaultPreCloneCheckTimeout
        if bi.PreCloneCheckTimeout > 0 {
            preCloneCheckTimeout = bi.PreCloneCheckTimeout
        }

        pccCtx, cancelFunc := context.WithTimeout(ctx, preCloneCheckTimeout)
        defer cancelFunc()

        gr.log().Printf("running %s pre-clone check (timeout: %s)",
            gr.Product.Name, preCloneCheckTimeout)
        err := bi.PreCloneCheck.Check(pccCtx)
        if err != nil {
            return "", err
        }
        gr.log().Printf("%s pre-clone check finished", gr.Product.Name)
    }

    if gr.pathsToRemove == nil {
        gr.pathsToRemove = make([]string, 0)
    }

    repoDir, err := ioutil.TempDir("",
        fmt.Sprintf("hc-install-build-%s", gr.Product.Name))
    if err != nil {
        return "", err
    }
    gr.pathsToRemove = append(gr.pathsToRemove, repoDir)

    ref := gr.Ref
    if ref == "" {
        ref = "HEAD"
    }

    cloneTimeout := defaultCloneTimeout
    if bi.CloneTimeout > 0 {
        cloneTimeout = bi.CloneTimeout
    }
    if gr.CloneTimeout > 0 {
        cloneTimeout = gr.CloneTimeout
    }
    cloneCtx, cancelFunc := context.WithTimeout(ctx, cloneTimeout)
    defer cancelFunc()

    gr.log().Printf("cloning %s repository from %s to %s (timeout: %s)",
        gr.Product.Name,
        gr.Product.BuildInstructions.GitRepoURL,
        repoDir, cloneTimeout)
    repo, err := git.PlainCloneContext(cloneCtx, repoDir, false, &git.CloneOptions{
        URL:           gr.Product.BuildInstructions.GitRepoURL,
        ReferenceName: plumbing.ReferenceName(gr.Ref),
        Depth:         1,
    })
    if err != nil {
        return "", fmt.Errorf("unable to clone %s from %q @ %q: %w",
            gr.Product.Name, gr.Product.BuildInstructions.GitRepoURL, ref, err)
    }
    gr.log().Printf("cloning %s finished", gr.Product.Name)
    head, err := repo.Head()
    if err != nil {
        return "", err
    }

    gr.log().Printf("%s repository HEAD is at %s", gr.Product.Name, head.Hash())

    buildTimeout := defaultBuildTimeout
    if bi.BuildTimeout > 0 {
        buildTimeout = bi.BuildTimeout
    }
    if gr.BuildTimeout > 0 {
        buildTimeout = gr.BuildTimeout
    }

    buildCtx, cancelFunc := context.WithTimeout(ctx, buildTimeout)
    defer cancelFunc()

    if loggableBuilder, ok := bi.Build.(withLogger); ok {
        loggableBuilder.SetLogger(gr.log())
    }
    installDir := gr.InstallDir
    if installDir == "" {
        tmpDir, err := ioutil.TempDir("",
            fmt.Sprintf("hc-install-%s-%s", gr.Product.Name, head.Hash()))
        if err != nil {
            return "", err
        }
        installDir = tmpDir
        gr.pathsToRemove = append(gr.pathsToRemove, installDir)
    }

    gr.log().Printf("building %s (timeout: %s)", gr.Product.Name, buildTimeout)
    defer gr.log().Printf("building of %s finished", gr.Product.Name)
    return bi.Build.Build(buildCtx, repoDir, installDir, gr.Product.BinaryName())
}

func (gr *GitRevision) Remove(ctx context.Context) error {
    if gr.pathsToRemove != nil {
        for _, path := range gr.pathsToRemove {
            err := os.RemoveAll(path)
            if err != nil {
                return err
            }
        }
    }

    return gr.Product.BuildInstructions.Build.Remove(ctx)
}

type withLogger interface {
    SetLogger(*log.Logger)
}
