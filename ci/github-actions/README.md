# ci/github-actions

`container-image.yml` is the real linux/amd64 image build + container smoke
workflow (spec 009). It performs:

```bash
docker buildx build --platform linux/amd64 --load -t burstel:ci .
scripts/container-smoke.sh burstel:ci
```

It needs no Cloudflare credentials and deploys nothing.

## Why it is not already under `.github/workflows/`

The managed GitHub App used to push this repository does **not** have the
GitHub App `workflows` permission. GitHub rejects any push that creates or
updates a file under `.github/workflows/**` from such an App, so a commit that
adds `.github/workflows/container-image.yml` cannot be pushed by this pipeline
(the non-workflow commit succeeds and the identical commit with the workflow
file fails). This was verified directly: the same commit succeeds without the
workflow and fails with it.

## Install it

From the repository root, with an account/app that has the `workflows`
permission (or via the GitHub UI):

```bash
scripts/install-github-workflow.sh
git add .github/workflows/container-image.yml
git commit -m "ci: install container image build + smoke workflow"
git push
```

Until it is installed, the workflow does not run and the actual image build is
**not verified** (see `docs/cloudflare-deploy.md` and the spec 009 result).
