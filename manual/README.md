# Manual Argo Workflows

- While the ScaleODM app orchestrates Argo Workflows,
  it's also possible to manually trigger processing.
- We can defined our own Argo manifests, to trigger
  a processing pipelines for our imagery.

## Standard Processing

Ensure Argo Workflows is installed first:
https://argo-workflows.readthedocs.io/en/latest/installation

```bash
kubectl apply -f standard.yaml
```
