---
name: Bug report
about: Create a report to help us to improve the project
labels: 'bug'

---

<!--
NOTE: This repo is only for issues with the python tecton-client specifically. If you're experiencing an issue that seems broader and may involve other components of the system, or if you're unsure about the source of the problem, we recommend following the instructions outlined in our [support documentation](https://docs.tecton.ai/creating-a-tecton-support-ticket). This will ensure that your concern is properly addressed by our support team.
-->

## Summary
<!-- Short summary of the issue -->

## Repro Steps
### Current Behavior:
<!-- A concise description of what you're experiencing. -->

### Expected Behavior:
<!-- A concise description of what you expected to happen. -->

### Steps To Reproduce:

If possible, please create a repro using https://explore.tecton.ai/. This will make reproducing the issue easier.

```python
from tecton_client import TectonClient

client = TectonClient(...)
```

## System Information

1. tecton_client version (`pip freeze | grep tecton_client`)
2. Python version (`python --version`)
3. Operating system (MacOS, Linux, Windows, etc)
