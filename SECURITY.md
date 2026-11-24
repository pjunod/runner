# Security — reporting vulnerabilities privately

Report a suspected vulnerability through
[GitHub private vulnerability reporting](https://github.com/pjunod/runner/security/advisories/new).
Include affected versions, a minimal reproducer, impact, and any proposed fix.
Keep credentials and private media out of public issues and logs.

Use the latest reviewed main revision or a current release. Fixes are tested
through protected pull requests; a green scan is evidence of the checks that
ran, not a guarantee of vulnerability-free software. This project does not
claim an independent audit or Best Practices certification.

Runner supports intentional LAN HTTP and operator-configured paths/scripts.
Configure authentication and an appropriate network boundary before exposing
the API. These capabilities are part of the product's trusted-operator model;
see [deployment](docs/DEPLOY.md) and [configuration](docs/CONFIGURATION.md).
