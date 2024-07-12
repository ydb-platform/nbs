[![PkgGoDev](https://pkg.go.dev/badge/github.com/hashicorp/terraform-plugin-log)](https://pkg.go.dev/github.com/hashicorp/terraform-plugin-log)

# terraform-plugin-log

terraform-plugin-log is a helper module for logging from Terraform providers. It uses RPC-specific loggers to attach context and information to logs, and has multiple loggers to allow filtering of log output, making finding what you're looking for easier. It is a small wrapper on top of [go-hclog](https://github.com/hashicorp/go-hclog), adding some conventions and reframing things for Terraform plugin developers.

## Go Compatibility

This project follows the [support policy](https://golang.org/doc/devel/release.html#policy) of Go as its support policy. The two latest major releases of Go are supported by the project.

Currently, that means Go **1.19** or later must be used when including this project as a dependency.

## Contributing

See [`.github/CONTRIBUTING.md`](https://github.com/hashicorp/terraform-plugin-log/blob/main/.github/CONTRIBUTING.md)

## License

[Mozilla Public License v2.0](https://github.com/hashicorp/terraform-plugin-log/blob/main/LICENSE)
